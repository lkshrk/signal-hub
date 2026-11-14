package compat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

const (
	hermesGroup = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	dshGroup    = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	sender      = "+4915187654321"
	botNumber   = "+4915112345678"
	wait        = 5 * time.Second
)

var baseTime = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type upstreamCall struct {
	Method string
	Path   string
	Body   []byte
}

type fakeUpstream struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	body   string
	calls  []upstreamCall
}

func newFakeUpstream(t *testing.T, status int, body string) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{status: status, body: body}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.calls = append(f.calls, upstreamCall{r.Method, r.URL.EscapedPath(), raw})
		status, body := f.status, f.body
		f.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeUpstream) taken() []upstreamCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]upstreamCall(nil), f.calls...)
}

func (f *fakeUpstream) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

type fixture struct {
	srv      *Server
	http     *httptest.Server
	dbPath   string
	store    *store.Store
	router   *route.Router
	logs     *lockedBuffer
	upstream *fakeUpstream
	parked   chan struct{}
	exited   chan struct{}
}

type fixtureOpts struct {
	status        int
	body          string
	upstreamURL   string
	maxReplayAge  time.Duration
	attachmentDir string
}

func newFixture(t *testing.T, o fixtureOpts) *fixture {
	t.Helper()
	if o.status == 0 {
		o.status = http.StatusNoContent
	}
	if o.maxReplayAge == 0 {
		o.maxReplayAge = 7 * 24 * time.Hour
	}
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "hub.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &lockedBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	now := func() time.Time { return baseTime }
	state := health.New(now)
	consumers := []config.Consumer{
		{Name: "hermes", GroupIDs: []string{hermesGroup}, Senders: []string{sender}, MaxReplayAge: config.Duration(o.maxReplayAge)},
		{Name: "dsh", GroupIDs: []string{dshGroup}, MaxReplayAge: config.Duration(o.maxReplayAge)},
	}
	up := newFakeUpstream(t, o.status, o.body)
	url := up.URL
	if o.upstreamURL != "" {
		url = o.upstreamURL
	}
	router := route.New(consumers, now, log, state)
	f := &fixture{dbPath: dbPath, store: st, router: router, logs: logs, upstream: up, parked: make(chan struct{}, 64), exited: make(chan struct{}, 64)}
	f.srv = &Server{
		Consumer: "hermes",
		Store:    st,
		Router:   router,
		Upstream: upstream.New(url, botNumber),
		Health:   state,
		Log:      log,
		Now:      now,

		AttachmentDir: o.attachmentDir,
	}
	f.srv.waiting = func() {
		select {
		case f.parked <- struct{}{}:
		default:
		}
	}
	f.srv.exited = func() { f.exited <- struct{}{} }
	f.http = httptest.NewServer(f.srv.Handler())
	t.Cleanup(f.http.Close)
	return f
}

func (f *fixture) appendFrame(t *testing.T, raw []byte, at time.Time) int64 {
	t.Helper()
	p, err := message.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	consumer, _ := f.router.Route(p)
	seq, dup, err := f.store.Append(context.Background(), store.Entry{Parsed: p, Consumer: consumer, ReceivedAt: at, Raw: raw})
	if err != nil || dup {
		t.Fatalf("append: seq=%d dup=%v err=%v", seq, dup, err)
	}
	return seq
}

func envelopeFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "envelopes", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (f *fixture) rpc(t *testing.T, body []byte) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.http.URL+"/api/v1/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header.Get("Content-Type"), string(out)
}

func (f *fixture) call(t *testing.T, method string, params map[string]any) rpcReply {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": "x"})
	_, _, out := f.rpc(t, body)
	var r rpcReply
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatalf("decode %s: %v", out, err)
	}
	return r
}

type rpcReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (f *fixture) get(t *testing.T, path string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, f.http.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header.Get("Content-Type"), string(out)
}

type stream struct {
	res    *http.Response
	blocks chan string
	cancel context.CancelFunc
}

func (f *fixture) openStream(t *testing.T, path string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.http.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	t.Cleanup(cancel)
	st := &stream{res: res, blocks: make(chan string, 64), cancel: cancel}
	go func() {
		defer close(st.blocks)
		rd := bufio.NewReader(res.Body)
		var b strings.Builder
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			b.WriteString(line)
			if line == "\n" {
				st.blocks <- b.String()
				b.Reset()
			}
		}
	}()
	return st
}

func (s *stream) next(t *testing.T) string {
	t.Helper()
	deadline := time.After(wait)
	for {
		select {
		case b, ok := <-s.blocks:
			if !ok {
				t.Fatal("stream closed")
			}
			if b == ": keepalive\n\n" {
				continue
			}
			return b
		case <-deadline:
			t.Fatal("timed out waiting for an SSE block")
		}
	}
}

func (f *fixture) awaitParked(t *testing.T) {
	t.Helper()
	select {
	case <-f.parked:
	case <-time.After(wait):
		t.Fatal("events handler never parked")
	}
}

func (f *fixture) awaitExit(t *testing.T) {
	t.Helper()
	select {
	case <-f.exited:
	case <-time.After(wait):
		t.Fatal("events handler never exited")
	}
}

func expectError(t *testing.T, r rpcReply, code int, msg string) {
	t.Helper()
	if r.Error == nil || r.Error.Code != code || r.Error.Message != msg {
		t.Fatalf("error = %+v, want {%d %q}", r.Error, code, msg)
	}
}

func TestUnknownMethodNamesTheHub(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	expectError(t, f.call(t, "updateProfile", map[string]any{}), -32601, "method not supported by signal-hub: updateProfile")
}

func TestRecipientOutsideConsumerRefused(t *testing.T) {
	f := newFixture(t, fixtureOpts{status: http.StatusCreated, body: `[{"timestamp":"1"}]`})
	for _, p := range []map[string]any{
		{"account": botNumber, "groupId": dshGroup, "message": "hi"},
		{"account": botNumber, "recipient": []any{sender, "+4917000000000"}, "message": "hi"},
		{"account": botNumber, "groupId": "not-a-group", "message": "hi"},
	} {
		for _, method := range []string{"send", "sendTyping", "sendReaction", "sendPollTerminate"} {
			expectError(t, f.call(t, method, with(p, "pollTimestamp", 1)), -32603, "recipient not allowed for consumer hermes")
		}
		expectError(t, f.call(t, "sendPollCreate", with(p, "question", "q?", "options", []any{"a", "b"})), -32603, "recipient not allowed for consumer hermes")
	}
	if calls := f.upstream.taken(); len(calls) != 0 {
		t.Fatalf("upstream called: %+v", calls)
	}
}

func with(base map[string]any, kv ...any) map[string]any {
	out := map[string]any{}
	for k, v := range base {
		out[k] = v
	}
	for i := 0; i < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

func TestAccountMismatchRefused(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	expectError(t, f.call(t, "send", map[string]any{"account": "+499", "groupId": hermesGroup, "message": "hi"}), -32603, "account +499 is not served by this hub")
	expectError(t, f.call(t, "sendPollCreate", map[string]any{"account": "+499", "groupId": hermesGroup, "question": "q?", "options": []any{"a", "b"}}), -32603, "account +499 is not served by this hub")
	if calls := f.upstream.taken(); len(calls) != 0 {
		t.Fatalf("upstream called: %+v", calls)
	}
}

func TestMissingAccountUsesHubAccount(t *testing.T) {
	f := newFixture(t, fixtureOpts{status: http.StatusCreated, body: `{"timestamp":"1769271479"}`})
	r := f.call(t, "sendPollCreate", map[string]any{"recipient": []any{sender}, "question": "Start now?", "options": []any{"Yes", "No"}})
	if r.Error != nil {
		t.Fatalf("error %+v", r.Error)
	}
	calls := f.upstream.taken()
	if len(calls) != 1 || calls[0].Method != http.MethodPost || calls[0].Path != "/v1/polls/+4915112345678" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestGetAttachmentOfAnotherConsumerAnswersAsUnknown(t *testing.T) {
	f := newFixture(t, fixtureOpts{status: http.StatusOK, body: "SECRET-BYTES"})
	raw := envelopeFixture(t, "direct_attachment")
	p, err := message.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.Append(context.Background(), store.Entry{Parsed: p, Consumer: "dsh", ReceivedAt: baseTime, Raw: raw}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"x1y2.jpg", "never-seen.png"} {
		want := `GET /v1/attachments/` + id + ` returned HTTP 404: {"error":"No attachment with that name found"}`
		expectError(t, f.call(t, "getAttachment", map[string]any{"account": botNumber, "id": id}), -32603, want)
	}
	if calls := f.upstream.taken(); len(calls) != 0 {
		t.Fatalf("upstream called: %+v", calls)
	}
}

func TestEventsWrongAccount404(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	status, _, body := f.get(t, "/api/v1/events?account=%2B499")
	if status != http.StatusNotFound || body != "unknown account\n" {
		t.Fatalf("status %d body %q", status, body)
	}
}

const eventsPath = "/api/v1/events?account=%2B4915112345678"

func TestEventsResumeFromCompatPosition(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	st := f.openStream(t, eventsPath)
	if got := st.next(t); got != ": connected\n\n" {
		t.Fatalf("first block %q", got)
	}
	f.awaitParked(t)
	frames := [][]byte{
		envelopeFixture(t, "group_text"),
		envelopeFixture(t, "direct_attachment"),
		envelopeFixture(t, "receipt"),
	}
	f.appendFrame(t, frames[0], baseTime)
	f.appendFrame(t, frames[1], baseTime)
	for _, want := range frames[:2] {
		if got := st.next(t); got != "data: "+string(compact(want))+"\n\n" {
			t.Fatalf("block %q", got)
		}
	}
	st.cancel()
	f.awaitExit(t)
	if pos, ok, err := f.store.Position(context.Background(), "hermes", store.ChannelCompat); err != nil || !ok || pos != 2 {
		t.Fatalf("position = %d %v %v", pos, ok, err)
	}
	f.appendFrame(t, frames[2], baseTime)
	st = f.openStream(t, eventsPath)
	if got := st.next(t); got != ": connected\n\n" {
		t.Fatalf("first block %q", got)
	}
	if got := st.next(t); got != "data: "+string(compact(frames[2]))+"\n\n" {
		t.Fatalf("resumed block %q", got)
	}
}

func TestEventsOnlyRoutedEnvelopes(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	st := f.openStream(t, eventsPath)
	st.next(t)
	f.awaitParked(t)
	f.appendFrame(t, envelopeFixture(t, "group_hermes_text"), baseTime)
	own := envelopeFixture(t, "group_text")
	f.appendFrame(t, own, baseTime)
	if got := st.next(t); got != "data: "+string(compact(own))+"\n\n" {
		t.Fatalf("block %q", got)
	}
}

func TestEventsSkipOlderThanMaxReplayAge(t *testing.T) {
	f := newFixture(t, fixtureOpts{maxReplayAge: 10 * time.Minute})
	f.appendFrame(t, envelopeFixture(t, "group_text"), baseTime.Add(-20*time.Minute))
	if err := f.store.SetPosition(context.Background(), "hermes", store.ChannelCompat, 0); err != nil {
		t.Fatal(err)
	}
	st := f.openStream(t, eventsPath)
	st.next(t)
	f.awaitParked(t)
	if pos, ok, err := f.store.Position(context.Background(), "hermes", store.ChannelCompat); err != nil || !ok || pos != 1 {
		t.Fatalf("position after skip = %d %v %v", pos, ok, err)
	}
	fresh := envelopeFixture(t, "receipt")
	f.appendFrame(t, fresh, baseTime)
	if got := st.next(t); got != "data: "+string(compact(fresh))+"\n\n" {
		t.Fatalf("block %q", got)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"msg":"messages missed"`) || !strings.Contains(logs, `"channel":"compat"`) || !strings.Contains(logs, `"consumer":"hermes"`) {
		t.Fatalf("logs %s", logs)
	}
	if strings.Contains(logs, botNumber) || strings.Contains(logs, "please fix the build") {
		t.Fatalf("logs leak: %s", logs)
	}
}

func TestEventsStayOpenWithoutUpstream(t *testing.T) {
	f := newFixture(t, fixtureOpts{upstreamURL: "http://127.0.0.1:1"})
	st := f.openStream(t, eventsPath)
	if st.res.StatusCode != http.StatusOK || st.res.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d %q", st.res.StatusCode, st.res.Header.Get("Content-Type"))
	}
	if got := st.next(t); got != ": connected\n\n" {
		t.Fatalf("first block %q", got)
	}
	f.awaitParked(t)
}

func TestEventsKeepalive(t *testing.T) {
	f := newFixture(t, fixtureOpts{})
	f.srv.Keepalive = 10 * time.Millisecond
	st := f.openStream(t, eventsPath)
	st.next(t)
	select {
	case b := <-st.blocks:
		if b != ": keepalive\n\n" {
			t.Fatalf("block %q", b)
		}
	case <-time.After(wait):
		t.Fatal("no keepalive")
	}
}

func missingFile(path string) string {
	return "read attachment " + filepath.Base(path) + ": open " + path + ": no such file or directory"
}

func sendAttachment(t *testing.T, f *fixture, path string) rpcReply {
	t.Helper()
	return f.call(t, "send", map[string]any{"account": botNumber, "recipient": []any{sender}, "message": "see file", "attachments": []any{path}})
}

func TestAttachmentWithoutDirRefusesEveryPath(t *testing.T) {
	f := newFixture(t, fixtureOpts{status: http.StatusCreated, body: `[{"timestamp":"1"}]`})
	expectError(t, sendAttachment(t, f, f.dbPath), -32603, missingFile(f.dbPath))
	if calls := f.upstream.taken(); len(calls) != 0 {
		t.Fatalf("upstream called: %+v", calls)
	}
}

func TestAttachmentInsideDirIsInlined(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "note.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, fixtureOpts{status: http.StatusCreated, body: `[{"timestamp":"1"}]`, attachmentDir: dir})
	if r := sendAttachment(t, f, path); r.Error != nil {
		t.Fatalf("error %+v", r.Error)
	}
	calls := f.upstream.taken()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	var body struct {
		Attachments []string `json:"base64_attachments"`
	}
	if err := json.Unmarshal(calls[0].Body, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Attachments) != 1 || body.Attachments[0] != "data:text/plain;filename=note.txt;base64,aGVsbG8=" {
		t.Fatalf("attachments = %v", body.Attachments)
	}
}

func TestAttachmentEscapesRefused(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "out")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(root, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(secret, link); err != nil {
		t.Fatal(err)
	}
	dirLink := filepath.Join(dir, "up")
	if err := os.Symlink(root, dirLink); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, fixtureOpts{status: http.StatusCreated, body: `[{"timestamp":"1"}]`, attachmentDir: dir})
	for _, p := range []string{
		dir + "/../secret.txt",
		link,
		filepath.Join(dirLink, "secret.txt"),
		"secret.txt",
		f.dbPath,
	} {
		expectError(t, sendAttachment(t, f, p), -32603, missingFile(p))
	}
	if calls := f.upstream.taken(); len(calls) != 0 {
		t.Fatalf("upstream called: %+v", calls)
	}
}

func TestUpstreamErrorRedactsAccount(t *testing.T) {
	f := newFixture(t, fixtureOpts{status: http.StatusBadRequest, body: `{"error":"account +4915112345678 is not registered"}`})
	r := f.call(t, "sendTyping", map[string]any{"account": botNumber, "groupId": hermesGroup})
	expectError(t, r, -32603, `PUT /v1/typing-indicator/<account> returned HTTP 400: {"error":"account <account> is not registered"}`)
}

func TestCheckUnreachableUpstream(t *testing.T) {
	f := newFixture(t, fixtureOpts{upstreamURL: "http://127.0.0.1:1"})
	status, ct, body := f.get(t, "/api/v1/check")
	if status != http.StatusBadGateway || ct != "text/plain; charset=utf-8" ||
		!strings.HasPrefix(body, "GET /v1/about: dial tcp 127.0.0.1:1: ") || !strings.HasSuffix(body, "\n") || strings.Contains(body, `Get "`) {
		t.Fatalf("status %d %q %q", status, ct, body)
	}
}
