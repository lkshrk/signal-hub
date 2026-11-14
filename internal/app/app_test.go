package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/testupstream"
)

const (
	account   = "+4915112345678"
	restA     = "group.QUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0="
	hermesID  = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	wait      = 5 * time.Second
	accountNS = "4915112345678"
)

var dshToken = strings.Repeat("d", 32)

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

func envelope(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "envelopes", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newConfig(t *testing.T, upstreamURL string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tokens"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tokens", "dsh"), []byte(dshToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf(`version: 1
upstream:
  url: %s
  account: "%s"
listen: 127.0.0.1:1
adminListen: 127.0.0.1:2
database:
  url: sqlite://%s/data/hub.db
tokenDir: %s/tokens
consumers:
  - name: dsh
    groups: ["%s"]
  - name: hermes
    groups: ["%s"]
    compat:
      listen: 127.0.0.1:3
`, upstreamURL, account, dir, dir, restA, hermesID)
	cfg, err := config.Parse([]byte(raw), config.Env{Getenv: func(string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen, cfg.AdminListen, cfg.Consumers[1].Compat.Listen = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	return cfg
}

type running struct {
	app    *App
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func (r *running) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(d):
		t.Fatalf("Run did not return within %s", d)
		return nil
	}
}

func launch(t *testing.T, a *App) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{app: a, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.err = a.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
	})
	return r
}

func start(t *testing.T, cfg *config.Config, log *slog.Logger) *running {
	t.Helper()
	a, err := New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	r := launch(t, a)
	select {
	case <-a.Ready():
	case <-r.done:
		t.Fatalf("Run returned before ready: %v", r.err)
	case <-time.After(wait):
		t.Fatal("app never became ready")
	}
	return r
}

func get(t *testing.T, rawURL, token string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*wait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, body
}

func awaitHealthy(t *testing.T, a *App) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		code, body := get(t, "http://"+a.Addr("admin")+"/healthz", "")
		if code == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthz = %d %s", code, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type stream struct {
	blocks chan string
}

func openStream(t *testing.T, rawURL, token string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	if res.StatusCode != http.StatusOK {
		t.Fatalf("stream %s: status %d", rawURL, res.StatusCode)
	}
	s := &stream{blocks: make(chan string, 64)}
	go func() {
		defer close(s.blocks)
		rd := bufio.NewReader(res.Body)
		var b strings.Builder
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			b.WriteString(line)
			if line == "\n" {
				s.blocks <- b.String()
				b.Reset()
			}
		}
	}()
	return s
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
			if strings.HasPrefix(b, ": keepalive") {
				continue
			}
			return b
		case <-deadline:
			t.Fatal("timed out waiting for an SSE block")
		}
	}
}

func (s *stream) awaitEnd(t *testing.T) {
	t.Helper()
	deadline := time.After(wait)
	for {
		select {
		case _, ok := <-s.blocks:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream body did not end")
		}
	}
}

func compatEventsURL(a *App) string {
	return "http://" + a.Addr("compat:hermes") + "/api/v1/events?account=" + url.QueryEscape(account)
}

type pull struct {
	Messages []json.RawMessage `json:"messages"`
	Next     int64             `json:"next"`
}

func TestRunEndToEnd(t *testing.T) {
	release := make(chan struct{})
	up := testupstream.New(t, testupstream.Seq(
		testupstream.Wait(release),
		testupstream.Write(envelope(t, "group_text"), envelope(t, "group_hermes_text")),
		testupstream.Hold,
	))
	sends := make(chan map[string]any, 4)
	up.HandleHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/send" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sends <- body
		_, _ = w.Write([]byte(`[{"timestamp":"1763114600000"}]`))
	}))

	logs := &lockedBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := start(t, newConfig(t, up.HTTPURL()), log)
	a := r.app
	awaitHealthy(t, a)

	sse := openStream(t, compatEventsURL(a), "")
	if b := sse.next(t); !strings.HasPrefix(b, ": connected") {
		t.Fatalf("first compat block = %q", b)
	}
	close(release)

	code, body := get(t, "http://"+a.Addr("api")+"/v1/messages?after=0&wait=5", dshToken)
	if code != http.StatusOK {
		t.Fatalf("pull = %d %s", code, body)
	}
	var p pull
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Messages) != 1 || !strings.Contains(string(p.Messages[0]), "please fix the build") {
		t.Fatalf("dsh pull = %s", body)
	}

	frame := sse.next(t)
	if !strings.Contains(frame, "hello hermes") || strings.Contains(frame, "please fix the build") {
		t.Fatalf("compat frame = %q", frame)
	}

	sendBody := fmt.Sprintf(`{"recipient":%q,"text":"done"}`, restA)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+a.Addr("api")+"/v1/send", strings.NewReader(sendBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+dshToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resBody, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("send = %d %s", res.StatusCode, resBody)
	}
	select {
	case got := <-sends:
		recipients, _ := got["recipients"].([]any)
		if len(recipients) != 1 || recipients[0] != restA {
			t.Fatalf("upstream recipients = %v", got["recipients"])
		}
	case <-time.After(wait):
		t.Fatal("upstream /v2/send never called")
	}

	r.cancel()
	if err := r.wait(t, wait); err != nil {
		t.Fatalf("Run = %v", err)
	}
	out := logs.String()
	for _, want := range []string{`"msg":"listening"`, `"msg":"upstream connected"`, `"msg":"pruned"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs lack %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, accountNS) {
		t.Fatalf("logs contain the account number:\n%s", out)
	}
}

func seed(t *testing.T, path string, at time.Time, raws ...[]byte) {
	t.Helper()
	st, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, raw := range raws {
		p, err := message.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Append(context.Background(), store.Entry{Parsed: p, ReceivedAt: at, Raw: raw}); err != nil {
			t.Fatal(err)
		}
	}
}

func rows(t *testing.T, path string) ([]int64, int64) {
	t.Helper()
	st, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	page, err := st.Read(context.Background(), store.Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	h, err := st.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, r := range page.Records {
		seqs = append(seqs, r.Seq)
	}
	return seqs, h
}

func TestRetentionPrunesOnStart(t *testing.T) {
	up := testupstream.New(t)
	cfg := newConfig(t, up.HTTPURL())
	seed(t, cfg.Database.Path, time.Now().Add(-15*24*time.Hour), envelope(t, "group_text"))
	seed(t, cfg.Database.Path, time.Now().Add(-time.Hour), envelope(t, "group_hermes_text"))

	logs := &lockedBuffer{}
	start(t, cfg, slog.New(slog.NewJSONHandler(logs, nil)))

	if seqs, h := rows(t, cfg.Database.Path); len(seqs) != 1 || seqs[0] != 2 || h != 2 {
		t.Fatalf("rows, head = %v, %d; want [2], 2", seqs, h)
	}
	if !strings.Contains(logs.String(), `"msg":"pruned","count":1`) {
		t.Fatalf("logs = %s", logs.String())
	}
}

func TestRetentionRunsPeriodically(t *testing.T) {
	up := testupstream.New(t)
	cfg := newConfig(t, up.HTTPURL())
	a, err := New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	a.pruneInterval = 20 * time.Millisecond
	launch(t, a)
	select {
	case <-a.Ready():
	case <-time.After(wait):
		t.Fatal("never ready")
	}
	seed(t, cfg.Database.Path, time.Now().Add(-15*24*time.Hour), envelope(t, "group_text"))
	deadline := time.Now().Add(wait)
	for {
		seqs, h := rows(t, cfg.Database.Path)
		if h == 1 && len(seqs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("old row never pruned: rows %v head %d", seqs, h)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGracefulShutdown(t *testing.T) {
	up := testupstream.New(t, testupstream.Seq(testupstream.Write(envelope(t, "group_text")), testupstream.Hold))
	cfg := newConfig(t, up.HTTPURL())
	r := start(t, cfg, slog.New(slog.DiscardHandler))
	a := r.app

	code, body := get(t, "http://"+a.Addr("api")+"/v1/messages?after=0&wait=5", dshToken)
	var p pull
	if err := json.Unmarshal(body, &p); code != http.StatusOK || err != nil || len(p.Messages) != 1 {
		t.Fatalf("pull = %d %s", code, body)
	}

	compat := openStream(t, compatEventsURL(a), "")
	compat.next(t)
	events := openStream(t, "http://"+a.Addr("api")+"/v1/events", dshToken)

	sent := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			WroteRequest: func(httptrace.WroteRequestInfo) { close(sent) },
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/v1/messages?after=%d&wait=60", a.Addr("api"), p.Next), nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+dshToken)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}()
	select {
	case <-sent:
	case <-time.After(wait):
		t.Fatal("long poll never sent")
	}
	time.Sleep(50 * time.Millisecond)

	began := time.Now()
	r.cancel()
	if err := r.wait(t, 3*time.Second); err != nil {
		t.Fatalf("Run = %v", err)
	}
	t.Logf("shutdown took %s", time.Since(began))
	select {
	case <-pollDone:
	case <-time.After(time.Second):
		t.Fatal("long poll still open after Run returned")
	}
	compat.awaitEnd(t)
	events.awaitEnd(t)

	if _, h := rows(t, cfg.Database.Path); h != 1 {
		t.Fatalf("head after restart = %d, want 1", h)
	}
}

func TestRunFailsOnPortInUse(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	up := testupstream.New(t)
	cfg := newConfig(t, up.HTTPURL())
	cfg.Consumers[1].Compat.Listen = ln.Addr().String()
	a, err := New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	r := launch(t, a)
	err = r.wait(t, wait)
	if err == nil || !strings.Contains(err.Error(), ln.Addr().String()) {
		t.Fatalf("Run = %v, want an error naming %s", err, ln.Addr())
	}
	select {
	case <-a.Ready():
		t.Fatal("Ready closed although a listener failed")
	default:
	}
	if _, h := rows(t, cfg.Database.Path); h != 0 {
		t.Fatalf("head = %d", h)
	}
}
