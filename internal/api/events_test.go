package api

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type stream struct {
	frames chan string
	cancel context.CancelFunc
}

func (f *fixture) openStream(t *testing.T, token, query string, header map[string]string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.http.URL+"/v1/events"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "text/event-stream" {
		cancel()
		t.Fatalf("status %d %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	st := &stream{frames: make(chan string, 64), cancel: cancel}
	go func() {
		defer close(st.frames)
		sc := bufio.NewScanner(res.Body)
		sc.Buffer(nil, 1<<20)
		var b strings.Builder
		for sc.Scan() {
			if sc.Text() == "" {
				st.frames <- b.String() + "\n"
				b.Reset()
				continue
			}
			b.WriteString(sc.Text() + "\n")
		}
	}()
	t.Cleanup(cancel)
	return st
}

func (s *stream) next(t *testing.T) string {
	t.Helper()
	select {
	case fr, ok := <-s.frames:
		if !ok {
			t.Fatal("stream closed")
		}
		return fr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a frame")
	}
	return ""
}

func (s *stream) expectNone(t *testing.T) {
	t.Helper()
	select {
	case fr := <-s.frames:
		t.Fatalf("unexpected frame %q", fr)
	default:
	}
}

func (f *fixture) onWaiting() chan struct{} {
	parked := make(chan struct{}, 16)
	f.srv.waiting = func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	}
	return parked
}

func awaitParked(t *testing.T, parked chan struct{}) {
	t.Helper()
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never parked")
	}
}

func idLine(frame string) string {
	line, _, _ := strings.Cut(frame, "\n")
	return line
}

func TestEventsStreamsWithIDs(t *testing.T) {
	f := newFixture(t)
	seq := f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	st := f.openStream(t, dshToken, "?after=0", nil)
	if got := st.next(t); got != ": connected\n\n" {
		t.Fatalf("first frame %q", got)
	}
	fr := st.next(t)
	if !strings.HasPrefix(fr, "id: 1\nevent: message\ndata: {") || !strings.HasSuffix(fr, "}\n\n") || seq != 1 {
		t.Fatalf("frame %q", fr)
	}
	if strings.Count(fr, "\n") != 4 {
		t.Fatalf("data spans lines: %q", fr)
	}
}

func TestEventsRequiresAuth(t *testing.T) {
	f := newFixture(t)
	if res := f.get(t, "", "/v1/events"); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("%d", res.StatusCode)
	}
}

func TestEventsRejectsMalformedStart(t *testing.T) {
	f := newFixture(t)
	if res := f.get(t, dshToken, "/v1/events?after=x"); res.StatusCode != http.StatusBadRequest || !strings.Contains(string(res.Body), "after") {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
}

func TestEventsResumeFromLastEventID(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	}
	st := f.openStream(t, dshToken, "?after=0", map[string]string{"Last-Event-ID": "2"})
	st.next(t)
	if got := idLine(st.next(t)); got != "id: 3" {
		t.Fatalf("got %q", got)
	}
}

func TestEventsAfterQueryParam(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	}
	st := f.openStream(t, dshToken, "?after=1", nil)
	st.next(t)
	if got := idLine(st.next(t)); got != "id: 2" {
		t.Fatalf("got %q", got)
	}
}

func TestEventsUseServerSidePositionWithoutCursor(t *testing.T) {
	f := newFixture(t)
	parked := f.onWaiting()
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	st := f.openStream(t, dshToken, "?after=0", nil)
	st.next(t)
	for _, want := range []string{"id: 1", "id: 2"} {
		if got := idLine(st.next(t)); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
	awaitParked(t, parked)
	st.cancel()
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))

	st = f.openStream(t, dshToken, "", nil)
	st.next(t)
	if got := idLine(st.next(t)); got != "id: 3" {
		t.Fatalf("got %q", got)
	}
	pos, ok, err := f.store.Position(context.Background(), "dsh", "events")
	if err != nil || !ok || pos < 2 {
		t.Fatalf("pos=%d ok=%v err=%v", pos, ok, err)
	}
}

func TestEventsFirstConnectStartsAtHead(t *testing.T) {
	f := newFixture(t)
	parked := f.onWaiting()
	for range 3 {
		f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	}
	st := f.openStream(t, dshToken, "", nil)
	st.next(t)
	awaitParked(t, parked)
	st.expectNone(t)
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Minute))
	if got := idLine(st.next(t)); got != "id: 4" {
		t.Fatalf("got %q", got)
	}
}

func TestEventsIgnoreOtherConsumersAppend(t *testing.T) {
	f := newFixture(t)
	parked := f.onWaiting()
	st := f.openStream(t, dshToken, "", nil)
	st.next(t)
	awaitParked(t, parked)
	f.append(t, "hermes", "group_hermes_text", baseTime.Add(-time.Minute))
	awaitParked(t, parked)
	st.expectNone(t)
}

func TestEventsMissedEvent(t *testing.T) {
	f := newFixture(t)
	old := baseTime.Add(-8 * 24 * time.Hour)
	f.append(t, "dsh", "group_text", old)
	f.append(t, "dsh", "group_text", old)
	fresh := f.append(t, "dsh", "group_text", baseTime)
	st := f.openStream(t, dshToken, "?after=0", nil)
	st.next(t)
	if got := st.next(t); got != "event: missed\ndata: {\"count\":2}\n\n" {
		t.Fatalf("got %q", got)
	}
	if got := idLine(st.next(t)); got != "id: 3" || fresh != 3 {
		t.Fatalf("got %q", got)
	}
	logs := f.logs.String()
	for _, want := range []string{`"msg":"messages missed"`, `"consumer":"dsh"`, `"channel":"events"`, `"count":2`} {
		if !strings.Contains(logs, want) {
			t.Errorf("log missing %s: %s", want, logs)
		}
	}
	if m := metricsText(t, f); !strings.Contains(m, `signal_hub_missed_total{consumer="dsh"} 2`) {
		t.Errorf("metric missing:\n%s", m)
	}
}

type failingWriter struct {
	header http.Header
	writes int
	failAt int
}

func (w *failingWriter) Header() http.Header { return w.header }
func (w *failingWriter) WriteHeader(int)     {}
func (w *failingWriter) Flush()              {}
func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes >= w.failAt {
		return 0, errors.New("broken pipe")
	}
	return len(p), nil
}

func TestEventsPositionNotAdvancedOnFailedWrite(t *testing.T) {
	f := newFixture(t)
	first := f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/events?after=0", nil)
	req = req.WithContext(context.WithValue(req.Context(), consumerKey{}, "dsh"))
	w := &failingWriter{header: http.Header{}, failAt: 3}
	f.srv.events(w, req)
	pos, ok, err := f.store.Position(context.Background(), "dsh", "events")
	if err != nil || !ok || pos != first {
		t.Fatalf("pos=%d ok=%v err=%v want %d", pos, ok, err, first)
	}
	if got := f.srv.Health.Snapshot().Consumers["dsh"]["events"]; got.Subscribers != 0 || got.Position != first {
		t.Fatalf("%+v", got)
	}
}

func TestEventsKeepalive(t *testing.T) {
	f := newFixture(t)
	f.srv.Keepalive = 50 * time.Millisecond
	st := f.openStream(t, dshToken, "", nil)
	st.next(t)
	if got := st.next(t); got != ": keepalive\n\n" {
		t.Fatalf("got %q", got)
	}
}

func TestEventsEndOnContextCancel(t *testing.T) {
	f := newFixture(t)
	parked := f.onWaiting()
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), consumerKey{}, "dsh"))
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/v1/events", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.srv.events(httptest.NewRecorder(), req)
	}()
	awaitParked(t, parked)
	if got := f.srv.Health.Snapshot().Consumers["dsh"]["events"].Subscribers; got != 1 {
		t.Fatalf("subscribers = %d", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return")
	}
	if got := f.srv.Health.Snapshot().Consumers["dsh"]["events"].Subscribers; got != 0 {
		t.Fatalf("subscribers = %d", got)
	}
	if strings.Contains(f.logs.String(), `"level":"ERROR"`) {
		t.Fatalf("error logged: %s", f.logs.String())
	}
}
