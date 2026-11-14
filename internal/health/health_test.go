package health

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newState() (*State, *clock) {
	c := &clock{now: t0}
	return New(c.Now), c
}

func get(t *testing.T, s *State, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func decode(t *testing.T, body string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return m
}

func TestHealthzOKWhenConnectedAndWritable(t *testing.T) {
	s, _ := newState()
	s.UpstreamConnected(true)
	s.StoreResult(nil)
	code, body := get(t, s, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("code = %d, body %s", code, body)
	}
	m := decode(t, body)
	if m["status"] != "ok" {
		t.Errorf("status = %v", m["status"])
	}
	if m["upstream"].(map[string]any)["connected"] != true {
		t.Errorf("upstream = %v", m["upstream"])
	}
	if m["store"].(map[string]any)["writable"] != true {
		t.Errorf("store = %v", m["store"])
	}
}

func TestHealthz503WhenUpstreamDisconnected(t *testing.T) {
	s, _ := newState()
	s.StoreResult(nil)
	code, body := get(t, s, "/healthz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
	if decode(t, body)["status"] != "error" {
		t.Errorf("body %s", body)
	}
}

func TestHealthz503WhenStoreNotWritable(t *testing.T) {
	s, _ := newState()
	s.UpstreamConnected(true)
	s.StoreResult(errors.New("disk full"))
	code, body := get(t, s, "/healthz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
	st := decode(t, body)["store"].(map[string]any)
	if st["writable"] != false || st["error"] != "disk full" {
		t.Errorf("store = %v", st)
	}
}

func TestHealthzReportsAgesAndCounters(t *testing.T) {
	s, c := newState()
	s.UpstreamConnected(true)
	s.StoreResult(nil)
	s.Reconnected()
	s.Reconnected()
	s.MessageStored("message", t0)
	c.now = t0.Add(42 * time.Second)
	s.Unclaimed()
	s.Unclaimed()
	s.Missed("hermes", 5)
	s.Subscribers("dsh", "events", 1)
	s.Position("dsh", "events", 1042)

	_, body := get(t, s, "/healthz")
	m := decode(t, body)
	if m["lastMessageAgeSeconds"] != float64(42) {
		t.Errorf("age = %v", m["lastMessageAgeSeconds"])
	}
	if m["lastMessageAt"] != "2026-11-14T10:00:00Z" {
		t.Errorf("lastMessageAt = %v", m["lastMessageAt"])
	}
	if m["unclaimed"] != float64(2) {
		t.Errorf("unclaimed = %v", m["unclaimed"])
	}
	if m["missed"].(map[string]any)["hermes"] != float64(5) {
		t.Errorf("missed = %v", m["missed"])
	}
	up := m["upstream"].(map[string]any)
	if up["reconnects"] != float64(2) || up["connectedSince"] != "2026-11-14T10:00:00Z" {
		t.Errorf("upstream = %v", up)
	}
	ev := m["consumers"].(map[string]any)["dsh"].(map[string]any)["events"].(map[string]any)
	if ev["subscribers"] != float64(1) || ev["position"] != float64(1042) {
		t.Errorf("events = %v", ev)
	}
}

func TestHealthzLastMessageNullBeforeFirst(t *testing.T) {
	s, _ := newState()
	_, body := get(t, s, "/healthz")
	if !strings.Contains(body, `"lastMessageAt":null`) {
		t.Errorf("body %s", body)
	}
}

func TestReadyz(t *testing.T) {
	s, _ := newState()
	if code, _ := get(t, s, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
	s.SetReady(true)
	if code, _ := get(t, s, "/readyz"); code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
}

func TestMetricsExposeNames(t *testing.T) {
	s, _ := newState()
	s.UpstreamConnected(true)
	s.Reconnected()
	s.StoreResult(nil)
	s.MessageStored("message", t0)
	s.Duplicate()
	s.Rejected()
	s.Unclaimed()
	s.Missed("dsh", 2)
	s.Delivered("dsh", "pull", 3)
	s.Sent("dsh", "ok")
	s.Position("dsh", "events", 7)
	s.Subscribers("dsh", "events", 1)
	s.Pruned(4)

	m := s.m
	for name, c := range map[string]prometheus.Collector{
		"envelopes":     m.envelopes.WithLabelValues("message"),
		"duplicates":    m.duplicates,
		"rejected":      m.rejected,
		"unclaimed":     m.unclaimed,
		"reconnects":    m.reconnects,
		"connected":     m.connected,
		"storeWritable": m.storeWritable,
	} {
		if got := testutil.ToFloat64(c); got != 1 {
			t.Errorf("%s = %v, want 1", name, got)
		}
	}
	want := map[string]struct {
		c prometheus.Collector
		v float64
	}{
		"missed":      {m.missed.WithLabelValues("dsh"), 2},
		"delivered":   {m.delivered.WithLabelValues("dsh", "pull"), 3},
		"sends":       {m.sends.WithLabelValues("dsh", "ok"), 1},
		"position":    {m.position.WithLabelValues("dsh", "events"), 7},
		"pruned":      {m.pruned, 4},
		"lastMessage": {m.lastMessage, float64(t0.Unix())},
	}
	for name, w := range want {
		if got := testutil.ToFloat64(w.c); got != w.v {
			t.Errorf("%s = %v, want %v", name, got, w.v)
		}
	}

	code, body := get(t, s, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	for _, name := range []string{
		"signal_hub_envelopes_total", "signal_hub_duplicates_total", "signal_hub_frames_rejected_total",
		"signal_hub_unclaimed_total", "signal_hub_missed_total", "signal_hub_delivered_total",
		"signal_hub_sends_total", "signal_hub_upstream_connected", "signal_hub_upstream_reconnects_total",
		"signal_hub_last_message_timestamp_seconds", "signal_hub_store_writable", "signal_hub_pruned_total",
		"signal_hub_consumer_position", "go_goroutines", "process_open_fds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metrics missing %q", name)
		}
	}
}

func TestSubscribersClampAtZero(t *testing.T) {
	s, _ := newState()
	s.Subscribers("dsh", "events", 1)
	s.Subscribers("dsh", "events", -3)
	if got := s.Snapshot().Consumers["dsh"]["events"].Subscribers; got != 0 {
		t.Errorf("subscribers = %d", got)
	}
	s.Subscribers("dsh", "events", 1)
	if got := s.Snapshot().Consumers["dsh"]["events"].Subscribers; got != 1 {
		t.Errorf("subscribers = %d", got)
	}
}

func TestConnectedSinceStableAcrossRepeats(t *testing.T) {
	s, c := newState()
	s.UpstreamConnected(true)
	c.now = t0.Add(time.Minute)
	s.UpstreamConnected(true)
	since := s.Snapshot().Upstream.ConnectedSince
	if since == nil || !since.Equal(t0) {
		t.Fatalf("connectedSince = %v", since)
	}
	s.UpstreamConnected(false)
	if s.Snapshot().Upstream.ConnectedSince != nil {
		t.Error("connectedSince set while disconnected")
	}
	c.now = t0.Add(2 * time.Minute)
	s.UpstreamConnected(true)
	if got := s.Snapshot().Upstream.ConnectedSince; got == nil || !got.Equal(c.now) {
		t.Errorf("connectedSince after reconnect = %v", got)
	}
}

func TestMessageStoredOlderDoesNotMoveBackwards(t *testing.T) {
	s, _ := newState()
	s.MessageStored("message", t0.Add(time.Minute))
	s.MessageStored("message", t0)
	snap := s.Snapshot()
	if snap.LastMessageAt == nil || !snap.LastMessageAt.Equal(t0.Add(time.Minute)) {
		t.Errorf("lastMessageAt = %v", snap.LastMessageAt)
	}
	if got := testutil.ToFloat64(s.m.lastMessage); got != float64(t0.Add(time.Minute).Unix()) {
		t.Errorf("gauge = %v", got)
	}
	if got := testutil.ToFloat64(s.m.envelopes.WithLabelValues("message")); got != 2 {
		t.Errorf("envelopes = %v", got)
	}
}

func TestTimestampsAreUTCWholeSeconds(t *testing.T) {
	s, c := newState()
	zone := time.FixedZone("x", 3600)
	c.now = time.Date(2026, 11, 14, 11, 0, 0, 123456789, zone)
	s.UpstreamConnected(true)
	s.MessageStored("message", c.now)
	_, body := get(t, s, "/healthz")
	m := decode(t, body)
	if m["lastMessageAt"] != "2026-11-14T10:00:00Z" {
		t.Errorf("lastMessageAt = %v", m["lastMessageAt"])
	}
	if m["upstream"].(map[string]any)["connectedSince"] != "2026-11-14T10:00:00Z" {
		t.Errorf("upstream = %v", m["upstream"])
	}
}

func TestNonGETReturns405(t *testing.T) {
	s, _ := newState()
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d", path, rec.Code)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	s, _ := newState()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer := "c" + string(rune('a'+g%4))
			for i := range 40 {
				s.SetReady(i%2 == 0)
				s.UpstreamConnected(i%3 != 0)
				s.Reconnected()
				s.StoreResult(nil)
				s.MessageStored("message", t0.Add(time.Duration(i)*time.Second))
				s.Duplicate()
				s.Rejected()
				s.Unclaimed()
				s.Missed(consumer, 1)
				s.Delivered(consumer, "pull", 1)
				s.Sent(consumer, "ok")
				s.Position(consumer, "events", int64(i))
				s.Subscribers(consumer, "events", 1)
				s.Subscribers(consumer, "events", -1)
				s.Pruned(1)
				s.Snapshot()
				get(t, s, "/metrics")
				get(t, s, "/healthz")
			}
		}()
	}
	wg.Wait()
	if got := s.Snapshot().Upstream.Reconnects; got != 16*40 {
		t.Errorf("reconnects = %d", got)
	}
}
