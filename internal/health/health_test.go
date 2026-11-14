package health

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
	s.SetReady(true)
	s.UpstreamConnected(true)
	s.Reconnected()
	s.StoreResult(nil)
	s.MessageStored("message", t0)
	s.Duplicate()
	s.Rejected()
	s.Unclaimed()
	s.Missed("dsh", 1)
	s.Delivered("dsh", "pull", 1)
	s.Sent("dsh", "ok")
	s.Position("dsh", "events", 7)
	s.Subscribers("dsh", "events", 1)
	s.Pruned(3)

	code, body := get(t, s, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	for _, name := range []string{
		`signal_hub_envelopes_total{kind="message"} 1`,
		"signal_hub_duplicates_total 1",
		"signal_hub_frames_rejected_total 1",
		"signal_hub_unclaimed_total 1",
		`signal_hub_missed_total{consumer="dsh"} 1`,
		`signal_hub_delivered_total{channel="pull",consumer="dsh"} 1`,
		`signal_hub_sends_total{consumer="dsh",result="ok"} 1`,
		"signal_hub_upstream_connected 1",
		"signal_hub_upstream_reconnects_total 1",
		"signal_hub_last_message_timestamp_seconds",
		"signal_hub_store_writable 1",
		"signal_hub_pruned_total 3",
		`signal_hub_consumer_position{channel="events",consumer="dsh"} 7`,
		"go_goroutines",
		"process_open_fds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metrics missing %q", name)
		}
	}
}
