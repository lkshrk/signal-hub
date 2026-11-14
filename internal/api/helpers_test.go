package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	internalA = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	internalB = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	botNumber = "+4915112345678"
)

var (
	dshToken    = strings.Repeat("d", 32)
	hermesToken = strings.Repeat("h", 32)
	baseTime    = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
)

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

type fixture struct {
	srv      *Server
	http     *httptest.Server
	store    *store.Store
	logs     *lockedBuffer
	upstream *fakeUpstream
	nextTS   int64
}

type fakeUpstream struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	body   []byte
	hits   int
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{status: http.StatusOK}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.hits++
		if f.status != http.StatusOK {
			http.Error(w, "boom "+botNumber, f.status)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)))
		_, _ = w.Write(f.body)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeUpstream) set(status int, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

func (f *fakeUpstream) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &lockedBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	now := func() time.Time { return baseTime }
	state := health.New(now)
	consumers := []config.Consumer{
		{Name: "dsh", GroupIDs: []string{internalA}, Token: dshToken, MaxReplayAge: config.Duration(7 * 24 * time.Hour)},
		{Name: "hermes", GroupIDs: []string{internalB}, Token: hermesToken, MaxReplayAge: config.Duration(7 * 24 * time.Hour)},
	}
	up := newFakeUpstream(t)
	srv := &Server{
		Store:    st,
		Router:   route.New(consumers, now, log, state),
		Upstream: upstream.New(up.URL, botNumber),
		Health:   state,
		Log:      log,
		Now:      now,
		MaxBody:  1 << 20,
		MaxWait:  60 * time.Second,
	}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return &fixture{srv: srv, http: hs, store: st, logs: logs, upstream: up, nextTS: 1_000_000_000_000}
}

func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "envelopes", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (f *fixture) append(t *testing.T, consumer, name string, at time.Time) int64 {
	t.Helper()
	raw := fixtureBytes(t, name)
	p, err := message.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	f.nextTS++
	p.SignalTS = f.nextTS
	seq, dup, err := f.store.Append(context.Background(), store.Entry{Parsed: p, Consumer: consumer, ReceivedAt: at, Raw: raw})
	if err != nil || dup {
		t.Fatalf("append: seq=%d dup=%v err=%v", seq, dup, err)
	}
	return seq
}

type response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (f *fixture) get(t *testing.T, token, path string) response {
	t.Helper()
	if token == "" {
		return f.getAuth(t, "", path)
	}
	return f.getAuth(t, "Bearer "+token, path)
}

func (f *fixture) getAuth(t *testing.T, authorization, path string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, f.http.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response{res.StatusCode, res.Header, body}
}

type pullBody struct {
	Messages []json.RawMessage `json:"messages"`
	Next     int64             `json:"next"`
	Missed   int               `json:"missed"`
	Reset    bool              `json:"reset"`
}

func (f *fixture) pull(t *testing.T, token, query string) (int, pullBody, string) {
	t.Helper()
	res := f.get(t, token, "/v1/messages"+query)
	raw := res.Body
	var pb pullBody
	if res.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &pb); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	return res.StatusCode, pb, string(raw)
}

func seqsOf(t *testing.T, pb pullBody) []int64 {
	t.Helper()
	out := []int64{}
	for _, m := range pb.Messages {
		var v struct {
			Seq int64 `json:"seq"`
		}
		if err := json.Unmarshal(m, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v.Seq)
	}
	return out
}

func metricsText(t *testing.T, f *fixture) string {
	t.Helper()
	rec := httptest.NewRecorder()
	f.srv.Health.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func appendEntry(t *testing.T, f *fixture, consumer string) store.Entry {
	t.Helper()
	name := "group_text"
	if consumer == "hermes" {
		name = "group_hermes_text"
	}
	raw := fixtureBytes(t, name)
	p, err := message.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	p.SignalTS = time.Now().UnixNano()
	return store.Entry{Parsed: p, Consumer: consumer, ReceivedAt: baseTime.Add(-time.Minute), Raw: raw}
}
