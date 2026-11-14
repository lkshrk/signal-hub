package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/testupstream"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

const (
	groupDsh    = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	groupHermes = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	botNumber   = "+4915112345678"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type rig struct {
	ing    *Ingester
	store  *store.Store
	health *health.State
	logs   *syncBuf
	clock  *clock

	mu        sync.Mutex
	sleeps    []time.Duration
	stopAfter int
	cancel    context.CancelFunc
}

func frame(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "envelopes", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

func newRig(t *testing.T, url string) *rig {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	r := &rig{
		store: st,
		logs:  &syncBuf{},
		clock: &clock{t: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)},
	}
	r.health = health.New(r.clock.now)
	log := slog.New(slog.NewTextHandler(r.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	router := route.New([]config.Consumer{
		{Name: "dsh", GroupIDs: []string{groupDsh}},
		{Name: "hermes", GroupIDs: []string{groupHermes}},
	}, r.clock.now, log, r.health)
	r.ing = &Ingester{
		URL:          url,
		Store:        st,
		Router:       router,
		Health:       r.health,
		Log:          log,
		Backoff:      DefaultBackoff,
		Now:          r.clock.now,
		Rand:         func() float64 { return 0.5 },
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
		Sleep:        r.sleep,
	}
	return r
}

func (r *rig) sleep(ctx context.Context, d time.Duration) error {
	r.mu.Lock()
	r.sleeps = append(r.sleeps, d)
	if r.stopAfter > 0 && len(r.sleeps) >= r.stopAfter && r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func (r *rig) recorded() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.sleeps)
}

func (r *rig) start(t *testing.T) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, c := context.WithCancel(context.Background())
	r.mu.Lock()
	r.cancel = c
	r.mu.Unlock()
	ch := make(chan error, 1)
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		ch <- r.ing.Run(ctx, ctx)
	}()
	t.Cleanup(func() {
		c()
		select {
		case <-fin:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return c, ch
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (r *rig) waitHead(t *testing.T, n int64) {
	t.Helper()
	eventually(t, "store head", func() bool {
		h, err := r.store.Head(context.Background())
		return err == nil && h >= n
	})
}

func (r *rig) waitConnected(t *testing.T) {
	t.Helper()
	eventually(t, "upstream connected", func() bool { return r.health.Snapshot().Upstream.Connected })
}

func (r *rig) scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.health.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func (r *rig) metric(t *testing.T, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(r.scrape(t), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			var f float64
			if _, err := fmt.Sscan(v, &f); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return f
		}
	}
	return 0
}

func (r *rig) healthz(t *testing.T) int {
	t.Helper()
	rec := httptest.NewRecorder()
	r.health.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	return rec.Code
}

func (r *rig) records(t *testing.T, consumer string) []store.Record {
	t.Helper()
	p, err := r.store.Read(context.Background(), store.Query{Consumer: consumer})
	if err != nil {
		t.Fatal(err)
	}
	return p.Records
}

func countOf(s, sub string) int { return strings.Count(s, sub) }

func TestBackoffNext(t *testing.T) {
	b := DefaultBackoff
	cases := []struct {
		rnd  float64
		want []time.Duration
	}{
		{0.5, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}},
		{0, []time.Duration{800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 6400 * time.Millisecond, 12800 * time.Millisecond, 25600 * time.Millisecond, 30 * time.Second}},
		{1, []time.Duration{1200 * time.Millisecond, 2400 * time.Millisecond, 4800 * time.Millisecond, 9600 * time.Millisecond, 19200 * time.Millisecond, 30 * time.Second, 30 * time.Second}},
	}
	for _, c := range cases {
		for attempt, want := range c.want {
			got := b.Next(attempt, func() float64 { return c.rnd })
			if got != want {
				t.Errorf("rnd %v attempt %d: got %v, want %v", c.rnd, attempt, got, want)
			}
			if got > b.Max {
				t.Errorf("rnd %v attempt %d: %v exceeds max", c.rnd, attempt, got)
			}
		}
	}
	if got := b.Next(10_000, func() float64 { return 1 }); got != b.Max {
		t.Errorf("huge attempt: got %v, want %v", got, b.Max)
	}
}

func TestIngestStoresFramesInOrder(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(
		testupstream.Write(frame(t, "group_text"), frame(t, "group_hermes_text"), frame(t, "edit")),
		testupstream.Hold,
	))
	r := newRig(t, srv.URL())
	r.start(t)
	r.waitHead(t, 3)

	dsh := r.records(t, "dsh")
	if len(dsh) != 2 || dsh[0].Seq != 1 || dsh[0].Kind != "message" || dsh[1].Seq != 3 || dsh[1].Kind != "edit" {
		t.Fatalf("dsh records = %+v", dsh)
	}
	hermes := r.records(t, "hermes")
	if len(hermes) != 1 || hermes[0].Seq != 2 || hermes[0].Kind != "message" {
		t.Fatalf("hermes records = %+v", hermes)
	}
	if got := r.metric(t, `signal_hub_envelopes_total{kind="message"}`); got != 2 {
		t.Errorf("message envelopes = %v, want 2", got)
	}
	if got := r.metric(t, `signal_hub_envelopes_total{kind="edit"}`); got != 1 {
		t.Errorf("edit envelopes = %v, want 1", got)
	}
	if !bytes.Equal(dsh[0].Raw, frame(t, "group_text")) {
		t.Errorf("raw frame was not stored verbatim")
	}
}

func TestIngestReconnectsAfterDropAndDedupes(t *testing.T) {
	a, b, c := frame(t, "group_text"), frame(t, "group_hermes_text"), frame(t, "edit")
	d, e := frame(t, "reaction"), frame(t, "group_quote_mentions")
	srv := testupstream.New(t,
		testupstream.Seq(testupstream.Write(a, b, c), testupstream.Drop),
		testupstream.Seq(testupstream.Write(c, d, e), testupstream.Hold),
	)
	r := newRig(t, srv.URL())
	r.start(t)
	r.waitHead(t, 5)
	eventually(t, "duplicate counted", func() bool { return r.metric(t, "signal_hub_duplicates_total") == 1 })

	if got := r.metric(t, "signal_hub_upstream_reconnects_total"); got != 1 {
		t.Errorf("reconnects = %v, want 1", got)
	}
	if h, _ := r.store.Head(context.Background()); h != 5 {
		t.Errorf("head = %d, want 5", h)
	}
	logs := r.logs.String()
	if n := countOf(logs, `msg="upstream connected"`); n != 2 {
		t.Errorf("connected logs = %d, want 2\n%s", n, logs)
	}
	if !strings.Contains(logs, `level=WARN msg="upstream disconnected"`) {
		t.Errorf("missing disconnect warning\n%s", logs)
	}
}

func TestIngestBackoffGrowsWhenUpstreamClosesImmediately(t *testing.T) {
	srv := testupstream.New(t, testupstream.Drop)
	r := newRig(t, srv.URL())
	r.stopAfter = 7
	_, done := r.start(t)
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	if got := r.recorded(); !slices.Equal(got, want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
}

func TestIngestBackoffResetsAfterStableSession(t *testing.T) {
	release := make(chan struct{})
	srv := testupstream.New(t,
		testupstream.Drop,
		testupstream.Drop,
		testupstream.Seq(testupstream.Wait(release), testupstream.Drop),
		testupstream.Hold,
	)
	r := newRig(t, srv.URL())
	r.start(t)

	eventually(t, "third dial connected", func() bool {
		return srv.Dials() == 3 && r.health.Snapshot().Upstream.Connected
	})
	r.clock.advance(2 * time.Minute)
	close(release)
	eventually(t, "fourth dial", func() bool { return srv.Dials() == 4 })

	want := []time.Duration{time.Second, 2 * time.Second, time.Second}
	if got := r.recorded(); !slices.Equal(got, want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
}

func TestIngestDetectsHalfOpenConnection(t *testing.T) {
	srv := testupstream.New(t, testupstream.Stall, testupstream.Hold)
	r := newRig(t, srv.URL())
	r.ing.PingInterval = 50 * time.Millisecond
	r.ing.PingTimeout = 50 * time.Millisecond
	r.start(t)
	eventually(t, "second dial", func() bool { return srv.Dials() >= 2 })
}

func TestIngestRejectsFrameWithoutEnvelope(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(
		testupstream.Write(frame(t, "no_envelope"), frame(t, "group_text")),
		testupstream.Hold,
	))
	r := newRig(t, srv.URL())
	r.start(t)
	r.waitHead(t, 1)

	if h, _ := r.store.Head(context.Background()); h != 1 {
		t.Errorf("head = %d, want 1 (only the valid frame stored)", h)
	}
	if got := r.metric(t, "signal_hub_frames_rejected_total"); got != 1 {
		t.Errorf("rejected = %v, want 1", got)
	}
	if !strings.Contains(r.logs.String(), `level=WARN msg="frame rejected"`) {
		t.Errorf("missing rejection warning\n%s", r.logs.String())
	}
}

type flakyStore struct {
	inner   *store.Store
	failing atomic.Bool
	calls   atomic.Int64
}

func (f *flakyStore) Append(ctx context.Context, e store.Entry) (int64, bool, error) {
	f.calls.Add(1)
	if f.failing.Load() {
		return 0, false, errors.New("disk full")
	}
	return f.inner.Append(ctx, e)
}

func TestIngestRetriesStoreFailure(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frame(t, "group_text")), testupstream.Hold))
	r := newRig(t, srv.URL())
	fs := &flakyStore{inner: r.store}
	fs.failing.Store(true)
	r.ing.Store = fs
	r.start(t)

	eventually(t, "retries", func() bool { return fs.calls.Load() >= 3 })
	r.waitConnected(t)
	if got := r.metric(t, "signal_hub_store_writable"); got != 0 {
		t.Errorf("store_writable = %v while failing, want 0", got)
	}
	if code := r.healthz(t); code != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d while failing, want 503", code)
	}
	if got := r.health.Snapshot().Store.Error; got != "disk full" {
		t.Errorf("store error = %q", got)
	}

	fs.failing.Store(false)
	r.waitHead(t, 1)
	eventually(t, "store recovered", func() bool { return r.metric(t, "signal_hub_store_writable") == 1 })
	if code := r.healthz(t); code != http.StatusOK {
		t.Errorf("healthz = %d after recovery, want 200", code)
	}
	if got := r.recorded(); len(got) < 2 || got[0] != 100*time.Millisecond || got[1] != 200*time.Millisecond {
		t.Errorf("retry delays = %v, want 100ms then 200ms", got)
	}
	if !strings.Contains(r.logs.String(), `level=ERROR msg="store append failed"`) {
		t.Errorf("missing store failure log\n%s", r.logs.String())
	}
}

type orderStore struct {
	inner *store.Store
	logs  *syncBuf
	seen  atomic.Bool
}

func (o *orderStore) Append(ctx context.Context, e store.Entry) (int64, bool, error) {
	if strings.Contains(o.logs.String(), "unclaimed message") {
		o.seen.Store(true)
	}
	return o.inner.Append(ctx, e)
}

func TestIngestUnclaimedNotedAfterAppend(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frame(t, "group_unknown_text")), testupstream.Hold))
	r := newRig(t, srv.URL())
	ord := &orderStore{inner: r.store, logs: r.logs}
	r.ing.Store = ord
	r.start(t)
	r.waitHead(t, 1)
	eventually(t, "unclaimed log", func() bool { return strings.Contains(r.logs.String(), "unclaimed message") })

	if ord.seen.Load() {
		t.Error("unclaimed message was logged before the append")
	}
	if recs := r.records(t, ""); len(recs) != 1 || recs[0].Consumer != "" {
		t.Errorf("unclaimed records = %+v", recs)
	}
	if got := r.metric(t, "signal_hub_unclaimed_total"); got != 1 {
		t.Errorf("unclaimed = %v, want 1", got)
	}
}

func TestIngestDuplicateIsNotNotedAsUnclaimed(t *testing.T) {
	f := frame(t, "group_unknown_text")
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(f, f), testupstream.Hold))
	r := newRig(t, srv.URL())
	r.start(t)
	eventually(t, "duplicate counted", func() bool { return r.metric(t, "signal_hub_duplicates_total") == 1 })

	if got := r.metric(t, "signal_hub_unclaimed_total"); got != 1 {
		t.Errorf("unclaimed = %v, want 1", got)
	}
	if got := r.metric(t, `signal_hub_envelopes_total{kind="message"}`); got != 1 {
		t.Errorf("stored messages = %v, want 1", got)
	}
}

func TestIngestStopsOnContextCancel(t *testing.T) {
	srv := testupstream.New(t)
	r := newRig(t, srv.URL())
	cancel, done := r.start(t)
	r.waitConnected(t)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s")
	}
	select {
	case code := <-srv.Closed():
		if code != websocket.StatusGoingAway {
			t.Fatalf("close code = %d, want 1001", code)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream saw no close")
	}
	if r.health.Snapshot().Upstream.Connected {
		t.Error("still marked connected after stop")
	}
}

func TestIngestNeverLogsBotNumber(t *testing.T) {
	assertClean := func(t *testing.T, logs string) {
		t.Helper()
		for _, leak := range []string{botNumber, "4915112345678", url.PathEscape(botNumber)} {
			if strings.Contains(logs, leak) {
				t.Fatalf("logs leak the bot number (%q):\n%s", leak, logs)
			}
		}
	}

	t.Run("connected", func(t *testing.T) {
		srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frame(t, "group_text")), testupstream.Drop), testupstream.Hold)
		r := newRig(t, srv.URL()+"/v1/receive/"+url.PathEscape(botNumber))
		r.ing.Redact = upstream.New("http://127.0.0.1:1", botNumber).Redact
		r.start(t)
		eventually(t, "reconnect", func() bool { return srv.Dials() >= 2 })
		logs := r.logs.String()
		if !strings.Contains(logs, "upstream connected") || !strings.Contains(logs, "upstream disconnected") {
			t.Fatalf("expected lifecycle logs:\n%s", logs)
		}
		assertClean(t, logs)
	})

	t.Run("dial failure", func(t *testing.T) {
		dead := httptest.NewServer(http.NotFoundHandler())
		base := dead.URL
		dead.Close()
		c := upstream.New(base, botNumber)
		r := newRig(t, c.ReceiveURL())
		r.ing.Redact = c.Redact
		r.stopAfter = 2
		_, done := r.start(t)
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}
		logs := r.logs.String()
		if !strings.Contains(logs, "upstream dial failed") {
			t.Fatalf("expected a dial failure log:\n%s", logs)
		}
		assertClean(t, logs)
	})
}

func TestIngestDialTimeoutRetries(t *testing.T) {
	srv := testupstream.New(t)
	srv.StallUpgrades(1)
	r := newRig(t, srv.URL())
	r.ing.DialTimeout = 50 * time.Millisecond
	r.start(t)
	eventually(t, "second dial", func() bool { return srv.Dials() >= 2 })
	r.waitConnected(t)
	if got := r.recorded(); len(got) == 0 || got[0] != time.Second {
		t.Fatalf("sleeps = %v, want first 1s", got)
	}
}

func TestIngestMarksDisconnectedWhileStoreIsDown(t *testing.T) {
	f := frame(t, "group_text")
	frames := make([][]byte, 3*frameBuffer)
	for i := range frames {
		frames[i] = f
	}
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frames...), testupstream.Stall))
	r := newRig(t, srv.URL())
	fs := &flakyStore{inner: r.store}
	fs.failing.Store(true)
	r.ing.Store = fs
	r.ing.PingInterval = 50 * time.Millisecond
	r.ing.PingTimeout = 50 * time.Millisecond
	r.start(t)

	r.waitConnected(t)
	eventually(t, "upstream marked disconnected", func() bool { return !r.health.Snapshot().Upstream.Connected })
	if code := r.healthz(t); code != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, want 503", code)
	}
	if h, _ := r.store.Head(context.Background()); h != 0 {
		t.Errorf("head = %d while store is down", h)
	}
}

func TestIngestCancelDoesNotMarkStoreUnwritable(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frame(t, "group_text")), testupstream.Hold))
	r := newRig(t, srv.URL())
	r.ing.Store = blockingStore{}
	cancel, done := r.start(t)
	r.waitConnected(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	if got := r.health.Snapshot().Store.Error; got != "" {
		t.Errorf("store error after cancel = %q", got)
	}
}

type blockingStore struct{}

func (blockingStore) Append(ctx context.Context, _ store.Entry) (int64, bool, error) {
	<-ctx.Done()
	return 0, false, ctx.Err()
}

type gatedStore struct {
	inner *store.Store
	gate  chan struct{}
}

func (g *gatedStore) Append(ctx context.Context, e store.Entry) (int64, bool, error) {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
	return g.inner.Append(ctx, e)
}

func TestIngestStoresBufferedFramesAfterCancel(t *testing.T) {
	names := []string{"group_text", "group_hermes_text", "direct_attachment", "reaction", "edit"}
	frames := make([][]byte, len(names))
	for i, n := range names {
		frames[i] = frame(t, n)
	}
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frames...), testupstream.Hold))
	r := newRig(t, srv.URL())
	gs := &gatedStore{inner: r.store, gate: make(chan struct{})}
	r.ing.Store = gs
	var queued atomic.Int64
	r.ing.queued = func() { queued.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	storeCtx, cancelStore := context.WithCancel(context.Background())
	defer cancelStore()
	done := make(chan error, 1)
	go func() { done <- r.ing.Run(ctx, storeCtx) }()

	eventually(t, "frames queued", func() bool { return queued.Load() == int64(len(frames)) })
	cancel()
	close(gs.gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if h, err := r.store.Head(context.Background()); err != nil || h != int64(len(frames)) {
		t.Fatalf("head = %d, %v; want %d", h, err, len(frames))
	}
}

func TestIngestDrainEndsWithStoreContext(t *testing.T) {
	srv := testupstream.New(t, testupstream.Seq(testupstream.Write(frame(t, "group_text"), frame(t, "reaction")), testupstream.Hold))
	r := newRig(t, srv.URL())
	r.ing.Store = blockingStore{}
	var queued atomic.Int64
	r.ing.queued = func() { queued.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	storeCtx, cancelStore := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.ing.Run(ctx, storeCtx) }()

	eventually(t, "frames queued", func() bool { return queued.Load() == 2 })
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run = %v before the store context ended", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancelStore()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the store context ended")
	}
}
