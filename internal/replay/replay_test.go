package replay

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
)

var now = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*Replayer, *bytes.Buffer) {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logs := &bytes.Buffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))
	clock := func() time.Time { return now }
	state := health.New(clock)
	consumers := []config.Consumer{{Name: "dsh", Token: strings.Repeat("d", 32), MaxReplayAge: config.Duration(7 * 24 * time.Hour)}}
	return &Replayer{Store: st, Router: route.New(consumers, clock, log, state), Health: state, Log: log, Now: clock}, logs
}

func add(t *testing.T, r *Replayer, ts int64, at time.Time) int64 {
	t.Helper()
	p := message.Parsed{Kind: message.KindMessage, SignalTS: ts, Source: "+4912345"}
	seq, _, err := r.Store.Append(context.Background(), store.Entry{Parsed: p, Consumer: "dsh", ReceivedAt: at, Raw: []byte(`{"envelope":{}}`)})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestReadMissedAndNext(t *testing.T) {
	r, logs := setup(t)
	add(t, r, 1, now.Add(-8*24*time.Hour))
	fresh := add(t, r, 2, now.Add(-time.Hour))
	res, err := r.Read(context.Background(), "dsh", "events", 0, message.Deliverable, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Missed != 1 || res.Reset || len(res.Records) != 1 || res.Records[0].Seq != fresh || res.Next != fresh {
		t.Fatalf("%+v", res)
	}
	if !strings.Contains(logs.String(), `"channel":"events"`) || !strings.Contains(logs.String(), "messages missed") {
		t.Fatalf("logs: %s", logs)
	}
}

func TestReadResetWhenCursorAheadOfHead(t *testing.T) {
	r, logs := setup(t)
	add(t, r, 1, now.Add(-time.Hour))
	add(t, r, 2, now.Add(-time.Hour))
	res, err := r.Read(context.Background(), "dsh", "compat", 99, message.Deliverable, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reset || len(res.Records) != 2 || res.Next != 2 {
		t.Fatalf("%+v", res)
	}
	if !strings.Contains(logs.String(), "cursor ahead of log") {
		t.Fatalf("logs: %s", logs)
	}
}
