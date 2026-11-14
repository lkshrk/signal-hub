package route

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/groupid"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
)

const (
	number = "+4915112345678"
	uuidLo = "1b4f0e2a-3c5d-4e6f-8a9b-0c1d2e3f4a5b"
	uuidUp = "1B4F0E2A-3C5D-4E6F-8A9B-0C1D2E3F4A5B"
)

func gid(b byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
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

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

type fixture struct {
	r     *Router
	clk   *clock
	logs  *bytes.Buffer
	state *health.State
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	internalA, internalB := gid('a'), gid('b')
	consumers := []config.Consumer{
		{Name: "dsh", GroupIDs: []string{internalA}, Token: strings.Repeat("d", 32)},
		{Name: "hermes", GroupIDs: []string{internalB}, Senders: []string{number, uuidUp}, Token: strings.Repeat("h", 32)},
		{Name: "compat", Senders: []string{"+4915100000000"}},
	}
	clk := &clock{t: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)}
	logs := &bytes.Buffer{}
	state := health.New(clk.now)
	return fixture{New(consumers, clk.now, slog.New(slog.NewJSONHandler(logs, nil)), state), clk, logs, state}
}

func TestRouteGroup(t *testing.T) {
	f := newFixture(t)
	got, ok := f.r.Route(message.Parsed{Kind: message.KindMessage, GroupID: gid('a')})
	if !ok || got != "dsh" {
		t.Fatalf("Route = (%q, %v), want (dsh, true)", got, ok)
	}
}

func TestRouteDirectBySenderNumberOrUUID(t *testing.T) {
	f := newFixture(t)
	for _, p := range []message.Parsed{
		{Kind: message.KindMessage, SourceNumber: number},
		{Kind: message.KindMessage, SourceUUID: uuidLo},
	} {
		if got, ok := f.r.Route(p); !ok || got != "hermes" {
			t.Fatalf("Route(%+v) = (%q, %v), want (hermes, true)", p, got, ok)
		}
	}
}

func TestGroupMessageIgnoresSenderList(t *testing.T) {
	f := newFixture(t)
	got, ok := f.r.Route(message.Parsed{Kind: message.KindMessage, GroupID: gid('a'), SourceNumber: number})
	if !ok || got != "dsh" {
		t.Fatalf("got (%q, %v), want (dsh, true)", got, ok)
	}
}

func TestRouteUnclaimed(t *testing.T) {
	f := newFixture(t)
	if got, ok := f.r.Route(message.Parsed{Kind: message.KindMessage, GroupID: gid('z')}); ok || got != "" {
		t.Fatalf("unknown group = (%q, %v)", got, ok)
	}
	if got, ok := f.r.Route(message.Parsed{Kind: message.KindMessage, SourceNumber: "+4915199999999"}); ok || got != "" {
		t.Fatalf("unknown sender = (%q, %v)", got, ok)
	}
}

func unclaimedLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		if m["msg"] == "unclaimed message" {
			out = append(out, m)
		}
	}
	return out
}

func TestUnclaimedLoggedOncePerKeyPerDay(t *testing.T) {
	f := newFixture(t)
	grp := message.Parsed{Kind: message.KindMessage, GroupID: gid('z')}
	for range 3 {
		f.r.NoteUnclaimed(grp)
	}
	lines := unclaimedLines(t, f.logs)
	if len(lines) != 1 || lines[0]["group"] != gid('z') || lines[0]["kind"] != message.KindMessage {
		t.Fatalf("lines = %v", lines)
	}
	if n := f.state.Snapshot().Unclaimed; n != 3 {
		t.Fatalf("unclaimed = %d, want 3", n)
	}

	f.clk.set(time.Date(2026, 11, 15, 0, 0, 1, 0, time.UTC))
	f.r.NoteUnclaimed(grp)
	if n := len(unclaimedLines(t, f.logs)); n != 2 {
		t.Fatalf("after day change lines = %d, want 2", n)
	}

	f.r.NoteUnclaimed(message.Parsed{Kind: message.KindMessage, SourceNumber: "+4915199999999"})
	lines = unclaimedLines(t, f.logs)
	if len(lines) != 3 || lines[2]["sender"] != "+4915199999999" {
		t.Fatalf("lines = %v", lines)
	}

	before := f.state.Snapshot().Unclaimed
	f.r.NoteUnclaimed(message.Parsed{Kind: message.KindReceipt, GroupID: gid('y')})
	if n := len(unclaimedLines(t, f.logs)); n != 3 || f.state.Snapshot().Unclaimed != before {
		t.Fatal("receipt must not log or count")
	}
}

func TestAllowedRecipients(t *testing.T) {
	f := newFixture(t)
	internalA, restA := gid('a'), groupid.REST(gid('a'))
	cases := []struct {
		consumer, recipient string
		want                string
		ok                  bool
	}{
		{"dsh", internalA, restA, true},
		{"dsh", restA, restA, true},
		{"dsh", gid('b'), "", false},
		{"hermes", number, number, true},
		{"hermes", uuidLo, uuidLo, true},
		{"hermes", "+4915100000001", "", false},
		{"dsh", number, "", false},
		{"nobody", internalA, "", false},
	}
	for _, c := range cases {
		got, ok := f.r.Allowed(c.consumer, c.recipient)
		if got != c.want || ok != c.ok {
			t.Errorf("Allowed(%q, %q) = (%q, %v), want (%q, %v)", c.consumer, c.recipient, got, ok, c.want, c.ok)
		}
	}
}

func TestByToken(t *testing.T) {
	f := newFixture(t)
	if got, ok := f.r.ByToken(strings.Repeat("h", 32)); !ok || got != "hermes" {
		t.Fatalf("got (%q, %v)", got, ok)
	}
	for _, tok := range []string{strings.Repeat("x", 32), "", strings.Repeat("h", 31)} {
		if got, ok := f.r.ByToken(tok); ok || got != "" {
			t.Fatalf("ByToken(%q) = (%q, %v)", tok, got, ok)
		}
	}
}

func TestConsumer(t *testing.T) {
	f := newFixture(t)
	if c, ok := f.r.Consumer("dsh"); !ok || c.Name != "dsh" {
		t.Fatalf("got (%+v, %v)", c, ok)
	}
	if _, ok := f.r.Consumer("nobody"); ok {
		t.Fatal("unknown consumer found")
	}
}

func TestInvalidGroupFailsClosed(t *testing.T) {
	f := newFixture(t)
	for _, raw := range []string{
		`{"envelope":{"sourceNumber":"` + number + `","timestamp":5,"dataMessage":{"message":"x","groupInfo":{"groupId":"not-a-group"}}}}`,
		`{"envelope":{"sourceNumber":"` + number + `","timestamp":5,"dataMessage":{"message":"x","groupInfo":{}}}}`,
	} {
		p, err := message.Parse([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := f.r.Route(p); ok || got != "" {
			t.Fatalf("Route = (%q, %v), want unclaimed", got, ok)
		}
		f.r.NoteUnclaimed(p)
	}
	lines := unclaimedLines(t, f.logs)
	if len(lines) != 1 || lines[0]["group"] != "invalid" || strings.Contains(f.logs.String(), "not-a-group") {
		t.Fatalf("lines = %v", lines)
	}
	if n := f.state.Snapshot().Unclaimed; n != 2 {
		t.Fatalf("unclaimed = %d, want 2", n)
	}
}

func TestRouteCanonicalVectors(t *testing.T) {
	const (
		internalA = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
		restA     = "group.QUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0="
	)
	cfgID, err := groupid.Normalize(restA)
	if err != nil || cfgID != internalA {
		t.Fatalf("Normalize(restA) = %q, %v", cfgID, err)
	}
	r := New([]config.Consumer{{Name: "dsh", GroupIDs: []string{cfgID}}}, time.Now, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), health.New(time.Now))
	if got, ok := r.Route(message.Parsed{Kind: message.KindMessage, GroupID: internalA}); !ok || got != "dsh" {
		t.Fatalf("Route = (%q, %v)", got, ok)
	}
	if got, ok := r.Allowed("dsh", internalA); !ok || got != restA {
		t.Fatalf("Allowed = (%q, %v)", got, ok)
	}
}

func TestUnclaimedKeyCap(t *testing.T) {
	f := newFixture(t)
	f.r.maxKeys = 2
	for i := range 5 {
		f.r.NoteUnclaimed(message.Parsed{Kind: message.KindMessage, SourceNumber: "+49151000000" + string(rune('0'+i))})
	}
	if n := len(unclaimedLines(t, f.logs)); n != 2 {
		t.Fatalf("key lines = %d, want 2", n)
	}
	if n := strings.Count(f.logs.String(), "unclaimed messages suppressed"); n != 1 {
		t.Fatalf("suppressed lines = %d, want 1", n)
	}
	if n := f.state.Snapshot().Unclaimed; n != 5 {
		t.Fatalf("unclaimed = %d, want 5", n)
	}
	f.clk.set(time.Date(2026, 11, 15, 0, 0, 1, 0, time.UTC))
	f.r.NoteUnclaimed(message.Parsed{Kind: message.KindMessage, SourceNumber: "+491510000009"})
	if n := len(unclaimedLines(t, f.logs)); n != 3 {
		t.Fatalf("after day change key lines = %d, want 3", n)
	}
}
