package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/store"
)

type golden struct {
	Name          string          `json:"name"`
	Kind          string          `json:"kind"`
	Path          string          `json:"path"`
	Request       json.RawMessage `json:"request"`
	RawBody       string          `json:"rawBody"`
	UpstreamReply struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	} `json:"upstreamReply"`
	Setup       []json.RawMessage `json:"setup"`
	Frames      []string          `json:"frames"`
	Status      int               `json:"status"`
	ContentType string            `json:"contentType"`
	Response    string            `json:"response"`
	Upstream    []struct {
		Method string          `json:"method"`
		Path   string          `json:"path"`
		Body   json.RawMessage `json:"body"`
	} `json:"upstream"`
}

func loadGoldens(t *testing.T) []golden {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "golden", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 23 {
		t.Fatalf("%d goldens, want 23", len(paths))
	}
	out := make([]golden, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var g golden
		if err := json.Unmarshal(raw, &g); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out = append(out, g)
	}
	return out
}

func jsonEqual(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(va, vb)
}

func TestGoldens(t *testing.T) {
	for _, g := range loadGoldens(t) {
		t.Run(g.Name, func(t *testing.T) {
			f := newFixture(t, fixtureOpts{status: g.UpstreamReply.Status, body: g.UpstreamReply.Body})
			if g.Name == "rpc-get-attachment" {
				raw := envelopeFixture(t, "direct_attachment")
				p, err := message.Parse(raw)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := f.store.Append(context.Background(), store.Entry{Parsed: p, Consumer: "hermes", ReceivedAt: baseTime, Raw: raw}); err != nil {
					t.Fatal(err)
				}
			}
			for _, s := range g.Setup {
				if _, _, out := f.rpc(t, s); strings.Contains(out, `"error"`) {
					t.Fatalf("setup failed: %s", out)
				}
			}
			f.upstream.reset()

			var status int
			var ct, body string
			switch g.Kind {
			case "rpc":
				req := []byte(g.RawBody)
				if g.RawBody == "" {
					req = g.Request
				}
				status, ct, body = f.rpc(t, req)
			case "check":
				status, ct, body = f.get(t, g.Path)
			case "events":
				status, ct, body = f.events(t, g)
			default:
				t.Fatalf("kind %q", g.Kind)
			}
			if status != g.Status || ct != g.ContentType || body != g.Response {
				t.Fatalf("got %d %q %q\nwant %d %q %q", status, ct, body, g.Status, g.ContentType, g.Response)
			}

			calls := f.upstream.taken()
			if len(calls) != len(g.Upstream) {
				t.Fatalf("upstream calls %+v, want %+v", calls, g.Upstream)
			}
			for i, want := range g.Upstream {
				got := calls[i]
				if got.Method != want.Method || got.Path != want.Path || !jsonEqual(got.Body, want.Body) {
					t.Fatalf("call %d = %s %s %s, want %s %s %s", i, got.Method, got.Path, got.Body, want.Method, want.Path, want.Body)
				}
			}
		})
	}
}

func (f *fixture) events(t *testing.T, g golden) (int, string, string) {
	t.Helper()
	if len(g.Frames) == 0 {
		return f.get(t, g.Path)
	}
	st := f.openStream(t, g.Path)
	var out strings.Builder
	out.WriteString(st.next(t))
	f.awaitParked(t)
	// Goldens pin the wire format, so frames go to this consumer without routing.
	for _, fr := range g.Frames {
		p, err := message.Parse([]byte(fr))
		if err != nil {
			t.Fatal(err)
		}
		f.appendFor(t, p, f.srv.Consumer, []byte(fr), baseTime)
	}
	for range g.Frames {
		out.WriteString(st.next(t))
	}
	return st.res.StatusCode, st.res.Header.Get("Content-Type"), out.String()
}
