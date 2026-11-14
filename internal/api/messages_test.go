package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/message"
)

func TestUnauthorized(t *testing.T) {
	f := newFixture(t)
	for name, header := range map[string]string{"none": "", "wrong": "Bearer wrong", "basic": "Basic x", "empty": "Bearer "} {
		res := f.getAuth(t, header, "/v1/messages")
		body := string(res.Body)
		if res.StatusCode != http.StatusUnauthorized || strings.TrimSpace(body) != `{"error":"unauthorized"}` || res.Header.Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("%s: %d %q %q", name, res.StatusCode, body, res.Header.Get("WWW-Authenticate"))
		}
	}
}

func TestUnauthorizedNeverLogsToken(t *testing.T) {
	f := newFixture(t)
	f.get(t, "secret-token-value", "/v1/messages")
	f.get(t, dshToken, "/v1/messages")
	if strings.Contains(f.logs.String(), "secret-token-value") || strings.Contains(f.logs.String(), dshToken) {
		t.Fatalf("token in logs: %s", f.logs.String())
	}
}

func TestPullReturnsOnlyOwnDeliverableMessages(t *testing.T) {
	f := newFixture(t)
	at := baseTime.Add(-time.Hour)
	own := f.append(t, "dsh", "group_text", at)
	f.append(t, "dsh", "receipt", at)
	f.append(t, "hermes", "group_hermes_text", at)
	edit := f.append(t, "dsh", "edit", at)

	code, pb, raw := f.pull(t, dshToken, "?after=0")
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if got := seqsOf(t, pb); !reflect.DeepEqual(got, []int64{own, edit}) {
		t.Fatalf("seqs = %v", got)
	}
	if pb.Next != edit || pb.Missed != 0 || pb.Reset {
		t.Fatalf("%+v", pb)
	}
}

func TestPullWithoutAfterStartsAtHead(t *testing.T) {
	f := newFixture(t)
	at := baseTime.Add(-time.Hour)
	for range 3 {
		f.append(t, "dsh", "group_text", at)
	}
	code, pb, raw := f.pull(t, dshToken, "")
	if code != 200 || len(pb.Messages) != 0 || pb.Next != 3 {
		t.Fatalf("%d %s", code, raw)
	}
	if !strings.Contains(raw, `"messages":[]`) {
		t.Fatalf("messages must be an empty array: %s", raw)
	}
}

func TestPullNextAdvancesPastSkippedRows(t *testing.T) {
	f := newFixture(t)
	at := baseTime.Add(-time.Hour)
	f.append(t, "dsh", "receipt", at)
	f.append(t, "dsh", "typing_group", at)
	_, pb, _ := f.pull(t, dshToken, "?after=0")
	if len(pb.Messages) != 0 || pb.Next != 2 {
		t.Fatalf("%+v", pb)
	}
}

func TestPullAfterZeroReplaysWithinMaxReplayAge(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "group_text", baseTime.Add(-8*24*time.Hour))
	fresh := f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))

	code, pb, raw := f.pull(t, dshToken, "?after=0")
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if got := seqsOf(t, pb); !reflect.DeepEqual(got, []int64{fresh}) || pb.Missed != 1 {
		t.Fatalf("seqs=%v missed=%d", got, pb.Missed)
	}
	logs := f.logs.String()
	for _, want := range []string{`"msg":"messages missed"`, `"consumer":"dsh"`, `"channel":"pull"`, `"count":1`} {
		if !strings.Contains(logs, want) {
			t.Errorf("log missing %s: %s", want, logs)
		}
	}
	if m := metricsText(t, f); !strings.Contains(m, `signal_hub_missed_total{consumer="dsh"} 1`) {
		t.Errorf("metric missing:\n%s", m)
	}
}

func TestPullLongPollWakesOnAppend(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _, _ = f.store.Append(context.Background(), appendEntry(t, f, "dsh"))
	}()
	code, pb, raw := f.pull(t, dshToken, "?after=0&wait=5")
	if code != 200 || len(pb.Messages) != 1 {
		t.Fatalf("%d %s", code, raw)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s", d)
	}
}

func TestPullLongPollIgnoresOtherConsumersAppend(t *testing.T) {
	f := newFixture(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _, _ = f.store.Append(context.Background(), appendEntry(t, f, "hermes"))
	}()
	start := time.Now()
	code, pb, raw := f.pull(t, dshToken, "?after=0&wait=1")
	if code != 200 || len(pb.Messages) != 0 || pb.Next != 0 {
		t.Fatalf("%d %s", code, raw)
	}
	if time.Since(start) < time.Second {
		t.Fatalf("returned early")
	}
}

func TestPullLongPollTimesOut(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	code, pb, raw := f.pull(t, dshToken, "?after=0&wait=1")
	if code != 200 || len(pb.Messages) != 0 || pb.Next != 0 || !strings.Contains(raw, `"messages":[]`) {
		t.Fatalf("%d %s", code, raw)
	}
	if time.Since(start) < time.Second {
		t.Fatalf("elapsed %s", time.Since(start))
	}
}

func TestPullParamValidation(t *testing.T) {
	f := newFixture(t)
	for _, q := range []string{"?after=x", "?after=-1", "?wait=x", "?limit=x"} {
		code, _, raw := f.pull(t, dshToken, q)
		if code != 400 || !strings.Contains(raw, `"error"`) {
			t.Errorf("%s: %d %s", q, code, raw)
		}
	}
	_, _, raw := f.pull(t, dshToken, "?after=x")
	if strings.TrimSpace(raw) != `{"error":"after: must be a non-negative integer"}` {
		t.Errorf("raw = %s", raw)
	}
}

func TestParsePullClamps(t *testing.T) {
	cases := []struct {
		q     string
		wait  time.Duration
		limit int
	}{
		{"wait=600", 60 * time.Second, 100},
		{"wait=-5", 0, 100},
		{"wait=30", 30 * time.Second, 100},
		{"limit=0", 0, 1},
		{"limit=-3", 0, 1},
		{"limit=9999", 0, 500},
		{"limit=20", 0, 20},
		{"wait=99999999999999", 60 * time.Second, 100},
	}
	for _, c := range cases {
		v, _ := url.ParseQuery(c.q)
		p, err := parsePull(v, 60*time.Second)
		if err != nil || p.wait != c.wait || p.limit != c.limit {
			t.Errorf("%s: %+v %v", c.q, p, err)
		}
	}
}

func TestPullWaitClampedToMaxWait(t *testing.T) {
	f := newFixture(t)
	f.srv.MaxWait = 300 * time.Millisecond
	start := time.Now()
	code, _, _ := f.pull(t, dshToken, "?after=0&wait=600")
	if d := time.Since(start); code != 200 || d < 300*time.Millisecond || d > 3*time.Second {
		t.Fatalf("%d after %s", code, d)
	}
}

func TestPullLimit(t *testing.T) {
	f := newFixture(t)
	for range 3 {
		f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	}
	_, pb, _ := f.pull(t, dshToken, "?after=0&limit=0")
	if len(pb.Messages) != 1 || pb.Next != 1 {
		t.Fatalf("%+v", pb)
	}
	_, pb, _ = f.pull(t, dshToken, "?after=1&limit=2")
	if len(pb.Messages) != 2 || pb.Next != 3 {
		t.Fatalf("%+v", pb)
	}
}

func TestPullResetWhenCursorAheadOfHead(t *testing.T) {
	f := newFixture(t)
	at := baseTime.Add(-time.Hour)
	for range 3 {
		f.append(t, "dsh", "group_text", at)
	}
	code, pb, raw := f.pull(t, dshToken, "?after=999")
	if code != 200 || !pb.Reset || !reflect.DeepEqual(seqsOf(t, pb), []int64{1, 2, 3}) || pb.Next != 3 {
		t.Fatalf("%d %s", code, raw)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"msg":"cursor ahead of log"`) || !strings.Contains(logs, `"consumer":"dsh"`) {
		t.Fatalf("logs: %s", logs)
	}
}

func TestPullResetOnEmptyLog(t *testing.T) {
	f := newFixture(t)
	code, pb, raw := f.pull(t, dshToken, "?after=5")
	if code != 200 || !pb.Reset || pb.Next != 0 {
		t.Fatalf("%d %s", code, raw)
	}
}

func TestPullMessageMatchesNeutralFormat(t *testing.T) {
	f := newFixture(t)
	at := baseTime.Add(-time.Hour).Truncate(time.Millisecond)
	seq := f.append(t, "dsh", "group_text", at)
	_, pb, _ := f.pull(t, dshToken, "?after=0")
	m, err := message.Build(seq, at, fixtureBytes(t, "group_text"))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(m)
	var a, b any
	if err := json.Unmarshal(pb.Messages[0], &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("got  %s\nwant %s", pb.Messages[0], want)
	}
}

func TestPullCountsDelivered(t *testing.T) {
	f := newFixture(t)
	f.append(t, "dsh", "group_text", baseTime.Add(-time.Hour))
	f.pull(t, dshToken, "?after=0")
	if m := metricsText(t, f); !strings.Contains(m, `consumer="dsh"`) || !strings.Contains(m, `channel="pull"`) {
		t.Fatalf("delivered metric missing:\n%s", m)
	}
}
