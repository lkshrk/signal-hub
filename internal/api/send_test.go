package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/lkshrk/signal-hub/internal/groupid"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

type sendCapture struct {
	mu     sync.Mutex
	path   string
	body   string
	status int
	reply  string
	hits   int
}

func (c *sendCapture) last() (string, string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.path, c.body, c.hits
}

func sendFixture(t *testing.T) (*fixture, *sendCapture) {
	t.Helper()
	f := newFixture(t)
	rec := &sendCapture{status: http.StatusOK, reply: `[{"timestamp":"1761245000123","errors":{}}]`}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.path, rec.body = r.URL.Path, string(raw)
		rec.hits++
		w.WriteHeader(rec.status)
		_, _ = w.Write([]byte(rec.reply))
	}))
	t.Cleanup(up.Close)
	f.srv.Upstream = upstream.New(up.URL, botNumber)
	return f, rec
}

func (f *fixture) post(t *testing.T, token, path, body string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.http.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response{res.StatusCode, res.Header, raw}
}

func jsonEq(t *testing.T, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("got %q: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if !bytes.Equal(gb, wb) {
		t.Errorf("got  %s\nwant %s", gb, wb)
	}
}

func restA() string { return groupid.REST(internalA) }

func TestSendTextToOwnGroupEitherForm(t *testing.T) {
	for _, recipient := range []string{internalA, restA()} {
		f, rec := sendFixture(t)
		res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+recipient+`","text":"hi"}`)
		if res.StatusCode != 200 {
			t.Fatalf("%d %s", res.StatusCode, res.Body)
		}
		jsonEq(t, string(res.Body), `{"timestamp":1761245000123,"timestamps":[1761245000123]}`)
		path, body, _ := rec.last()
		if path != "/v2/send" {
			t.Errorf("path = %s", path)
		}
		jsonEq(t, body, `{"number":"+4915112345678","recipients":["`+restA()+`"],"message":"hi"}`)
		if m := metricsText(t, f); !strings.Contains(m, `signal_hub_sends_total{consumer="dsh",result="ok"} 1`) {
			t.Errorf("metrics:\n%s", m)
		}
	}
}

func TestSendQuoteMentionsAndStyles(t *testing.T) {
	f, rec := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"@ look","quote":{"timestamp":1763114300000,"author":"+4915187654321","text":"orig"},"mentions":[{"start":0,"length":1,"author":"uuid-1"}]}`)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
	_, body, _ := rec.last()
	jsonEq(t, body, `{"number":"+4915112345678","recipients":["`+restA()+`"],"message":"@ look","quote_timestamp":1763114300000,"quote_author":"+4915187654321","quote_message":"orig","mentions":[{"start":0,"length":1,"author":"uuid-1"}]}`)

	res = f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"Done now","styles":[{"start":0,"length":4,"style":"BOLD"}]}`)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
	_, body, _ = rec.last()
	jsonEq(t, body, `{"number":"+4915112345678","recipients":["`+restA()+`"],"message":"**Done** now","text_mode":"styled"}`)
}

func TestSendRejectsMentionsWithStyles(t *testing.T) {
	f, rec := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"hi","mentions":[{"start":0,"length":1,"author":"u"}],"styles":[{"start":0,"length":1,"style":"BOLD"}]}`)
	assertError(t, res, 400, "mentions and styles cannot be combined")
	if _, _, hits := rec.last(); hits != 0 {
		t.Errorf("upstream hit")
	}
}

func assertError(t *testing.T, res response, status int, msg string) {
	t.Helper()
	if res.StatusCode != status {
		t.Fatalf("status = %d (%s), want %d", res.StatusCode, res.Body, status)
	}
	var e map[string]string
	if err := json.Unmarshal(res.Body, &e); err != nil || e["error"] != msg {
		t.Errorf("body = %s, want error %q", res.Body, msg)
	}
}

func TestSendRejectsBadStyles(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"abc","styles":[{"start":0,"length":1,"style":"UNDERLINE"}]}`)
	assertError(t, res, 400, `styles[0].style: unknown style "UNDERLINE"`)
	res = f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"abc","styles":[{"start":0,"length":9,"style":"BOLD"}]}`)
	assertError(t, res, 400, "styles[0]: range outside text")
}

func TestSendRejectsQuoteWithoutAuthor(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"x","quote":{"timestamp":5}}`)
	assertError(t, res, 400, "quote.author: required")
}

func TestSendAttachmentsBecomeDataURIs(t *testing.T) {
	f, rec := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","attachments":[{"contentType":"image/png","filename":"a.png","data":"aGk="}]}`)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
	_, body, _ := rec.last()
	jsonEq(t, body, `{"number":"+4915112345678","recipients":["`+restA()+`"],"message":"","base64_attachments":["data:image/png;filename=a.png;base64,aGk="]}`)
}

func TestSendRejectsInvalidAttachmentBase64(t *testing.T) {
	f, rec := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","attachments":[{"contentType":"image/png","filename":"a.png","data":"***"}]}`)
	assertError(t, res, 400, "attachments[0].data: invalid base64")
	if _, _, hits := rec.last(); hits != 0 {
		t.Errorf("upstream hit")
	}
}

func TestSendBodyOverLimitIs413(t *testing.T) {
	f, rec := sendFixture(t)
	f.srv.MaxBody = 1024
	big := `{"recipient":"` + internalA + `","text":"` + strings.Repeat("a", 2048) + `"}`
	res := f.post(t, dshToken, "/v1/send", big)
	assertError(t, res, 413, "request body exceeds 1024 bytes")
	if _, _, hits := rec.last(); hits != 0 {
		t.Errorf("upstream hit")
	}
}

func TestSendMalformedJSONIs400(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":`)
	assertError(t, res, 400, "invalid JSON body")
	res = f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"x","bogus":1}`)
	assertError(t, res, 400, "invalid JSON body")
}

func TestSendMissingRecipientIs400(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"text":"x"}`)
	assertError(t, res, 400, "recipient required")
}

func TestSendRequiresToken(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, "", "/v1/send", `{"recipient":"`+internalA+`","text":"x"}`)
	assertError(t, res, 401, "unauthorized")
}

func TestSendPollCreatesPoll(t *testing.T) {
	f, rec := sendFixture(t)
	rec.reply = `{"timestamp":"1769271479"}`
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","poll":{"question":"Merge?","options":["yes","no"],"allowMultiple":false}}`)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
	jsonEq(t, string(res.Body), `{"timestamp":1769271479,"timestamps":[1769271479]}`)
	path, body, _ := rec.last()
	if path != "/v1/polls/+4915112345678" {
		t.Errorf("path = %s", path)
	}
	jsonEq(t, body, `{"recipient":"`+restA()+`","question":"Merge?","answers":["yes","no"],"allow_multiple_selections":false}`)
}

func TestSendPollExcludesOtherContent(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"x","poll":{"question":"q","options":["a","b"]}}`)
	assertError(t, res, 400, "poll cannot be combined with text, quote, mentions, styles or attachments")
}

func TestSendPollNeedsQuestionAndTwoOptions(t *testing.T) {
	f, _ := sendFixture(t)
	for _, poll := range []string{
		`{"question":"","options":["a","b"]}`,
		`{"question":"q","options":["a"]}`,
		`{"question":"q","options":["a",""]}`,
	} {
		res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","poll":`+poll+`}`)
		assertError(t, res, 400, "poll: question and at least two options required")
	}
}

func TestSendDeniedRecipientIs403(t *testing.T) {
	f, rec := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalB+`","text":"secret text"}`)
	assertError(t, res, 403, "recipient not allowed")
	if _, _, hits := rec.last(); hits != 0 {
		t.Errorf("upstream hit")
	}
	if m := metricsText(t, f); !strings.Contains(m, `signal_hub_sends_total{consumer="dsh",result="denied"} 1`) {
		t.Errorf("metrics:\n%s", m)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, "send denied") || strings.Contains(logs, "secret text") {
		t.Errorf("logs:\n%s", logs)
	}
}

func TestSendRateLimitedIs429(t *testing.T) {
	f, rec := sendFixture(t)
	rec.status, rec.reply = http.StatusTooManyRequests, `{"error":"RateLimitException"}`
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"x"}`)
	assertError(t, res, 429, "rate limited")
	if m := metricsText(t, f); !strings.Contains(m, `signal_hub_sends_total{consumer="dsh",result="rate_limited"} 1`) {
		t.Errorf("metrics:\n%s", m)
	}
}

func TestSendUpstreamErrorIs502(t *testing.T) {
	f, rec := sendFixture(t)
	rec.status, rec.reply = http.StatusInternalServerError, "boom "+botNumber
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"private words"}`)
	assertError(t, res, 502, "upstream error")
	if m := metricsText(t, f); !strings.Contains(m, `signal_hub_sends_total{consumer="dsh",result="error"} 1`) {
		t.Errorf("metrics:\n%s", m)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, "send failed") || strings.Contains(logs, botNumber) || strings.Contains(logs, "private words") {
		t.Errorf("logs:\n%s", logs)
	}
}

func TestSendEmptyIs400(t *testing.T) {
	f, _ := sendFixture(t)
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`"}`)
	assertError(t, res, 400, "text, attachments or poll required")
}

func TestSendReportsFailures(t *testing.T) {
	f, rec := sendFixture(t)
	rec.reply = `[{"timestamp":"1761245000123","errors":{"recipients":[{"number":"+492","uuid":"","reason":"UNREGISTERED"}]}}]`
	res := f.post(t, dshToken, "/v1/send", `{"recipient":"+4915187654321","text":"x"}`)
	if res.StatusCode != 403 {
		t.Fatalf("sender outside config must be denied: %d %s", res.StatusCode, res.Body)
	}
	res = f.post(t, dshToken, "/v1/send", `{"recipient":"`+internalA+`","text":"x"}`)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, res.Body)
	}
	jsonEq(t, string(res.Body), `{"timestamp":1761245000123,"timestamps":[1761245000123],"failures":[{"number":"+492","uuid":"","reason":"UNREGISTERED"}]}`)
}
