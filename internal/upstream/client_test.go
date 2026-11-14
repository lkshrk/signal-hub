package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const account = "+4915112345678"

type captured struct {
	method, path string
	body         map[string]any
}

func fake(t *testing.T, status int, resp string, ct ...string) (*Client, *captured) {
	t.Helper()
	c := &captured{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.method, c.path = r.Method, r.URL.EscapedPath()
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &c.body)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, account), c
}

func TestSendDecodesArrayResponse(t *testing.T) {
	c, got := fake(t, 201, `[{"timestamp":"1761245000123"}]`)
	q := int64(5)
	res, err := c.Send(context.Background(), SendRequest{Number: account, Recipients: []string{"+492"}, Message: "hi", QuoteTimestamp: &q})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Timestamp != 1761245000123 || len(res[0].Failures) != 0 {
		t.Fatalf("res = %+v", res)
	}
	if got.path != "/v2/send" || got.body["number"] != account || got.body["message"] != "hi" || got.body["quote_timestamp"] != float64(5) {
		t.Fatalf("request = %+v", got)
	}
	if _, ok := got.body["text_mode"]; ok {
		t.Fatalf("text_mode must be omitted: %v", got.body)
	}
}

func TestSendReportsRecipientFailures(t *testing.T) {
	c, _ := fake(t, 201, `[{"timestamp":"1","errors":{"recipients":[{"number":"+492","reason":"UNREGISTERED"}]}}]`)
	res, err := c.Send(context.Background(), SendRequest{Number: account, Recipients: []string{"+492"}, Message: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || len(res[0].Failures) != 1 || res[0].Failures[0] != (Failure{Number: "+492", Reason: "UNREGISTERED"}) {
		t.Fatalf("res = %+v", res)
	}
}

func TestSendEmptyArrayIsError(t *testing.T) {
	c, _ := fake(t, 201, `[]`)
	_, err := c.Send(context.Background(), SendRequest{})
	if err == nil || err.Error() != "decode send response: empty" {
		t.Fatalf("err = %v", err)
	}
}

func TestSendObjectResponseIsError(t *testing.T) {
	c, _ := fake(t, 201, `{"timestamp":"1"}`)
	_, err := c.Send(context.Background(), SendRequest{})
	if err == nil || !strings.HasPrefix(err.Error(), "decode send response") {
		t.Fatalf("err = %v", err)
	}
}

func TestRateLimitByStatusAndBody(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{"status": {429, `slow down`}, "body": {400, `{"error":"[RateLimitException]"}`}} {
		t.Run(name, func(t *testing.T) {
			c, _ := fake(t, tc.status, tc.body)
			_, err := c.Do(context.Background(), "POST", "/v2/send", []byte(`{}`))
			var rl *RateLimitError
			if !errors.As(err, &rl) || !strings.HasPrefix(rl.Msg, "rate limited: ") || rl.Error() != rl.Msg {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestRateLimitTextInSuccessBodyIsData(t *testing.T) {
	body := "binary\x00RateLimit\x01data"
	c, _ := fake(t, 200, body)
	out, err := c.Do(context.Background(), "GET", "/v1/attachments/a1", nil)
	if err != nil || string(out) != body {
		t.Fatalf("Do = %q, %v", out, err)
	}
}

func TestHTTPErrorFormat(t *testing.T) {
	c, _ := fake(t, 400, `{"error":"bad"}`)
	_, err := c.Do(context.Background(), "POST", "/v2/send", []byte(`{}`))
	if err == nil || err.Error() != `POST /v2/send returned HTTP 400: {"error":"bad"}` {
		t.Fatalf("err = %v", err)
	}
}

func TestCreatePollParsesStringTimestamp(t *testing.T) {
	c, got := fake(t, 201, `{"timestamp":"1769271479"}`)
	ts, err := c.CreatePoll(context.Background(), "+492", "q?", []string{"a", "b"}, true)
	if err != nil || ts != 1769271479 {
		t.Fatalf("ts = %d, err = %v", ts, err)
	}
	if got.method != "POST" || got.path != "/v1/polls/+4915112345678" || got.body["recipient"] != "+492" || got.body["question"] != "q?" ||
		got.body["allow_multiple_selections"] != true || len(got.body["answers"].([]any)) != 2 {
		t.Fatalf("request = %+v", got)
	}
}

func TestClosePollSendsStringTimestamp(t *testing.T) {
	c, got := fake(t, 200, ``)
	if err := c.ClosePoll(context.Background(), "+492", 1769271479); err != nil {
		t.Fatal(err)
	}
	if got.method != "DELETE" || got.path != "/v1/polls/+4915112345678" || got.body["recipient"] != "+492" || got.body["poll_timestamp"] != "1769271479" {
		t.Fatalf("request = %+v", got)
	}
}

func TestPollErrorsHideAccount(t *testing.T) {
	c, _ := fake(t, 400, `{"error":"bad"}`)
	for name, err := range map[string]error{
		"close": c.ClosePoll(context.Background(), "+492", 1),
		"create": func() error {
			_, err := c.CreatePoll(context.Background(), "+492", "q", []string{"a", "b"}, false)
			return err
		}(),
	} {
		if err == nil || strings.Contains(err.Error(), "4915112345678") || !strings.Contains(err.Error(), "/v1/polls/<account> returned HTTP 400") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	if _, err := c.Do(context.Background(), "DELETE", "/v1/polls/"+account, nil); err == nil || strings.Contains(err.Error(), "4915112345678") {
		t.Fatalf("raw account in path: %v", err)
	}
}

func TestTransportErrorHidesAccount(t *testing.T) {
	c, _ := fake(t, 200, ``)
	c.base = "http://127.0.0.1:1"
	_, err := c.Do(context.Background(), "GET", "/v1/typing-indicator/"+account, nil)
	if err == nil || strings.Contains(err.Error(), "4915112345678") {
		t.Fatalf("err = %v", err)
	}
}

func TestAttachmentStreams(t *testing.T) {
	const size = 50 << 20
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/attachments/a%2Fb" {
			t.Errorf("path = %s", r.URL.EscapedPath())
		}
		w.Header().Set("Content-Type", "image/png")
		chunk := make([]byte, 1<<20)
		for i := 0; i < size/len(chunk); i++ {
			_, _ = w.Write(chunk)
		}
	}))
	defer srv.Close()
	body, hdr, err := New(srv.URL, account).Attachment(context.Background(), "a/b")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	if hdr.Get("Content-Type") != "image/png" {
		t.Fatalf("header = %v", hdr)
	}
	if n, err := io.Copy(io.Discard, body); err != nil || n != size {
		t.Fatalf("n = %d, err = %v", n, err)
	}
}

func TestAttachmentHTTPError(t *testing.T) {
	c, _ := fake(t, 404, `nope`)
	_, _, err := c.Attachment(context.Background(), "x")
	if err == nil || err.Error() != "GET /v1/attachments/x returned HTTP 404: nope" {
		t.Fatalf("err = %v", err)
	}
}

func TestReceiveURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://x":   "ws://x/v1/receive/+4915112345678",
		"https://x/": "wss://x/v1/receive/+4915112345678",
	} {
		if got := New(in, account).ReceiveURL(); got != want {
			t.Fatalf("ReceiveURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTransportErrorKeepsCause(t *testing.T) {
	c, _ := fake(t, 200, ``)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Do(ctx, "GET", "/v1/about", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedactEmptyAccountIsIdentity(t *testing.T) {
	c := New("http://x", "")
	if got := c.Redact("GET /v1/about failed"); got != "GET /v1/about failed" {
		t.Fatalf("got %q", got)
	}
}

func TestRedactCoversEscapedForms(t *testing.T) {
	c := New("http://x", account)
	for _, in := range []string{account, "%2B4915112345678", "%2b4915112345678"} {
		if got := c.Redact("a " + in + " b"); got != "a <account> b" {
			t.Fatalf("Redact(%q) = %q", in, got)
		}
	}
}

func TestErrorBodyEchoingEscapedAccountIsRedacted(t *testing.T) {
	c, _ := fake(t, 400, `{"error":"no %2B4915112345678 or %2b4915112345678"}`)
	_, err := c.Do(context.Background(), "POST", "/v2/send", []byte(`{}`))
	if err == nil || strings.Contains(strings.ToLower(err.Error()), "4915112345678") {
		t.Fatalf("err = %v", err)
	}
}
