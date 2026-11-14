package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lkshrk/signal-hub/internal/upstream"
)

const account = "+4915112345678"

func validationText(t *testing.T, err error) string {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	return ve.Error()
}

func TestRequestPlainText(t *testing.T) {
	req, err := Request(account, "rest", Message{Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(req)
	want := `{"number":"+4915112345678","recipients":["rest"],"message":"hi"}`
	if string(raw) != want {
		t.Errorf("got %s", raw)
	}
}

func TestRequestStylesRenderStyledMode(t *testing.T) {
	req, err := Request(account, "rest", Message{Text: "Done now", Styles: []Style{{Start: 0, Length: 4, Style: "BOLD"}}})
	if err != nil {
		t.Fatal(err)
	}
	if req.Message != "**Done** now" || req.TextMode == nil || *req.TextMode != "styled" {
		t.Errorf("message=%q mode=%v", req.Message, req.TextMode)
	}
}

func TestRequestInexpressibleStylesSendUnstyled(t *testing.T) {
	req, err := Request(account, "rest", Message{Text: "a*b", Styles: []Style{{Start: 2, Length: 1, Style: "ITALIC"}, {Start: 0, Length: 2, Style: "ITALIC"}}})
	if err != nil {
		t.Fatal(err)
	}
	if req.TextMode != nil || req.Message != "a*b" {
		t.Errorf("message=%q mode=%v", req.Message, req.TextMode)
	}
}

func TestRequestStyleErrors(t *testing.T) {
	cases := []struct {
		name string
		m    Message
		want string
	}{
		{"unknown", Message{Text: "abc", Styles: []Style{{0, 1, "UNDERLINE"}}}, `styles[0].style: unknown style "UNDERLINE"`},
		{"past end", Message{Text: "abc", Styles: []Style{{0, 9, "BOLD"}}}, "styles[0]: range outside text"},
		{"negative start", Message{Text: "abc", Styles: []Style{{-1, 1, "BOLD"}}}, "styles[0]: range outside text"},
		{"zero length", Message{Text: "abc", Styles: []Style{{0, 0, "BOLD"}}}, "styles[0]: range outside text"},
		{"second", Message{Text: "abc", Styles: []Style{{0, 1, "BOLD"}, {2, 5, "BOLD"}}}, "styles[1]: range outside text"},
		{"utf16 units", Message{Text: "a\U0001F600b", Styles: []Style{{0, 5, "BOLD"}}}, "styles[0]: range outside text"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Request(account, "rest", c.m)
			if got := validationText(t, err); got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestRequestUTF16RangeAccepted(t *testing.T) {
	req, err := Request(account, "rest", Message{Text: "a\U0001F600b", Styles: []Style{{Start: 0, Length: 4, Style: "BOLD"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(req.Message, "**") {
		t.Errorf("message = %q", req.Message)
	}
}

func TestRequestMentionsAndStylesConflict(t *testing.T) {
	_, err := Request(account, "rest", Message{
		Text:     "hi",
		Mentions: []Mention{{0, 1, "u"}},
		Styles:   []Style{{0, 1, "BOLD"}},
	})
	if got := validationText(t, err); got != "mentions and styles cannot be combined" {
		t.Errorf("got %q", got)
	}
}

func TestRequestQuoteAndMentions(t *testing.T) {
	req, err := Request(account, "rest", Message{
		Text:     "@ hi",
		Quote:    &Quote{Timestamp: 1763114300000, Author: "+4915187654321", Text: "orig"},
		Mentions: []Mention{{0, 1, "uuid-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.QuoteTimestamp == nil || *req.QuoteTimestamp != 1763114300000 || *req.QuoteAuthor != "+4915187654321" || *req.QuoteMessage != "orig" {
		t.Errorf("quote = %+v", req)
	}
	if len(req.Mentions) != 1 || req.Mentions[0].Author != "uuid-1" || req.Mentions[0].Length != 1 {
		t.Errorf("mentions = %+v", req.Mentions)
	}
}

func TestRequestQuoteWithoutTextOmitsQuoteMessage(t *testing.T) {
	req, err := Request(account, "rest", Message{Text: "x", Quote: &Quote{Timestamp: 5, Author: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if req.QuoteMessage != nil {
		t.Errorf("quote message = %q", *req.QuoteMessage)
	}
}

func TestRequestQuoteNeedsAuthor(t *testing.T) {
	_, err := Request(account, "rest", Message{Text: "x", Quote: &Quote{Timestamp: 5}})
	if got := validationText(t, err); got != "quote.author: required" {
		t.Errorf("got %q", got)
	}
}

func TestRequestAttachmentsBecomeDataURIs(t *testing.T) {
	req, err := Request(account, "rest", Message{Attachments: []File{
		{ContentType: "image/png", Filename: "a.png", Data: "aGk="},
		{ContentType: "text/plain", Data: "aGk="},
		{Filename: "b.bin", Data: "aGk="},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"data:image/png;filename=a.png;base64,aGk=",
		"data:text/plain;base64,aGk=",
		"data:application/octet-stream;filename=b.bin;base64,aGk=",
	}
	if strings.Join(req.Base64Attachments, "|") != strings.Join(want, "|") {
		t.Errorf("got %v", req.Base64Attachments)
	}
}

func TestRequestInvalidBase64(t *testing.T) {
	_, err := Request(account, "rest", Message{Attachments: []File{{ContentType: "image/png", Data: "aGk="}, {ContentType: "image/png", Data: "!!!"}}})
	if got := validationText(t, err); got != "attachments[1].data: invalid base64" {
		t.Errorf("got %q", got)
	}
}

func TestValidateContentRules(t *testing.T) {
	cases := []struct {
		name string
		m    Message
		want string
	}{
		{"empty", Message{}, "text, attachments or poll required"},
		{"poll with text", Message{Text: "x", Poll: &Poll{Question: "q", Options: []string{"a", "b"}}}, "poll cannot be combined with text, quote, mentions, styles or attachments"},
		{"poll with attachment", Message{Attachments: []File{{Data: "aGk="}}, Poll: &Poll{Question: "q", Options: []string{"a", "b"}}}, "poll cannot be combined with text, quote, mentions, styles or attachments"},
		{"poll no question", Message{Poll: &Poll{Options: []string{"a", "b"}}}, "poll: question and at least two options required"},
		{"poll blank question", Message{Poll: &Poll{Question: "  ", Options: []string{"a", "b"}}}, "poll: question and at least two options required"},
		{"poll one option", Message{Poll: &Poll{Question: "q", Options: []string{"a"}}}, "poll: question and at least two options required"},
		{"poll blank option", Message{Poll: &Poll{Question: "q", Options: []string{"a", ""}}}, "poll: question and at least two options required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := validationText(t, Validate(c.m)); got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
	if err := Validate(Message{Poll: &Poll{Question: "q", Options: []string{"a", "b"}}}); err != nil {
		t.Errorf("valid poll: %v", err)
	}
}

func TestKind(t *testing.T) {
	if Kind(nil) != ResultOK {
		t.Error("nil")
	}
	if Kind(&upstream.RateLimitError{Msg: "x"}) != ResultRateLimited {
		t.Error("rate limit")
	}
	if Kind(errors.New("boom")) != ResultError {
		t.Error("other")
	}
	if Kind(&ValidationError{Msg: "bad"}) != ResultError {
		t.Error("validation")
	}
}

func TestDeliverSend(t *testing.T) {
	var path string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`[{"timestamp":"1761245000123","errors":{"recipients":[{"number":"+492","uuid":"","reason":"UNREGISTERED"}]}}]`))
	}))
	defer srv.Close()
	c := upstream.New(srv.URL, account)

	res, err := Deliver(context.Background(), c, "rest", Message{Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v2/send" || !strings.Contains(string(body), `"message":"hi"`) {
		t.Errorf("%s %s", path, body)
	}
	if res.Timestamp != 1761245000123 || len(res.Timestamps) != 1 || len(res.Failures) != 1 || res.Failures[0].Reason != "UNREGISTERED" {
		t.Errorf("res = %+v", res)
	}
}

func TestDeliverPoll(t *testing.T) {
	var path string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"timestamp":"1769271479"}`))
	}))
	defer srv.Close()
	c := upstream.New(srv.URL, account)

	res, err := Deliver(context.Background(), c, "rest", Message{Poll: &Poll{Question: "Merge?", Options: []string{"yes", "no"}}})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/polls/+4915112345678" || !strings.Contains(string(body), `"answers":["yes","no"]`) {
		t.Errorf("%s %s", path, body)
	}
	if res.Timestamp != 1769271479 || len(res.Timestamps) != 1 || len(res.Failures) != 0 {
		t.Errorf("res = %+v", res)
	}
}

func TestDeliverValidationFailsBeforeUpstream(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer srv.Close()
	_, err := Deliver(context.Background(), upstream.New(srv.URL, account), "rest", Message{})
	validationText(t, err)
	if hits != 0 {
		t.Errorf("upstream hit %d times", hits)
	}
}
