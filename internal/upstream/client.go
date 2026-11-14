package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponse = 128 << 20

type RateLimitError struct{ Msg string }

func (e *RateLimitError) Error() string { return e.Msg }

type Mention struct {
	Start  int64  `json:"start"`
	Length int64  `json:"length"`
	Author string `json:"author"`
}

type SendRequest struct {
	Number            string    `json:"number"`
	Recipients        []string  `json:"recipients"`
	Message           string    `json:"message"`
	Base64Attachments []string  `json:"base64_attachments,omitempty"`
	Mentions          []Mention `json:"mentions,omitempty"`
	QuoteTimestamp    *int64    `json:"quote_timestamp,omitempty"`
	QuoteAuthor       *string   `json:"quote_author,omitempty"`
	QuoteMessage      *string   `json:"quote_message,omitempty"`
	TextMode          *string   `json:"text_mode,omitempty"`
}

type Failure struct {
	Number string `json:"number"`
	UUID   string `json:"uuid"`
	Reason string `json:"reason"`
}

type SendResult struct {
	Timestamp int64
	Failures  []Failure
}

type Client struct {
	base    string
	account string
	http    *http.Client
	stream  *http.Client
	redact  *strings.Replacer
}

func New(baseURL, account string) *Client {
	return &Client{
		base:    strings.TrimRight(baseURL, "/"),
		account: account,
		http:    &http.Client{Timeout: 60 * time.Second},
		stream:  &http.Client{Timeout: 5 * time.Minute},
		redact:  strings.NewReplacer(url.PathEscape(account), "<account>", account, "<account>"),
	}
}

func (c *Client) Account() string { return c.account }

func (c *Client) ReceiveURL() string {
	return strings.Replace(c.base, "http", "ws", 1) + "/v1/receive/" + url.PathEscape(c.account)
}

func (c *Client) Do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	res, err := c.roundTrip(ctx, c.http, method, path, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	out, _ := io.ReadAll(io.LimitReader(res.Body, maxResponse))
	if err := c.statusError(method, path, res.StatusCode, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) DoJSON(ctx context.Context, method, path string, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return c.Do(ctx, method, path, raw)
}

func (c *Client) About(ctx context.Context) ([]byte, error) {
	return c.Do(ctx, http.MethodGet, "/v1/about", nil)
}

func (c *Client) Send(ctx context.Context, req SendRequest) ([]SendResult, error) {
	raw, err := c.DoJSON(ctx, http.MethodPost, "/v2/send", req)
	if err != nil {
		return nil, err
	}
	var resp []struct {
		Timestamp string `json:"timestamp"`
		Errors    struct {
			Recipients []Failure `json:"recipients"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode send response: %w", err)
	}
	if len(resp) == 0 {
		return nil, errors.New("decode send response: empty")
	}
	results := make([]SendResult, len(resp))
	for i, r := range resp {
		ts, err := strconv.ParseInt(r.Timestamp, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode send timestamp %q: %w", r.Timestamp, err)
		}
		results[i] = SendResult{Timestamp: ts, Failures: r.Errors.Recipients}
	}
	return results, nil
}

func (c *Client) CreatePoll(ctx context.Context, recipient, question string, options []string, multi bool) (int64, error) {
	body := map[string]any{"recipient": recipient, "question": question, "answers": options, "allow_multiple_selections": multi}
	raw, err := c.DoJSON(ctx, http.MethodPost, "/v1/polls/"+url.PathEscape(c.account), body)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return 0, fmt.Errorf("decode poll response: %w", err)
	}
	ts, err := strconv.ParseInt(resp.Timestamp, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("decode poll timestamp %q: %w", resp.Timestamp, err)
	}
	return ts, nil
}

func (c *Client) ClosePoll(ctx context.Context, recipient string, ts int64) error {
	body := map[string]any{"recipient": recipient, "poll_timestamp": strconv.FormatInt(ts, 10)}
	_, err := c.DoJSON(ctx, http.MethodDelete, "/v1/polls/"+url.PathEscape(c.account), body)
	return err
}

func (c *Client) Attachment(ctx context.Context, id string) (io.ReadCloser, http.Header, error) {
	path := "/v1/attachments/" + url.PathEscape(id)
	res, err := c.roundTrip(ctx, c.stream, http.MethodGet, path, nil)
	if err != nil {
		return nil, nil, err
	}
	if res.StatusCode >= 300 {
		defer func() { _ = res.Body.Close() }()
		out, _ := io.ReadAll(io.LimitReader(res.Body, 1<<10))
		return nil, nil, c.statusError(http.MethodGet, path, res.StatusCode, out)
	}
	return res.Body, res.Header, nil
}

func (c *Client) roundTrip(ctx context.Context, hc *http.Client, method, path string, body []byte) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, errors.New(c.redact.Replace(err.Error()))
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := hc.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, &transportError{msg: c.redact.Replace(fmt.Sprintf("%s %s: %v", method, path, err)), err: err}
	}
	return res, nil
}

type transportError struct {
	msg string
	err error
}

func (e *transportError) Error() string { return e.msg }
func (e *transportError) Unwrap() error { return e.err }

func (c *Client) statusError(method, path string, status int, body []byte) error {
	switch {
	case status == http.StatusTooManyRequests || bytes.Contains(body, []byte("RateLimit")):
		return &RateLimitError{Msg: c.redact.Replace(fmt.Sprintf("rate limited: %.200s", body))}
	case status >= 300:
		return errors.New(c.redact.Replace(fmt.Sprintf("%s %s returned HTTP %d: %.300s", method, path, status, body)))
	}
	return nil
}
