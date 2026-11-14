package outbound

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"

	"github.com/lkshrk/signal-hub/internal/styles"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

const (
	ResultOK          = "ok"
	ResultDenied      = "denied"
	ResultRateLimited = "rate_limited"
	ResultError       = "error"
)

var styleNames = map[string]bool{"BOLD": true, "ITALIC": true, "STRIKETHROUGH": true, "MONOSPACE": true, "SPOILER": true}

type Quote struct {
	Timestamp int64
	Author    string
	Text      string
}

type Mention struct {
	Start, Length int
	Author        string
}

type Style struct {
	Start, Length int
	Style         string
}

type File struct {
	ContentType string
	Filename    string
	Data        string
}

type Poll struct {
	Question      string
	Options       []string
	AllowMultiple bool
}

type Message struct {
	Text        string
	Quote       *Quote
	Mentions    []Mention
	Styles      []Style
	Attachments []File
	Poll        *Poll
}

type Result struct {
	Timestamp  int64
	Timestamps []int64
	Failures   []upstream.Failure
}

type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

func Validate(m Message) error {
	if m.Poll != nil {
		if m.Text != "" || m.Quote != nil || len(m.Mentions) > 0 || len(m.Styles) > 0 || len(m.Attachments) > 0 {
			return invalid("poll cannot be combined with text, quote, mentions, styles or attachments")
		}
		return validatePoll(m.Poll)
	}
	if m.Text == "" && len(m.Attachments) == 0 {
		return invalid("text, attachments or poll required")
	}
	if m.Quote != nil && m.Quote.Author == "" {
		return invalid("quote.author: required")
	}
	if len(m.Mentions) > 0 && len(m.Styles) > 0 {
		return invalid("mentions and styles cannot be combined")
	}
	if err := validateStyles(m.Text, m.Styles); err != nil {
		return err
	}
	for i, a := range m.Attachments {
		if _, err := base64.StdEncoding.DecodeString(a.Data); err != nil {
			return invalid("attachments[%d].data: invalid base64", i)
		}
	}
	return nil
}

func validatePoll(p *Poll) error {
	if strings.TrimSpace(p.Question) == "" || len(p.Options) < 2 {
		return invalid("poll: question and at least two options required")
	}
	for _, o := range p.Options {
		if strings.TrimSpace(o) == "" {
			return invalid("poll: question and at least two options required")
		}
	}
	return nil
}

func validateStyles(text string, list []Style) error {
	n := len(utf16.Encode([]rune(text)))
	for i, s := range list {
		if !styleNames[s.Style] {
			return invalid("styles[%d].style: unknown style %q", i, s.Style)
		}
		if s.Start < 0 || s.Length <= 0 || s.Start+s.Length > n {
			return invalid("styles[%d]: range outside text", i)
		}
	}
	return nil
}

func Request(account, recipient string, m Message) (upstream.SendRequest, error) {
	if err := Validate(m); err != nil {
		return upstream.SendRequest{}, err
	}
	req := upstream.SendRequest{Number: account, Recipients: []string{recipient}, Message: m.Text}
	if q := m.Quote; q != nil {
		req.QuoteTimestamp, req.QuoteAuthor = &q.Timestamp, &q.Author
		if q.Text != "" {
			req.QuoteMessage = &q.Text
		}
	}
	for _, mn := range m.Mentions {
		req.Mentions = append(req.Mentions, upstream.Mention{Start: int64(mn.Start), Length: int64(mn.Length), Author: mn.Author})
	}
	if len(m.Styles) > 0 {
		ranges := make([]string, len(m.Styles))
		for i, s := range m.Styles {
			ranges[i] = fmt.Sprintf("%d:%d:%s", s.Start, s.Length, s.Style)
		}
		if rendered, ok := styles.Render(m.Text, ranges); ok {
			mode := "styled"
			req.Message, req.TextMode = rendered, &mode
		}
	}
	for _, a := range m.Attachments {
		req.Base64Attachments = append(req.Base64Attachments, dataURI(a))
	}
	return req, nil
}

func dataURI(a File) string {
	ct := a.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	var b strings.Builder
	b.WriteString("data:" + ct)
	if a.Filename != "" {
		b.WriteString(";filename=" + a.Filename)
	}
	b.WriteString(";base64," + a.Data)
	return b.String()
}

func Deliver(ctx context.Context, c *upstream.Client, recipient string, m Message) (Result, error) {
	if err := Validate(m); err != nil {
		return Result{}, err
	}
	if p := m.Poll; p != nil {
		ts, err := c.CreatePoll(ctx, recipient, p.Question, p.Options, p.AllowMultiple)
		if err != nil {
			return Result{}, err
		}
		return Result{Timestamp: ts, Timestamps: []int64{ts}}, nil
	}
	req, err := Request(c.Account(), recipient, m)
	if err != nil {
		return Result{}, err
	}
	results, err := c.Send(ctx, req)
	if err != nil {
		return Result{}, err
	}
	out := Result{Timestamp: results[0].Timestamp, Timestamps: make([]int64, 0, len(results))}
	for _, r := range results {
		out.Timestamps = append(out.Timestamps, r.Timestamp)
		out.Failures = append(out.Failures, r.Failures...)
	}
	return out, nil
}

func Kind(err error) string {
	var rl *upstream.RateLimitError
	switch {
	case err == nil:
		return ResultOK
	case errors.As(err, &rl):
		return ResultRateLimited
	}
	return ResultError
}
