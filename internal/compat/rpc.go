package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lkshrk/signal-hub/internal/styles"
)

const unknownAttachment = `{"error":"No attachment with that name found"}`

func (s *Server) account(p map[string]any) (string, error) {
	hub := s.Upstream.Account()
	if a := str(p["account"]); a != "" && a != hub {
		return "", fmt.Errorf("account %s is not served by this hub", a)
	}
	return hub, nil
}

func (s *Server) constant(p map[string]any, result any) (any, error) {
	if _, err := s.account(p); err != nil {
		return nil, err
	}
	return result, nil
}

// targets maps signal-cli's groupId (internal id) or recipient list to REST recipients the consumer may reach.
func (s *Server) targets(p map[string]any) ([]string, error) {
	var in []string
	if g := str(p["groupId"]); g != "" {
		in = []string{g}
	} else {
		switch v := p["recipient"].(type) {
		case []any:
			for _, r := range v {
				if s := str(r); s != "" {
					in = append(in, s)
				}
			}
		case string:
			if v != "" {
				in = append(in, v)
			}
		}
	}
	var out []string
	for _, t := range in {
		rest, ok := s.Router.Allowed(s.Consumer, t)
		if !ok {
			return nil, fmt.Errorf("recipient not allowed for consumer %s", s.Consumer)
		}
		out = append(out, rest)
	}
	return out, nil
}

func (s *Server) send(ctx context.Context, p map[string]any) (any, error) {
	account, err := s.account(p)
	if err != nil {
		return nil, err
	}
	recipients, err := s.targets(p)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"number":     account,
		"message":    str(p["message"]),
		"recipients": recipients,
	}
	if paths, ok := p["attachments"].([]any); ok && len(paths) > 0 {
		encoded := make([]string, 0, len(paths))
		for _, raw := range paths {
			a, err := encodeAttachment(str(raw))
			if err != nil {
				return nil, err
			}
			encoded = append(encoded, a)
		}
		body["base64_attachments"] = encoded
	}
	if msg, ok := styles.Render(str(p["message"]), textStyles(p)); ok {
		body["message"] = msg
		body["text_mode"] = "styled"
	}
	if ts, ok := num(p["quoteTimestamp"]); ok {
		body["quote_timestamp"] = ts
		if a := str(p["quoteAuthor"]); a != "" {
			body["quote_author"] = a
		}
	}

	raw, err := s.Upstream.DoJSON(ctx, http.MethodPost, "/v2/send", body)
	if err != nil {
		return nil, err
	}
	var resp []struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode send response: %w", err)
	}
	if len(resp) == 0 {
		return nil, errors.New("decode send response: empty")
	}
	ts, _ := strconv.ParseInt(resp[0].Timestamp, 10, 64)
	return map[string]any{"timestamp": ts, "results": []any{map[string]any{"type": "SUCCESS"}}}, nil
}

func (s *Server) typing(ctx context.Context, p map[string]any) (any, error) {
	account, err := s.account(p)
	if err != nil {
		return nil, err
	}
	recipients, err := s.targets(p)
	if err != nil {
		return nil, err
	}
	method := http.MethodPut
	if b, _ := p["stop"].(bool); b {
		method = http.MethodDelete
	}
	for _, t := range recipients {
		if _, err := s.Upstream.DoJSON(ctx, method, "/v1/typing-indicator/"+url.PathEscape(account), map[string]any{"recipient": t}); err != nil {
			return nil, err
		}
	}
	return map[string]any{}, nil
}

func (s *Server) reaction(ctx context.Context, p map[string]any) (any, error) {
	account, err := s.account(p)
	if err != nil {
		return nil, err
	}
	recipients, err := s.targets(p)
	if err != nil {
		return nil, err
	}
	ts, _ := num(p["targetTimestamp"])
	method := http.MethodPost
	if b, _ := p["remove"].(bool); b {
		method = http.MethodDelete
	}
	for _, t := range recipients {
		body := map[string]any{"recipient": t, "reaction": str(p["emoji"]), "target_author": str(p["targetAuthor"]), "timestamp": ts}
		if _, err := s.Upstream.DoJSON(ctx, method, "/v1/reactions/"+url.PathEscape(account), body); err != nil {
			return nil, err
		}
	}
	return map[string]any{}, nil
}

func (s *Server) attachment(ctx context.Context, p map[string]any) (any, error) {
	if _, err := s.account(p); err != nil {
		return nil, err
	}
	id := str(p["id"])
	if id == "" {
		return nil, errors.New("attachment id missing")
	}
	path := "/v1/attachments/" + url.PathEscape(id)
	_, owned, err := s.Store.Attachment(ctx, s.Consumer, id)
	if err != nil {
		s.Log.Error("attachment lookup failed", "consumer", s.Consumer, "error", err)
		return nil, errors.New("attachment lookup failed")
	}
	// Another consumer's attachment answers exactly like an id upstream does not know.
	if !owned {
		return nil, errors.New(s.Upstream.Redact(fmt.Sprintf("%s %s returned HTTP %d: %s", http.MethodGet, path, http.StatusNotFound, unknownAttachment)))
	}
	data, err := s.Upstream.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": base64.StdEncoding.EncodeToString(data)}, nil
}

func textStyles(p map[string]any) []string {
	var out []string
	for _, key := range []string{"textStyle", "textStyles"} {
		switch v := p[key].(type) {
		case string:
			if v != "" {
				out = append(out, v)
			}
		case []any:
			for _, s := range v {
				if s := str(s); s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func encodeAttachment(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read attachment %s: %w", filepath.Base(path), err)
	}
	ct := mime.TypeByExtension(filepath.Ext(path))
	if ct == "" {
		ct = http.DetectContentType(data)
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return fmt.Sprintf("data:%s;filename=%s;base64,%s", ct, filepath.Base(path), base64.StdEncoding.EncodeToString(data)), nil
}

func compact(data []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return data
	}
	return buf.Bytes()
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func num(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	}
	return 0, false
}
