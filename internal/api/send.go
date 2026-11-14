package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/lkshrk/signal-hub/internal/outbound"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

type QuoteRef struct {
	Timestamp int64  `json:"timestamp"`
	Author    string `json:"author"`
	Text      string `json:"text"`
}

type MentionRef struct {
	Start  int    `json:"start"`
	Length int    `json:"length"`
	Author string `json:"author"`
}

type StyleRange struct {
	Start  int    `json:"start"`
	Length int    `json:"length"`
	Style  string `json:"style"`
}

type OutFile struct {
	ContentType string `json:"contentType"`
	Filename    string `json:"filename"`
	Data        string `json:"data"`
}

type PollRef struct {
	Question      string   `json:"question"`
	Options       []string `json:"options"`
	AllowMultiple bool     `json:"allowMultiple"`
}

type SendRequest struct {
	Recipient   string       `json:"recipient"`
	Text        string       `json:"text"`
	Quote       *QuoteRef    `json:"quote,omitempty"`
	Mentions    []MentionRef `json:"mentions,omitempty"`
	Styles      []StyleRange `json:"styles,omitempty"`
	Attachments []OutFile    `json:"attachments,omitempty"`
	Poll        *PollRef     `json:"poll,omitempty"`
}

type SendResponse struct {
	Timestamp  int64              `json:"timestamp"`
	Timestamps []int64            `json:"timestamps"`
	Failures   []upstream.Failure `json:"failures,omitempty"`
}

func (r SendRequest) message() outbound.Message {
	m := outbound.Message{Text: r.Text}
	if r.Quote != nil {
		m.Quote = &outbound.Quote{Timestamp: r.Quote.Timestamp, Author: r.Quote.Author, Text: r.Quote.Text}
	}
	for _, x := range r.Mentions {
		m.Mentions = append(m.Mentions, outbound.Mention(x))
	}
	for _, x := range r.Styles {
		m.Styles = append(m.Styles, outbound.Style(x))
	}
	for _, x := range r.Attachments {
		m.Attachments = append(m.Attachments, outbound.File(x))
	}
	if r.Poll != nil {
		m.Poll = &outbound.Poll{Question: r.Poll.Question, Options: r.Poll.Options, AllowMultiple: r.Poll.AllowMultiple}
	}
	return m
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	consumer := consumerFrom(r.Context())
	var req SendRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.MaxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", tooBig.Limit))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Recipient == "" {
		writeError(w, http.StatusBadRequest, "recipient required")
		return
	}
	msg := req.message()
	if err := outbound.Validate(msg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	recipient, ok := s.Router.Allowed(consumer, req.Recipient)
	if !ok {
		s.Health.Sent(consumer, outbound.ResultDenied)
		s.Log.Warn("send denied", "consumer", consumer, "recipient", req.Recipient)
		writeError(w, http.StatusForbidden, "recipient not allowed")
		return
	}
	res, err := outbound.Deliver(r.Context(), s.Upstream, recipient, msg)
	kind := outbound.Kind(err)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		s.Health.Sent(consumer, kind)
		s.Log.Error("send failed", "consumer", consumer, "error", s.Upstream.Redact(err.Error()))
		if kind == outbound.ResultRateLimited {
			writeError(w, http.StatusTooManyRequests, "rate limited")
			return
		}
		writeError(w, http.StatusBadGateway, "upstream error")
		return
	}
	s.Health.Sent(consumer, kind)
	writeJSON(w, http.StatusOK, SendResponse{Timestamp: res.Timestamp, Timestamps: res.Timestamps, Failures: res.Failures})
}
