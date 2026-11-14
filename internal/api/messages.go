package api

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/lkshrk/signal-hub/internal/message"
)

const (
	channelPull  = "pull"
	defaultLimit = 100
	maxLimit     = 500
)

type pullParams struct {
	after    int64
	hasAfter bool
	wait     time.Duration
	limit    int
}

func parsePull(q url.Values, maxWait time.Duration) (pullParams, error) {
	p := pullParams{limit: defaultLimit}
	if v := q.Get("after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return p, errors.New("after: must be a non-negative integer")
		}
		p.after, p.hasAfter = n, true
	}
	if v := q.Get("wait"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return p, errors.New("wait: must be an integer number of seconds")
		}
		p.wait = min(time.Duration(min(max(n, 0), 3600))*time.Second, maxWait)
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return p, errors.New("limit: must be an integer")
		}
		p.limit = int(min(max(n, 1), maxLimit))
	}
	return p, nil
}

func (s *Server) pull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	consumer := consumerFrom(ctx)
	p, err := parsePull(r.URL.Query(), s.maxWait())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	after := p.after
	if !p.hasAfter {
		if after, err = s.Store.Head(ctx); err != nil {
			s.fail(w, r, "head", err)
			return
		}
	}
	deadline := time.NewTimer(p.wait)
	defer deadline.Stop()
	out := PullResponse{Messages: []message.Message{}}
	for {
		changed := s.Store.Changed()
		res, err := s.replayer().Read(ctx, consumer, channelPull, after, message.Deliverable, p.limit)
		if err != nil {
			s.fail(w, r, "read", err)
			return
		}
		out.Missed += res.Missed
		out.Reset = out.Reset || res.Reset
		after, out.Next = res.Next, res.Next
		for _, rec := range res.Records {
			m, err := message.Build(rec.Seq, rec.ReceivedAt, rec.Raw)
			if err != nil {
				s.Log.Error("message build failed", "consumer", consumer, "seq", rec.Seq)
				continue
			}
			out.Messages = append(out.Messages, m)
		}
		if len(out.Messages) > 0 || p.wait == 0 {
			break
		}
		if s.waiting != nil {
			s.waiting()
		}
		select {
		case <-changed:
		case <-deadline.C:
			writeJSON(w, http.StatusOK, out)
			return
		case <-ctx.Done():
			return
		}
	}
	s.Health.Delivered(consumer, channelPull, len(out.Messages))
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, op string, err error) {
	if r.Context().Err() != nil {
		return
	}
	s.Log.Error("request failed", "consumer", consumerFrom(r.Context()), "op", op, "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}
