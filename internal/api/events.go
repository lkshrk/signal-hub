package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/store"
)

const (
	defaultKeepalive = 25 * time.Second
	eventsBatch      = 100
)

func (s *Server) keepalive() time.Duration {
	if s.Keepalive > 0 {
		return s.Keepalive
	}
	return defaultKeepalive
}

func parseSeq(v, name string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: must be a non-negative integer", name)
	}
	return n, nil
}

type badStartError struct{ error }

func (s *Server) eventsStart(r *http.Request, consumer string) (int64, error) {
	v, name := r.Header.Get("Last-Event-ID"), "Last-Event-ID"
	if v == "" {
		v, name = r.URL.Query().Get("after"), "after"
	}
	if v != "" {
		n, err := parseSeq(v, name)
		if err != nil {
			return 0, &badStartError{err}
		}
		return n, nil
	}
	pos, ok, err := s.Store.Position(r.Context(), consumer, store.ChannelEvents)
	if err != nil {
		return 0, err
	}
	if ok {
		return pos, nil
	}
	return s.Store.Head(r.Context())
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	consumer := consumerFrom(ctx)
	if s.exited != nil {
		defer s.exited()
	}
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.Log.Warn("events write deadline", "consumer", consumer, "error", err)
	}
	after, err := s.eventsStart(r, consumer)
	if err != nil {
		var bad *badStartError
		if errors.As(err, &bad) {
			writeError(w, http.StatusBadRequest, bad.Error())
			return
		}
		s.fail(w, r, "position", err)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	s.Health.Subscribers(consumer, store.ChannelEvents, 1)
	defer s.Health.Subscribers(consumer, store.ChannelEvents, -1)
	if !emit(w, rc, ": connected\n\n") {
		return
	}

	tick := time.NewTicker(s.keepalive())
	defer tick.Stop()
	for {
		changed := s.Store.Changed()
		res, err := s.replayer().Read(ctx, consumer, store.ChannelEvents, after, message.Deliverable, eventsBatch)
		if err != nil {
			if ctx.Err() == nil {
				s.Log.Error("events read failed", "consumer", consumer, "error", err)
			}
			return
		}
		after = res.Next
		if res.Reset {
			if err := s.Store.ResetPosition(context.WithoutCancel(ctx), consumer, store.ChannelEvents, res.From); err != nil {
				s.Log.Error("events position failed", "consumer", consumer, "error", err)
				return
			}
			s.Health.Position(consumer, store.ChannelEvents, res.From)
		}
		if res.Missed > 0 {
			if !emit(w, rc, fmt.Sprintf("event: missed\ndata: {\"count\":%d}\n\n", res.Missed)) {
				return
			}
			if len(res.Records) == 0 && !s.advance(ctx, consumer, res.Next) {
				return
			}
		}
		for _, rec := range res.Records {
			m, err := message.Build(rec.Seq, rec.ReceivedAt, rec.Raw)
			if err != nil {
				s.Log.Error("message build failed", "consumer", consumer, "seq", rec.Seq)
				continue
			}
			data, err := json.Marshal(m)
			if err != nil {
				s.Log.Error("message encode failed", "consumer", consumer, "seq", rec.Seq)
				continue
			}
			if !emit(w, rc, fmt.Sprintf("id: %d\nevent: message\ndata: %s\n\n", rec.Seq, data)) {
				return
			}
			if !s.advance(ctx, consumer, rec.Seq) {
				return
			}
			s.Health.Delivered(consumer, store.ChannelEvents, 1)
		}
		if res.More {
			continue
		}
		if s.waiting != nil {
			s.waiting()
		}
		select {
		case <-changed:
		case <-tick.C:
			if !emit(w, rc, ": keepalive\n\n") {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) advance(ctx context.Context, consumer string, seq int64) bool {
	if err := s.Store.SetPosition(context.WithoutCancel(ctx), consumer, store.ChannelEvents, seq); err != nil {
		s.Log.Error("events position failed", "consumer", consumer, "error", err)
		return false
	}
	s.Health.Position(consumer, store.ChannelEvents, seq)
	return true
}

func emit(w http.ResponseWriter, rc *http.ResponseController, frame string) bool {
	if _, err := w.Write([]byte(frame)); err != nil {
		return false
	}
	return rc.Flush() == nil
}
