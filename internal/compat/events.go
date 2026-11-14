package compat

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/lkshrk/signal-hub/internal/replay"
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

func (s *Server) eventsStart(ctx context.Context) (int64, error) {
	pos, ok, err := s.Store.Position(ctx, s.Consumer, store.ChannelCompat)
	if err != nil {
		return 0, err
	}
	if ok {
		return pos, nil
	}
	return s.Store.Head(ctx)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.exited != nil {
		defer s.exited()
	}
	ctx := r.Context()
	account := r.URL.Query().Get("account")
	if account == "" {
		http.Error(w, "account query parameter required", http.StatusBadRequest)
		return
	}
	if account != s.Upstream.Account() {
		http.Error(w, "unknown account", http.StatusNotFound)
		return
	}
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.Log.Warn("events write deadline", "consumer", s.Consumer, "channel", store.ChannelCompat, "error", err)
	}
	after, err := s.eventsStart(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.Log.Error("events position failed", "consumer", s.Consumer, "channel", store.ChannelCompat, "error", err)
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	s.Health.Subscribers(s.Consumer, store.ChannelCompat, 1)
	defer s.Health.Subscribers(s.Consumer, store.ChannelCompat, -1)
	if !emit(w, rc, []byte(": connected\n\n")) {
		return
	}

	rp := &replay.Replayer{Store: s.Store, Router: s.Router, Health: s.Health, Log: s.Log, Now: s.Now}
	tick := time.NewTicker(s.keepalive())
	defer tick.Stop()
	for {
		changed := s.Store.Changed()
		res, err := rp.Read(ctx, s.Consumer, store.ChannelCompat, after, nil, eventsBatch)
		if err != nil {
			if ctx.Err() == nil {
				s.Log.Error("events read failed", "consumer", s.Consumer, "channel", store.ChannelCompat, "error", err)
			}
			return
		}
		after = res.Next
		if res.Reset {
			if err := s.Store.ResetPosition(context.WithoutCancel(ctx), s.Consumer, store.ChannelCompat, res.From); err != nil {
				s.Log.Error("events position failed", "consumer", s.Consumer, "channel", store.ChannelCompat, "error", err)
				return
			}
			s.Health.Position(s.Consumer, store.ChannelCompat, res.From)
		}
		if res.Missed > 0 && len(res.Records) == 0 && !s.advance(ctx, res.Next) {
			return
		}
		for _, rec := range res.Records {
			frame := append([]byte("data: "), compact(s.withPollVoteText(rec.Raw))...)
			if !emit(w, rc, append(frame, "\n\n"...)) {
				return
			}
			if !s.advance(ctx, rec.Seq) {
				return
			}
			s.Health.Delivered(s.Consumer, store.ChannelCompat, 1)
		}
		if len(res.Records) >= eventsBatch {
			continue
		}
		if s.waiting != nil {
			s.waiting()
		}
		select {
		case <-changed:
		case <-tick.C:
			if !emit(w, rc, []byte(": keepalive\n\n")) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) advance(ctx context.Context, seq int64) bool {
	if err := s.Store.SetPosition(context.WithoutCancel(ctx), s.Consumer, store.ChannelCompat, seq); err != nil {
		s.Log.Error("events position failed", "consumer", s.Consumer, "channel", store.ChannelCompat, "error", err)
		return false
	}
	s.Health.Position(s.Consumer, store.ChannelCompat, seq)
	return true
}

func emit(w http.ResponseWriter, rc *http.ResponseController, frame []byte) bool {
	if _, err := w.Write(frame); err != nil {
		return false
	}
	return rc.Flush() == nil
}
