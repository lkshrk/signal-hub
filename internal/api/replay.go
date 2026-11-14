package api

import (
	"context"
	"time"

	"github.com/lkshrk/signal-hub/internal/store"
)

type replayResult struct {
	records []store.Record
	next    int64
	missed  int
	reset   bool
}

func (s *Server) replay(ctx context.Context, consumer, channel string, after int64, kinds []string, limit int) (replayResult, error) {
	var res replayResult
	head, err := s.Store.Head(ctx)
	if err != nil {
		return res, err
	}
	if after > head {
		oldest, err := s.Store.Oldest(ctx)
		if err != nil {
			return res, err
		}
		s.Log.Warn("cursor ahead of log", "consumer", consumer, "channel", channel, "after", after, "head", head)
		after, res.reset = oldest, true
	}
	q := store.Query{Consumer: consumer, After: after, Kinds: kinds, Limit: limit}
	var maxAge time.Duration
	if c, ok := s.Router.Consumer(consumer); ok && c.MaxReplayAge > 0 {
		maxAge = time.Duration(c.MaxReplayAge)
		q.NotBefore = s.Now().Add(-maxAge)
	}
	page, err := s.Store.Read(ctx, q)
	if err != nil {
		return res, err
	}
	res.records, res.next, res.missed = page.Records, page.Next, page.Missed
	if page.Missed > 0 {
		s.Log.Warn("messages missed", "consumer", consumer, "channel", channel, "count", page.Missed, "maxReplayAge", maxAge)
		s.Health.Missed(consumer, page.Missed)
	}
	return res, nil
}
