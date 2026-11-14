package replay

import (
	"context"
	"log/slog"
	"time"

	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
)

type Replayer struct {
	Store  *store.Store
	Router *route.Router
	Health *health.State
	Log    *slog.Logger
	Now    func() time.Time
}

type Result struct {
	Records []store.Record
	From    int64
	Next    int64
	Missed  int
	Reset   bool
}

func (r *Replayer) Read(ctx context.Context, consumer, channel string, after int64, kinds []string, limit int) (Result, error) {
	var res Result
	head, err := r.Store.Head(ctx)
	if err != nil {
		return res, err
	}
	if after > head {
		oldest, err := r.Store.Oldest(ctx)
		if err != nil {
			return res, err
		}
		r.Log.Warn("cursor ahead of log", "consumer", consumer, "channel", channel, "after", after, "head", head)
		after, res.Reset = oldest, true
	}
	res.From = after
	q := store.Query{Consumer: consumer, After: after, Kinds: kinds, Limit: limit}
	var maxAge time.Duration
	if c, ok := r.Router.Consumer(consumer); ok && c.MaxReplayAge > 0 {
		maxAge = time.Duration(c.MaxReplayAge)
		q.NotBefore = r.Now().Add(-maxAge)
	}
	page, err := r.Store.Read(ctx, q)
	if err != nil {
		return res, err
	}
	res.Records, res.Next, res.Missed = page.Records, page.Next, page.Missed
	if page.Missed > 0 {
		r.Log.Warn("messages missed", "consumer", consumer, "channel", channel, "count", page.Missed, "maxReplayAge", maxAge)
		r.Health.Missed(consumer, page.Missed)
	}
	return res, nil
}
