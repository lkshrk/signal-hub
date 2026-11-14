package ingest

import (
	"math"
	"time"
)

type Backoff struct {
	Initial, Max time.Duration
	Factor       float64
	Jitter       float64
	ResetAfter   time.Duration
}

var DefaultBackoff = Backoff{Initial: time.Second, Max: 30 * time.Second, Factor: 2, Jitter: 0.2, ResetAfter: time.Minute}

func (b Backoff) Next(attempt int, rnd func() float64) time.Duration {
	attempt = max(attempt, 0)
	d := float64(b.Initial) * math.Pow(b.Factor, float64(attempt))
	d *= 1 + b.Jitter*(2*rnd()-1)
	d = math.Min(d, float64(b.Max))
	return time.Duration(math.Round(d))
}
