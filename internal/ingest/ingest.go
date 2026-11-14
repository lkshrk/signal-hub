package ingest

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
)

const (
	readLimit     = 64 << 20
	frameBuffer   = 256
	appendInitial = 100 * time.Millisecond
	appendMax     = 5 * time.Second
)

type Appender interface {
	Append(ctx context.Context, e store.Entry) (int64, bool, error)
}

type Ingester struct {
	URL          string
	Store        Appender
	Router       *route.Router
	Health       *health.State
	Log          *slog.Logger
	Backoff      Backoff
	Now          func() time.Time
	Sleep        func(ctx context.Context, d time.Duration) error
	Rand         func() float64
	Redact       func(string) string
	PingInterval time.Duration
	PingTimeout  time.Duration
}

func (i *Ingester) applyDefaults() {
	if i.Log == nil {
		i.Log = slog.Default()
	}
	if i.Backoff == (Backoff{}) {
		i.Backoff = DefaultBackoff
	}
	if i.Now == nil {
		i.Now = time.Now
	}
	if i.Sleep == nil {
		i.Sleep = sleep
	}
	if i.Rand == nil {
		i.Rand = rand.Float64
	}
	if i.Redact == nil {
		i.Redact = func(s string) string { return s }
	}
	if i.PingInterval <= 0 {
		i.PingInterval = 30 * time.Second
	}
	if i.PingTimeout <= 0 {
		i.PingTimeout = 10 * time.Second
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (i *Ingester) Run(ctx context.Context) error {
	i.applyDefaults()
	attempt := 0
	connectedOnce := false
	for ctx.Err() == nil {
		conn, resp, err := websocket.Dial(ctx, i.URL, nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			i.Log.Warn("upstream dial failed", "error", i.Redact(err.Error()), "attempt", attempt)
		} else {
			conn.SetReadLimit(readLimit)
			started := i.Now()
			i.Log.Info("upstream connected", "url", i.Redact(i.URL), "attempt", attempt)
			if connectedOnce {
				i.Health.Reconnected()
			}
			connectedOnce = true
			i.Health.UpstreamConnected(true)

			sessionErr := i.session(ctx, conn)
			i.Health.UpstreamConnected(false)
			uptime := i.Now().Sub(started)
			if ctx.Err() == nil {
				i.Log.Warn("upstream disconnected", "error", i.Redact(errString(sessionErr)), "uptime", uptime)
			}
			if uptime >= i.Backoff.ResetAfter {
				attempt = 0
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := i.Sleep(ctx, i.Backoff.Next(attempt, i.Rand)); err != nil {
			return nil
		}
		attempt++
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (i *Ingester) session(ctx context.Context, conn *websocket.Conn) error {
	sessCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	frames := make(chan []byte, frameBuffer)
	var (
		mu      sync.Mutex
		endErr  error
		wg      sync.WaitGroup
		pingEnd = make(chan struct{})
	)
	fail := func(err error) {
		mu.Lock()
		if endErr == nil {
			endErr = err
		}
		mu.Unlock()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(frames)
		for {
			_, data, err := conn.Read(sessCtx)
			if err != nil {
				fail(err)
				return
			}
			select {
			case frames <- data:
			case <-sessCtx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(i.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-sessCtx.Done():
				return
			case <-pingEnd:
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(sessCtx, i.PingTimeout)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					fail(err)
					_ = conn.CloseNow()
					return
				}
			}
		}
	}()

	stopWatch := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			_ = conn.Close(websocket.StatusGoingAway, "")
		case <-stopWatch:
		}
	}()

	for data := range frames {
		i.handle(ctx, data)
	}
	close(pingEnd)
	close(stopWatch)
	cancel()
	_ = conn.CloseNow()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return endErr
}

func (i *Ingester) handle(ctx context.Context, data []byte) {
	p, err := message.Parse(data)
	if err != nil {
		i.Log.Warn("frame rejected", "error", err, "bytes", len(data))
		i.Health.Rejected()
		return
	}
	consumer, claimed := i.Router.Route(p)
	if !claimed {
		consumer = ""
	}
	entry := store.Entry{Parsed: p, Consumer: consumer, ReceivedAt: i.Now(), Raw: data}

	_, dup, ok := i.appendDurably(ctx, entry)
	if !ok {
		return
	}
	if dup {
		i.Health.Duplicate()
		return
	}
	i.Health.MessageStored(p.Kind, entry.ReceivedAt)
	if !claimed {
		i.Router.NoteUnclaimed(p)
	}
}

func (i *Ingester) appendDurably(ctx context.Context, e store.Entry) (seq int64, dup, ok bool) {
	delay := appendInitial
	for n := 1; ; n++ {
		seq, dup, err := i.Store.Append(ctx, e)
		i.Health.StoreResult(err)
		if err == nil {
			return seq, dup, true
		}
		if ctx.Err() != nil {
			return 0, false, false
		}
		i.Log.Error("store append failed", "attempt", n, "error", err)
		if i.Sleep(ctx, delay) != nil {
			return 0, false, false
		}
		delay = min(delay*2, appendMax)
	}
}
