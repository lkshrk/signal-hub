package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/lkshrk/signal-hub/internal/api"
	"github.com/lkshrk/signal-hub/internal/compat"
	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/ingest"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

const (
	pruneInterval     = time.Hour
	probeInterval     = 30 * time.Second
	shutdownTimeout   = 10 * time.Second
	exitBudget        = 15 * time.Second
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 120 * time.Second
	maxWait           = 60 * time.Second
	keepalive         = 25 * time.Second
)

type App struct {
	cfg      *config.Config
	log      *slog.Logger
	now      func() time.Time
	health   *health.State
	router   *route.Router
	upstream *upstream.Client

	pruneInterval   time.Duration
	probeInterval   time.Duration
	shutdownTimeout time.Duration
	exitBudget      time.Duration

	ready chan struct{}
	mu    sync.Mutex
	addrs map[string]string
}

type listener struct {
	name    string
	addr    string
	handler http.Handler
}

type server struct {
	name string
	ln   net.Listener
	http *http.Server
}

func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	if cfg == nil {
		return nil, errors.New("app: config is nil")
	}
	if cfg.Database.Path == "" {
		return nil, errors.New("app: database path is empty")
	}
	if log == nil {
		log = slog.Default()
	}
	now := time.Now
	h := health.New(now)
	return &App{
		cfg:             cfg,
		log:             log,
		now:             now,
		health:          h,
		router:          route.New(cfg.Consumers, now, log, h),
		upstream:        upstream.New(cfg.Upstream.URL, cfg.Upstream.ResolvedAccount),
		pruneInterval:   pruneInterval,
		probeInterval:   probeInterval,
		shutdownTimeout: shutdownTimeout,
		exitBudget:      exitBudget,
		ready:           make(chan struct{}),
		addrs:           map[string]string{},
	}, nil
}

func (a *App) Ready() <-chan struct{} { return a.ready }

func (a *App) Addr(name string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.addrs[name]
}

func (a *App) Run(ctx context.Context) error {
	st, err := store.Open(ctx, a.cfg.Database.Path)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	a.health.StoreResult(nil)
	a.health.SetReady(true)

	servers, err := a.bind(a.listeners(st))
	if err != nil {
		_ = st.Close()
		return err
	}

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	serveErr := make(chan error, len(servers))
	var serving sync.WaitGroup
	for _, s := range servers {
		s.http.BaseContext = func(net.Listener) context.Context { return baseCtx }
		a.log.Info("listening", "name", s.name, "addr", s.ln.Addr().String())
		serving.Go(func() {
			if err := s.http.Serve(s.ln); !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("serve %s on %s: %w", s.name, s.ln.Addr(), err)
			}
		})
	}

	// Outlives the servers so in-flight appends can finish.
	ingestCtx, cancelIngest := context.WithCancel(context.Background())
	defer cancelIngest()
	ing := &ingest.Ingester{
		URL:    a.upstream.ReceiveURL(),
		Store:  st,
		Router: a.router,
		Health: a.health,
		Log:    a.log,
		Now:    a.now,
		Redact: a.upstream.Redact,
	}
	ingestDone := make(chan struct{})
	go func() {
		defer close(ingestDone)
		_ = ing.Run(ingestCtx)
	}()

	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	a.prune(loopCtx, st)
	close(a.ready)

	var loops sync.WaitGroup
	loops.Go(func() { every(loopCtx, a.pruneInterval, func() { a.prune(loopCtx, st) }) })
	loops.Go(func() { every(loopCtx, a.probeInterval, func() { a.probe(loopCtx, st) }) })

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-serveErr:
		a.log.Error("listener failed", "error", runErr)
	}

	deadline := time.Now().Add(a.exitBudget)
	a.log.Info("shutting down")
	a.health.SetReady(false)
	cancelBase()
	cancelLoops()
	a.shutdown(servers)
	serving.Wait()

	cancelIngest()
	select {
	case <-ingestDone:
	case <-time.After(time.Until(deadline)):
		a.log.Warn("ingester did not stop within the exit budget")
	}
	loops.Wait()
	if err := st.Close(); err != nil {
		a.log.Error("close store", "error", err)
	}
	a.log.Info("stopped")
	return runErr
}

func (a *App) listeners(st *store.Store) []listener {
	apiSrv := &api.Server{
		Store:     st,
		Router:    a.router,
		Upstream:  a.upstream,
		Health:    a.health,
		Log:       a.log,
		Now:       a.now,
		MaxBody:   a.cfg.Limits.MaxRequestBytes,
		MaxWait:   maxWait,
		Keepalive: keepalive,
	}
	out := []listener{
		{name: "api", addr: a.cfg.Listen, handler: apiSrv.Handler()},
		{name: "admin", addr: a.cfg.AdminListen, handler: a.health.Handler()},
	}
	for _, c := range a.cfg.Consumers {
		if c.Compat == nil {
			continue
		}
		srv := &compat.Server{
			Consumer:      c.Name,
			Store:         st,
			Router:        a.router,
			Upstream:      a.upstream,
			Health:        a.health,
			Log:           a.log,
			Now:           a.now,
			Keepalive:     keepalive,
			AttachmentDir: c.Compat.AttachmentDir,
		}
		out = append(out, listener{name: "compat:" + c.Name, addr: c.Compat.Listen, handler: srv.Handler()})
	}
	return out
}

func (a *App) bind(ls []listener) ([]server, error) {
	var lc net.ListenConfig
	servers := make([]server, 0, len(ls))
	for _, l := range ls {
		ln, err := lc.Listen(context.Background(), "tcp", l.addr)
		if err != nil {
			for _, s := range servers {
				_ = s.ln.Close()
			}
			return nil, fmt.Errorf("listen %s on %s: %w", l.name, l.addr, err)
		}
		servers = append(servers, server{name: l.name, ln: ln, http: &http.Server{
			Handler:           l.handler,
			ReadHeaderTimeout: readHeaderTimeout,
			IdleTimeout:       idleTimeout,
			ErrorLog:          slog.NewLogLogger(a.log.Handler(), slog.LevelWarn),
		}})
	}
	a.mu.Lock()
	for _, s := range servers {
		a.addrs[s.name] = s.ln.Addr().String()
	}
	a.mu.Unlock()
	return servers, nil
}

func (a *App) shutdown(servers []server) {
	ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Go(func() {
			if err := s.http.Shutdown(ctx); err != nil {
				a.log.Warn("server shutdown", "name", s.name, "error", err)
				_ = s.http.Close()
			}
		})
	}
	wg.Wait()
}

func (a *App) prune(ctx context.Context, st *store.Store) {
	before := a.now().Add(-time.Duration(a.cfg.Retention)).UTC()
	n, err := st.Prune(ctx, before)
	if err != nil {
		if ctx.Err() == nil {
			a.log.Error("prune failed", "error", err)
		}
		return
	}
	a.health.Pruned(n)
	a.log.Info("pruned", "count", n, "before", before)
}

func (a *App) probe(ctx context.Context, st *store.Store) {
	err := st.Writable(ctx)
	if ctx.Err() != nil {
		return
	}
	a.health.StoreResult(err)
	if err != nil {
		a.log.Error("store not writable", "error", err)
	}
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}
