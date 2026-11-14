package testupstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

type Session func(ctx context.Context, c *websocket.Conn, s *Server)

type Server struct {
	srv      *httptest.Server
	cancel   context.CancelFunc
	sessions []Session
	dials    atomic.Int64
	stall    atomic.Int64
	closed   chan websocket.StatusCode
	wg       sync.WaitGroup
	rest     atomic.Pointer[http.Handler]
}

func New(t testing.TB, sessions ...Session) *Server {
	t.Helper()
	if len(sessions) == 0 {
		sessions = []Session{Hold}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{cancel: cancel, sessions: sessions, closed: make(chan websocket.StatusCode, 64)}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.wg.Add(1)
		defer s.wg.Done()
		if h := s.rest.Load(); h != nil && !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			(*h).ServeHTTP(w, r)
			return
		}
		n := int(s.dials.Add(1)) - 1
		if s.stall.Add(-1) >= 0 {
			<-ctx.Done()
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		sess := s.sessions[min(n, len(s.sessions)-1)]
		sess(ctx, c, s)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *Server) URL() string { return "ws" + strings.TrimPrefix(s.srv.URL, "http") }

func (s *Server) HTTPURL() string { return s.srv.URL }

// HandleHTTP serves plain (non-upgrade) requests with h, so one server can fake both the WebSocket and REST sides.
func (s *Server) HandleHTTP(h http.Handler) { s.rest.Store(&h) }

func (s *Server) StallUpgrades(n int) { s.stall.Store(int64(n)) }

func (s *Server) Dials() int { return int(s.dials.Load()) }

func (s *Server) Closed() <-chan websocket.StatusCode { return s.closed }

func (s *Server) Close() {
	s.cancel()
	s.srv.CloseClientConnections()
	s.srv.Close()
	s.wg.Wait()
}

func Write(frames ...[]byte) Session {
	return func(ctx context.Context, c *websocket.Conn, _ *Server) {
		for _, f := range frames {
			if err := c.Write(ctx, websocket.MessageText, f); err != nil {
				return
			}
		}
	}
}

func Seq(sessions ...Session) Session {
	return func(ctx context.Context, c *websocket.Conn, s *Server) {
		for _, f := range sessions {
			f(ctx, c, s)
		}
	}
}

func Drop(_ context.Context, c *websocket.Conn, _ *Server) { _ = c.CloseNow() }

func Stall(ctx context.Context, _ *websocket.Conn, _ *Server) { <-ctx.Done() }

func Hold(ctx context.Context, c *websocket.Conn, s *Server) {
	for {
		if _, _, err := c.Read(ctx); err != nil {
			select {
			case s.closed <- websocket.CloseStatus(err):
			default:
			}
			return
		}
	}
}

func Wait(ch <-chan struct{}) Session {
	return func(ctx context.Context, _ *websocket.Conn, _ *Server) {
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}
}
