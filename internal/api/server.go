package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
	"github.com/lkshrk/signal-hub/internal/replay"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

const defaultMaxWait = 60 * time.Second

type Server struct {
	Store     *store.Store
	Router    *route.Router
	Upstream  *upstream.Client
	Health    *health.State
	Log       *slog.Logger
	Now       func() time.Time
	MaxBody   int64
	MaxWait   time.Duration
	Keepalive time.Duration

	waiting func()
}

type PullResponse struct {
	Messages []message.Message `json:"messages"`
	Next     int64             `json:"next"`
	Missed   int               `json:"missed"`
	Reset    bool              `json:"reset"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/messages", s.pull)
	mux.HandleFunc("GET /v1/events", s.events)
	mux.HandleFunc("GET /v1/attachments/{id}", s.attachment)
	return s.authenticate(mux)
}

func (s *Server) replayer() *replay.Replayer {
	return &replay.Replayer{Store: s.Store, Router: s.Router, Health: s.Health, Log: s.Log, Now: s.Now}
}

func (s *Server) maxWait() time.Duration {
	if s.MaxWait > 0 {
		return s.MaxWait
	}
	return defaultMaxWait
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
