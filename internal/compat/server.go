// Package compat serves the signal-cli daemon HTTP contract (check, JSON-RPC, SSE events) from the log.
package compat

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/route"
	"github.com/lkshrk/signal-hub/internal/store"
	"github.com/lkshrk/signal-hub/internal/upstream"
)

type Server struct {
	Consumer  string
	Store     *store.Store
	Router    *route.Router
	Upstream  *upstream.Client
	Health    *health.State
	Log       *slog.Logger
	Now       func() time.Time
	Keepalive time.Duration

	AttachmentDir string
	MaxBody       int64

	polls pollStore

	scanLimit int
	waiting   func()
	exited    func()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/check", s.check)
	mux.HandleFunc("GET /api/v1/events", s.events)
	mux.HandleFunc("POST /api/v1/rpc", s.rpc)
	return mux
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	res, err := s.Upstream.About(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"versions":{"signal-cli":"via signal-cli-rest-api"},"upstream":`))
	_, _ = w.Write(res)
	_, _ = w.Write([]byte(`}`))
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Server) rpc(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.MaxBody)).Decode(&req); err != nil {
		writeRPC(w, nil, nil, &rpcError{Code: -32700, Message: "parse error"})
		return
	}
	var params map[string]any
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			writeRPC(w, req.ID, nil, &rpcError{Code: -32602, Message: "invalid params"})
			return
		}
	}

	var (
		result any
		err    error
	)
	ctx := r.Context()
	switch req.Method {
	case "send":
		result, err = s.send(ctx, params)
	case "sendTyping":
		result, err = s.typing(ctx, params)
	case "sendReaction":
		result, err = s.reaction(ctx, params)
	case "sendPollCreate":
		result, err = s.pollCreate(ctx, params)
	case "sendPollTerminate":
		result, err = s.pollTerminate(ctx, params)
	case "getAttachment":
		result, err = s.attachment(ctx, params)
	case "getContact":
		result, err = s.constant(params, map[string]any{})
	case "listContacts":
		result, err = s.constant(params, []any{})
	default:
		writeRPC(w, req.ID, nil, &rpcError{Code: -32601, Message: "method not supported by signal-hub: " + req.Method})
		return
	}
	if err != nil {
		code := -32603
		var rl *upstream.RateLimitError
		if errors.As(err, &rl) {
			code = 429
		}
		writeRPC(w, req.ID, nil, &rpcError{Code: code, Message: err.Error()})
		return
	}
	writeRPC(w, req.ID, result, nil)
}

func writeRPC(w http.ResponseWriter, id, result any, rerr *rpcError) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"jsonrpc": "2.0", "id": id}
	if rerr != nil {
		resp["error"] = rerr
	} else {
		resp["result"] = result
	}
	_ = json.NewEncoder(w).Encode(resp)
}
