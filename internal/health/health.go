package health

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Snapshot struct {
	Status                string                        `json:"status"`
	Upstream              UpstreamSnapshot              `json:"upstream"`
	Store                 StoreSnapshot                 `json:"store"`
	LastMessageAt         *time.Time                    `json:"lastMessageAt"`
	LastMessageAgeSeconds *int64                        `json:"lastMessageAgeSeconds"`
	Unclaimed             int64                         `json:"unclaimed"`
	Missed                map[string]int64              `json:"missed"`
	Consumers             map[string]map[string]Channel `json:"consumers"`
}

type UpstreamSnapshot struct {
	Connected      bool       `json:"connected"`
	ConnectedSince *time.Time `json:"connectedSince"`
	Reconnects     int64      `json:"reconnects"`
}

type StoreSnapshot struct {
	Writable bool   `json:"writable"`
	Error    string `json:"error"`
}

type Channel struct {
	Subscribers int   `json:"subscribers"`
	Position    int64 `json:"position"`
}

type State struct {
	now func() time.Time
	m   *metrics

	mu             sync.Mutex
	ready          bool
	connected      bool
	connectedSince time.Time
	reconnects     int64
	storeErr       string
	storeWritable  bool
	lastMessage    time.Time
	unclaimed      int64
	missed         map[string]int64
	consumers      map[string]map[string]*Channel
}

func New(now func() time.Time) *State {
	return &State{
		now:       now,
		m:         newMetrics(),
		missed:    map[string]int64{},
		consumers: map[string]map[string]*Channel{},
	}
}

func (s *State) channel(consumer, channel string) *Channel {
	byChannel := s.consumers[consumer]
	if byChannel == nil {
		byChannel = map[string]*Channel{}
		s.consumers[consumer] = byChannel
	}
	c := byChannel[channel]
	if c == nil {
		c = &Channel{}
		byChannel[channel] = c
	}
	return c
}

func (s *State) SetReady(ready bool) {
	s.mu.Lock()
	s.ready = ready
	s.mu.Unlock()
}

func (s *State) UpstreamConnected(connected bool) {
	s.mu.Lock()
	if connected && !s.connected {
		s.connectedSince = s.now()
	}
	s.connected = connected
	s.mu.Unlock()
	s.m.connected.Set(boolFloat(connected))
}

func (s *State) Reconnected() {
	s.mu.Lock()
	s.reconnects++
	s.mu.Unlock()
	s.m.reconnects.Inc()
}

func (s *State) StoreResult(err error) {
	s.mu.Lock()
	s.storeWritable = err == nil
	s.storeErr = ""
	if err != nil {
		s.storeErr = err.Error()
	}
	s.mu.Unlock()
	s.m.storeWritable.Set(boolFloat(err == nil))
}

func (s *State) MessageStored(kind string, at time.Time) {
	s.mu.Lock()
	s.lastMessage = at
	s.mu.Unlock()
	s.m.envelopes.WithLabelValues(kind).Inc()
	s.m.lastMessage.Set(float64(at.Unix()))
}

func (s *State) Duplicate() { s.m.duplicates.Inc() }

func (s *State) Rejected() { s.m.rejected.Inc() }

func (s *State) Unclaimed() {
	s.mu.Lock()
	s.unclaimed++
	s.mu.Unlock()
	s.m.unclaimed.Inc()
}

func (s *State) Missed(consumer string, n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.missed[consumer] += int64(n)
	s.mu.Unlock()
	s.m.missed.WithLabelValues(consumer).Add(float64(n))
}

func (s *State) Delivered(consumer, channel string, n int) {
	if n <= 0 {
		return
	}
	s.m.delivered.WithLabelValues(consumer, channel).Add(float64(n))
}

func (s *State) Sent(consumer, result string) {
	s.m.sends.WithLabelValues(consumer, result).Inc()
}

func (s *State) Position(consumer, channel string, seq int64) {
	s.mu.Lock()
	s.channel(consumer, channel).Position = seq
	s.mu.Unlock()
	s.m.position.WithLabelValues(consumer, channel).Set(float64(seq))
}

func (s *State) Subscribers(consumer, channel string, delta int) {
	s.mu.Lock()
	c := s.channel(consumer, channel)
	c.Subscribers = max(0, c.Subscribers+delta)
	s.mu.Unlock()
}

func (s *State) Pruned(n int64) {
	if n <= 0 {
		return
	}
	s.m.pruned.Add(float64(n))
}

func (s *State) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := Snapshot{
		Upstream:  UpstreamSnapshot{Connected: s.connected, Reconnects: s.reconnects},
		Store:     StoreSnapshot{Writable: s.storeWritable, Error: s.storeErr},
		Unclaimed: s.unclaimed,
		Missed:    make(map[string]int64, len(s.missed)),
		Consumers: make(map[string]map[string]Channel, len(s.consumers)),
	}
	if s.connected {
		since := s.connectedSince
		snap.Upstream.ConnectedSince = &since
	}
	if !s.lastMessage.IsZero() {
		at := s.lastMessage
		age := int64(s.now().Sub(at).Seconds())
		snap.LastMessageAt = &at
		snap.LastMessageAgeSeconds = &age
	}
	for k, v := range s.missed {
		snap.Missed[k] = v
	}
	for consumer, byChannel := range s.consumers {
		out := make(map[string]Channel, len(byChannel))
		for ch, c := range byChannel {
			out[ch] = *c
		}
		snap.Consumers[consumer] = out
	}
	snap.Status = "ok"
	if !s.connected || !s.storeWritable {
		snap.Status = "error"
	}
	return snap
}

func (s *State) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.m.reg, promhttp.HandlerOpts{}))
	return mux
}

func (s *State) healthz(w http.ResponseWriter, _ *http.Request) {
	snap := s.Snapshot()
	code := http.StatusOK
	if snap.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *State) readyz(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	ready := s.ready
	s.mu.Unlock()
	if !ready {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
