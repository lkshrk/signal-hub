package route

import (
	"crypto/subtle"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lkshrk/signal-hub/internal/config"
	"github.com/lkshrk/signal-hub/internal/groupid"
	"github.com/lkshrk/signal-hub/internal/health"
	"github.com/lkshrk/signal-hub/internal/message"
)

const maxLoggedKeys = 10000

type Router struct {
	consumers []config.Consumer
	byName    map[string]int
	groups    map[string]string
	now       func() time.Time
	log       *slog.Logger
	health    *health.State

	mu         sync.Mutex
	day        string
	logged     map[string]struct{}
	suppressed bool
	maxKeys    int
}

func New(consumers []config.Consumer, now func() time.Time, log *slog.Logger, h *health.State) *Router {
	r := &Router{
		consumers: slices.Clone(consumers),
		byName:    make(map[string]int, len(consumers)),
		groups:    map[string]string{},
		now:       now,
		log:       log,
		health:    h,
		logged:    map[string]struct{}{},
		maxKeys:   maxLoggedKeys,
	}
	for i, c := range r.consumers {
		r.byName[c.Name] = i
		for _, g := range c.GroupIDs {
			r.groups[g] = c.Name
		}
	}
	return r
}

func (r *Router) Route(p message.Parsed) (string, bool) {
	if p.GroupInvalid {
		return "", false
	}
	if p.GroupID != "" {
		name, ok := r.groups[p.GroupID]
		return name, ok
	}
	if p.Kind != message.KindTyping && !slices.Contains(message.Deliverable, p.Kind) {
		return "", false
	}
	for _, c := range r.consumers {
		if hasSender(c.Senders, p.SourceNumber) || hasSender(c.Senders, p.SourceUUID) {
			return c.Name, true
		}
	}
	return "", false
}

func hasSender(senders []string, s string) bool {
	if s == "" {
		return false
	}
	return slices.ContainsFunc(senders, func(l string) bool { return strings.EqualFold(l, s) })
}

func (r *Router) NoteUnclaimed(p message.Parsed) {
	if !slices.Contains(message.Deliverable, p.Kind) {
		return
	}
	r.health.Unclaimed()

	attr, key := "group", p.GroupID
	switch {
	case p.GroupInvalid:
		key = "invalid"
	case key == "":
		attr, key = "sender", p.SourceNumber
		if key == "" {
			key = p.SourceUUID
		}
	}

	day := r.now().UTC().Format(time.DateOnly)
	r.mu.Lock()
	if day != r.day {
		r.day = day
		r.logged = map[string]struct{}{}
		r.suppressed = false
	}
	mapKey := attr + ":" + key
	_, seen := r.logged[mapKey]
	var logKey, logSuppressed bool
	switch {
	case seen:
	case len(r.logged) >= r.maxKeys:
		logSuppressed = !r.suppressed
		r.suppressed = true
	default:
		r.logged[mapKey] = struct{}{}
		logKey = true
	}
	r.mu.Unlock()

	if logKey {
		r.log.Info("unclaimed message", "kind", p.Kind, attr, key)
	}
	if logSuppressed {
		r.log.Info("unclaimed messages suppressed", "limit", r.maxKeys)
	}
}

func (r *Router) Allowed(consumer, recipient string) (string, bool) {
	i, ok := r.byName[consumer]
	if !ok || recipient == "" {
		return "", false
	}
	c := r.consumers[i]
	if id, err := groupid.Normalize(recipient); err == nil {
		if slices.Contains(c.GroupIDs, id) {
			return groupid.REST(id), true
		}
		return "", false
	}
	if hasSender(c.Senders, recipient) {
		return recipient, true
	}
	return "", false
}

func (r *Router) ByToken(token string) (string, bool) {
	found := ""
	for _, c := range r.consumers {
		if c.Token == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(c.Token)) == 1 && token != "" {
			found = c.Name
		}
	}
	return found, found != ""
}

func (r *Router) Consumer(name string) (config.Consumer, bool) {
	i, ok := r.byName[name]
	if !ok {
		return config.Consumer{}, false
	}
	return r.consumers[i], true
}
