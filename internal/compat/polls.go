package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const pollMemory = 256

type poll struct {
	question string
	options  []string
}

// pollStore remembers created polls, because a vote carries only option indexes.
type pollStore struct {
	mu    sync.Mutex
	byTS  map[int64]poll
	order []int64
}

func (ps *pollStore) put(ts int64, p poll) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.byTS == nil {
		ps.byTS = map[int64]poll{}
	}
	if _, ok := ps.byTS[ts]; !ok {
		ps.order = append(ps.order, ts)
	}
	ps.byTS[ts] = p
	for len(ps.order) > pollMemory {
		delete(ps.byTS, ps.order[0])
		ps.order = ps.order[1:]
	}
}

func (ps *pollStore) get(ts int64) (poll, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p, ok := ps.byTS[ts]
	return p, ok
}

func (s *Server) pollCreate(ctx context.Context, p map[string]any) (any, error) {
	if _, err := s.account(p); err != nil {
		return nil, err
	}
	recipient, err := s.singleTarget(p)
	if err != nil {
		return nil, err
	}
	question := strings.TrimSpace(str(p["question"]))
	var options []string
	for _, key := range []string{"options", "option"} {
		if list, ok := p[key].([]any); ok {
			for _, o := range list {
				if o := strings.TrimSpace(str(o)); o != "" {
					options = append(options, o)
				}
			}
		}
	}
	if question == "" || len(options) < 2 {
		return nil, errors.New("a poll needs a question and at least two options")
	}
	multi, _ := p["allowMultiple"].(bool)
	ts, err := s.Upstream.CreatePoll(ctx, recipient, question, options, multi)
	if err != nil {
		return nil, err
	}
	s.polls.put(ts, poll{question: question, options: options})
	return map[string]any{"timestamp": ts}, nil
}

func (s *Server) pollTerminate(ctx context.Context, p map[string]any) (any, error) {
	if _, err := s.account(p); err != nil {
		return nil, err
	}
	recipient, err := s.singleTarget(p)
	if err != nil {
		return nil, err
	}
	ts, ok := num(p["pollTimestamp"])
	if !ok {
		if v, err := strconv.ParseInt(str(p["pollTimestamp"]), 10, 64); err == nil {
			ts, ok = v, true
		}
	}
	if !ok {
		return nil, errors.New("pollTimestamp missing")
	}
	if err := s.Upstream.ClosePoll(ctx, recipient, ts); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func (s *Server) singleTarget(p map[string]any) (string, error) {
	t, err := s.targets(p)
	if err != nil {
		return "", err
	}
	if len(t) != 1 {
		return "", fmt.Errorf("a poll needs exactly one recipient or groupId, got %d", len(t))
	}
	return t[0], nil
}

// withPollVoteText gives a poll vote the text message Hermes reads, since the adapter only reads dataMessage.message.
func (s *Server) withPollVoteText(data []byte) []byte {
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		return data
	}
	env, _ := msg["envelope"].(map[string]any)
	dm, _ := env["dataMessage"].(map[string]any)
	vote, _ := dm["pollVote"].(map[string]any)
	if vote == nil || str(dm["message"]) != "" {
		return data
	}
	ts, _ := num(vote["targetSentTimestamp"])
	var indexes []int64
	if list, ok := vote["optionIndexes"].([]any); ok {
		for _, v := range list {
			if i, ok := num(v); ok {
				indexes = append(indexes, i)
			}
		}
	}
	dm["message"] = voteText(s.polls.get, ts, indexes)
	out, err := json.Marshal(msg)
	if err != nil {
		return data
	}
	return out
}

func voteText(lookup func(int64) (poll, bool), ts int64, indexes []int64) string {
	p, known := lookup(ts)
	ref := fmt.Sprintf("(poll %d)", ts)
	if len(indexes) == 0 {
		if known {
			return fmt.Sprintf("🗳️ Poll %q: vote withdrawn %s", p.question, ref)
		}
		return "🗳️ Poll vote withdrawn " + ref
	}
	var picked []string
	for _, i := range indexes {
		if known && i >= 0 && int(i) < len(p.options) {
			picked = append(picked, p.options[i])
		} else {
			picked = append(picked, fmt.Sprintf("option %d", i+1))
		}
	}
	if known {
		return fmt.Sprintf("🗳️ Poll %q: %s %s", p.question, strings.Join(picked, ", "), ref)
	}
	return fmt.Sprintf("🗳️ Poll vote: %s %s", strings.Join(picked, ", "), ref)
}
