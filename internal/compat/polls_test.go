package compat

import "testing"

func TestVoteText(t *testing.T) {
	known := func(ts int64) (poll, bool) {
		return poll{question: "Approve plan: polls?", options: []string{"✅ Approve", "✏️ Changes", "❌ Reject"}}, ts == 7
	}
	cases := []struct {
		ts      int64
		indexes []int64
		want    string
	}{
		{7, []int64{0}, `🗳️ Poll "Approve plan: polls?": ✅ Approve (poll 7)`},
		{7, []int64{1, 2}, `🗳️ Poll "Approve plan: polls?": ✏️ Changes, ❌ Reject (poll 7)`},
		{7, []int64{9}, `🗳️ Poll "Approve plan: polls?": option 10 (poll 7)`},
		{7, nil, `🗳️ Poll "Approve plan: polls?": vote withdrawn (poll 7)`},
		{8, []int64{1}, `🗳️ Poll vote: option 2 (poll 8)`},
		{8, nil, `🗳️ Poll vote withdrawn (poll 8)`},
	}
	for _, c := range cases {
		if got := voteText(known, c.ts, c.indexes); got != c.want {
			t.Errorf("voteText(%d, %v) = %q, want %q", c.ts, c.indexes, got, c.want)
		}
	}
}

func TestPollMemoryKeeps256(t *testing.T) {
	var ps pollStore
	for i := int64(1); i <= pollMemory+1; i++ {
		ps.put(i, poll{question: "q", options: []string{"a", "b"}})
	}
	if pollMemory != 256 {
		t.Fatalf("pollMemory = %d", pollMemory)
	}
	if _, ok := ps.get(1); ok {
		t.Fatal("first poll kept")
	}
	for _, ts := range []int64{2, pollMemory + 1} {
		if _, ok := ps.get(ts); !ok {
			t.Fatalf("poll %d evicted", ts)
		}
	}
}

func TestOtherMessagesLeftUnchanged(t *testing.T) {
	s := &Server{}
	for _, in := range []string{
		`{"envelope":{"source":"+492","dataMessage":{"message":"ping"}},"account":"+491"}`,
		`{"envelope":{"source":"+492","typingMessage":{"action":"STARTED"}},"account":"+491"}`,
		`not json`,
	} {
		if out := string(s.withPollVoteText([]byte(in))); out != in {
			t.Errorf("withPollVoteText(%s) = %s", in, out)
		}
	}
}
