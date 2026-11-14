package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/groupid"
)

const (
	internalA = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	hermesID  = "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="
	unknownID = "QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl8="
	ownerUUID = "0b6a4f2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b"
	botNumber = "+4915112345678"
	botUUID   = "9f8e7d6c-5b4a-4321-8fed-cba987654321"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "envelopes", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseKinds(t *testing.T) {
	cases := []struct {
		fixture string
		kind    string
		group   string
		source  string
		ts      int64
	}{
		{"group_text", KindMessage, internalA, ownerUUID, 1763114500000},
		{"group_hermes_text", KindMessage, hermesID, ownerUUID, 1763114510000},
		{"group_unknown_text", KindMessage, unknownID, ownerUUID, 1763114520000},
		{"group_quote_mentions", KindMessage, internalA, ownerUUID, 1763114400000},
		{"direct_attachment", KindMessage, "", ownerUUID, 1763114600000},
		{"edit", KindEdit, internalA, ownerUUID, 1763114700000},
		{"reaction", KindReaction, internalA, ownerUUID, 1763114800000},
		{"poll_vote", KindPollVote, internalA, ownerUUID, 1763114900000},
		{"receipt", KindReceipt, "", ownerUUID, 1763115000000},
		{"typing_group", KindTyping, internalA, ownerUUID, 1763115100000},
		{"sync", KindSync, "", ownerUUID, 1763115200000},
	}
	for _, c := range cases {
		t.Run(c.fixture, func(t *testing.T) {
			p, err := Parse(fixture(t, c.fixture))
			if err != nil {
				t.Fatal(err)
			}
			if p.Kind != c.kind || p.GroupID != c.group || p.Source != c.source || p.SignalTS != c.ts {
				t.Fatalf("got %+v", p)
			}
		})
	}
}

func TestParseKindOther(t *testing.T) {
	p, err := Parse([]byte(`{"envelope":{"sourceNumber":"+4915187654321","timestamp":5,"callMessage":{}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != KindOther || p.Source != "+4915187654321" {
		t.Fatalf("got %+v", p)
	}
}

func TestParseKeepsUnparsableGroupAsDirect(t *testing.T) {
	raw := `{"envelope":{"sourceNumber":"+4915187654321","timestamp":5,"dataMessage":{"message":"x","groupInfo":{"groupId":"not-a-group"}}}}`
	p, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != KindMessage || p.GroupID != "" {
		t.Fatalf("got %+v", p)
	}
}

func TestParseSourcePrefersUUID(t *testing.T) {
	raw := fixture(t, "group_text")
	p, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.Source != ownerUUID || p.SourceUUID != ownerUUID || p.SourceNumber != "+4915187654321" {
		t.Fatalf("got %+v", p)
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	delete(m["envelope"].(map[string]any), "sourceUuid")
	noUUID, _ := json.Marshal(m)
	p, err = Parse(noUUID)
	if err != nil {
		t.Fatal(err)
	}
	if p.Source != "+4915187654321" {
		t.Fatalf("source = %q", p.Source)
	}

	delete(m["envelope"].(map[string]any), "sourceNumber")
	onlySource, _ := json.Marshal(m)
	p, err = Parse(onlySource)
	if err != nil {
		t.Fatal(err)
	}
	if p.Source != "+4915187654321" {
		t.Fatalf("source = %q", p.Source)
	}
}

func TestParseRejectsFrameWithoutEnvelope(t *testing.T) {
	for _, in := range [][]byte{fixture(t, "no_envelope"), []byte("not json"), []byte(`{"envelope":null}`), nil} {
		if _, err := Parse(in); !errors.Is(err, ErrNoEnvelope) {
			t.Fatalf("%q: err = %v", in, err)
		}
		if _, err := Build(1, time.Now(), in); !errors.Is(err, ErrNoEnvelope) {
			t.Fatalf("build %q: err = %v", in, err)
		}
	}
}

func TestParseCollectsAttachments(t *testing.T) {
	p, err := Parse(fixture(t, "direct_attachment"))
	if err != nil {
		t.Fatal(err)
	}
	want := []AttachmentRef{{ID: "x1y2.jpg", ContentType: "image/jpeg", Size: 48213, Filename: "shot.jpg"}}
	if !reflect.DeepEqual(p.Attachments, want) {
		t.Fatalf("got %+v", p.Attachments)
	}
}

func TestBuildGroupQuoteMentions(t *testing.T) {
	at := time.Date(2026, 11, 14, 10, 0, 0, 123000000, time.UTC)
	m, err := Build(7, at, fixture(t, "group_quote_mentions"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 7 || m.Kind != KindMessage || m.Timestamp != 1763114400000 || !m.ReceivedAt.Equal(at) {
		t.Fatalf("got %+v", m)
	}
	wantGroup := &Group{ID: internalA, InternalID: internalA, RESTID: groupid.REST(internalA), Name: "dsh"}
	if !reflect.DeepEqual(m.Group, wantGroup) {
		t.Fatalf("group = %+v", m.Group)
	}
	wantQuote := &Quote{Timestamp: 1763114300000, Author: Author{Number: botNumber, UUID: botUUID}, Text: "Tests failed"}
	if !reflect.DeepEqual(m.Quote, wantQuote) {
		t.Fatalf("quote = %+v", m.Quote)
	}
	wantMentions := []Mention{{Start: 0, Length: 1, Number: botNumber, UUID: botUUID, Name: botNumber}}
	if !reflect.DeepEqual(m.Mentions, wantMentions) {
		t.Fatalf("mentions = %+v", m.Mentions)
	}
	wantSender := Sender{Number: "+4915187654321", UUID: ownerUUID, Name: "Owner", Device: 1}
	if m.Sender != wantSender {
		t.Fatalf("sender = %+v", m.Sender)
	}
	if !strings.HasSuffix(m.Text, " please retry") {
		t.Fatalf("text = %q", m.Text)
	}
}

func TestBuildDirectHasNullGroupAndEmptyArrays(t *testing.T) {
	m, err := Build(1, time.Now(), fixture(t, "direct_attachment"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"group":null`, `"mentions":[]`, `"quote":null`, `"reaction":null`, `"pollVote":null`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %s in %s", want, b)
		}
	}
	if len(m.Attachments) != 1 || m.Attachments[0].ID != "x1y2.jpg" || m.Attachments[0].Size != 48213 {
		t.Fatalf("attachments = %+v", m.Attachments)
	}

	m, err = Build(2, time.Now(), fixture(t, "receipt"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(m)
	if !strings.Contains(string(b), `"attachments":[]`) {
		t.Fatalf("missing empty attachments in %s", b)
	}
}

func TestBuildJSONFieldNames(t *testing.T) {
	m, err := Build(1, time.Now(), fixture(t, "group_quote_mentions"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(m)
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatal(err)
	}
	var got []string
	for k := range top {
		got = append(got, k)
	}
	want := []string{"seq", "receivedAt", "timestamp", "kind", "sender", "group", "text", "quote", "mentions", "attachments", "editOf", "reaction", "pollVote", "raw"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v", got)
	}

	var nested map[string]map[string]any
	_ = json.Unmarshal(b, &nested)
	for obj, keys := range map[string][]string{
		"sender": {"number", "uuid", "name", "device"},
		"group":  {"id", "internalId", "restId", "name"},
	} {
		var have []string
		for k := range nested[obj] {
			have = append(have, k)
		}
		sort.Strings(have)
		sort.Strings(keys)
		if !reflect.DeepEqual(have, keys) {
			t.Fatalf("%s keys = %v", obj, have)
		}
	}
}

func TestBuildReceivedAtFormat(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 123000000, time.FixedZone("x", 7200))
	m, err := Build(1, at, fixture(t, "group_text"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(m)
	if !strings.Contains(string(b), `"receivedAt":"2026-11-14T10:00:00.123Z"`) {
		t.Fatalf("got %s", b)
	}
	at = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	m, _ = Build(1, at, fixture(t, "group_text"))
	b, _ = json.Marshal(m)
	if !strings.Contains(string(b), `"receivedAt":"2026-11-14T10:00:00.000Z"`) {
		t.Fatalf("got %s", b)
	}
}

func TestBuildKeepsRawIntact(t *testing.T) {
	in := []byte("  {\n \"envelope\" : {\"sourceNumber\":\"+4915187654321\",\"timestamp\":5,\"dataMessage\":{\"message\":\"x\"}} ,\n\"account\":\"+4915112345678\"}\n")
	orig := append([]byte(nil), in...)
	m, err := Build(1, time.Now(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Raw, orig) {
		t.Fatalf("raw changed: %q", m.Raw)
	}
	in[3] = 'X'
	if !bytes.Equal(m.Raw, orig) {
		t.Fatal("raw aliases the input buffer")
	}

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Raw json.RawMessage `json:"raw"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal(out.Raw, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(orig, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("raw = %s", out.Raw)
	}
}

func TestBuildEditCarriesTarget(t *testing.T) {
	m, err := Build(1, time.Now(), fixture(t, "edit"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Kind != KindEdit || m.EditOf != 1763114500000 || m.Text != "please fix the build now" {
		t.Fatalf("got %+v", m)
	}
	if m.Group == nil || m.Group.ID != internalA || m.Group.Name != "dsh" {
		t.Fatalf("group = %+v", m.Group)
	}
	if m.Timestamp != 1763114700000 {
		t.Fatalf("timestamp = %d", m.Timestamp)
	}
}

func TestBuildReactionAndPollVote(t *testing.T) {
	m, err := Build(1, time.Now(), fixture(t, "reaction"))
	if err != nil {
		t.Fatal(err)
	}
	wantR := &Reaction{Emoji: "👍", TargetAuthor: Author{Number: botNumber, UUID: botUUID}, TargetTimestamp: 1763114300000, Remove: false}
	if m.Kind != KindReaction || !reflect.DeepEqual(m.Reaction, wantR) || m.PollVote != nil {
		t.Fatalf("got %+v", m)
	}

	m, err = Build(2, time.Now(), fixture(t, "poll_vote"))
	if err != nil {
		t.Fatal(err)
	}
	wantP := &PollVote{TargetTimestamp: 1763114850000, OptionIndexes: []int{0, 2}}
	if m.Kind != KindPollVote || !reflect.DeepEqual(m.PollVote, wantP) || m.Reaction != nil {
		t.Fatalf("got %+v", m)
	}
}

func TestBuildTypingGroupHasGroup(t *testing.T) {
	m, err := Build(1, time.Now(), fixture(t, "typing_group"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Kind != KindTyping || m.Group == nil || m.Group.ID != internalA || m.Group.RESTID != groupid.REST(internalA) {
		t.Fatalf("got %+v", m)
	}
}

func TestBuildSecondGroupFixtures(t *testing.T) {
	for name, id := range map[string]string{"group_hermes_text": hermesID, "group_unknown_text": unknownID} {
		m, err := Build(1, time.Now(), fixture(t, name))
		if err != nil {
			t.Fatal(err)
		}
		if m.Group == nil || m.Group.ID != id {
			t.Fatalf("%s: group = %+v", name, m.Group)
		}
	}
}

func TestDeliverable(t *testing.T) {
	want := []string{KindMessage, KindEdit, KindReaction, KindPollVote}
	if !reflect.DeepEqual(Deliverable, want) {
		t.Fatalf("got %v", Deliverable)
	}
}
