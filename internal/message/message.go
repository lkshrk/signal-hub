package message

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/lkshrk/signal-hub/internal/groupid"
)

const receivedAtLayout = "2006-01-02T15:04:05.000Z07:00"

type Sender struct {
	Number string `json:"number"`
	UUID   string `json:"uuid"`
	Name   string `json:"name"`
	Device int    `json:"device"`
}

type Group struct {
	ID         string `json:"id"`
	InternalID string `json:"internalId"`
	RESTID     string `json:"restId"`
	Name       string `json:"name"`
}

type Author struct {
	Number string `json:"number"`
	UUID   string `json:"uuid"`
}

type Quote struct {
	Timestamp int64  `json:"timestamp"`
	Author    Author `json:"author"`
	Text      string `json:"text"`
}

type Mention struct {
	Start  int    `json:"start"`
	Length int    `json:"length"`
	Number string `json:"number"`
	UUID   string `json:"uuid"`
	Name   string `json:"name"`
}

type Attachment struct {
	ID          string `json:"id"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size"`
	Filename    string `json:"filename"`
	Caption     string `json:"caption"`
}

type Reaction struct {
	Emoji           string `json:"emoji"`
	TargetAuthor    Author `json:"targetAuthor"`
	TargetTimestamp int64  `json:"targetTimestamp"`
	Remove          bool   `json:"remove"`
}

type PollVote struct {
	TargetTimestamp int64 `json:"targetTimestamp"`
	OptionIndexes   []int `json:"optionIndexes"`
}

type Message struct {
	Seq         int64           `json:"seq"`
	ReceivedAt  time.Time       `json:"receivedAt"`
	Timestamp   int64           `json:"timestamp"`
	Kind        string          `json:"kind"`
	Sender      Sender          `json:"sender"`
	Group       *Group          `json:"group"`
	Text        string          `json:"text"`
	Quote       *Quote          `json:"quote"`
	Mentions    []Mention       `json:"mentions"`
	Attachments []Attachment    `json:"attachments"`
	EditOf      int64           `json:"editOf"`
	Reaction    *Reaction       `json:"reaction"`
	PollVote    *PollVote       `json:"pollVote"`
	Raw         json.RawMessage `json:"raw"`
}

func (m Message) MarshalJSON() ([]byte, error) {
	type plain Message
	return json.Marshal(struct {
		plain
		ReceivedAt string `json:"receivedAt"`
	}{plain(m), m.ReceivedAt.UTC().Format(receivedAtLayout)})
}

func author(ref, number, uuid string) Author {
	if number == "" && uuid == "" && strings.HasPrefix(ref, "+") {
		number = ref
	}
	return Author{Number: number, UUID: uuid}
}

func Build(seq int64, receivedAt time.Time, raw []byte) (Message, error) {
	e, err := decode(raw)
	if err != nil {
		return Message{}, err
	}
	m := Message{
		Seq:         seq,
		ReceivedAt:  receivedAt,
		Timestamp:   e.Timestamp,
		Kind:        e.kind(),
		Sender:      Sender{Number: e.SourceNumber, UUID: e.SourceUUID, Name: e.SourceName, Device: e.SourceDevice},
		Mentions:    []Mention{},
		Attachments: []Attachment{},
		Raw:         append(json.RawMessage(nil), raw...),
	}
	if m.Sender.Number == "" && strings.HasPrefix(e.Source, "+") {
		m.Sender.Number = e.Source
	}
	if id, name := e.groupID(); id != "" {
		m.Group = &Group{ID: id, InternalID: id, RESTID: groupid.REST(id), Name: name}
	}
	if e.EditMessage != nil {
		m.EditOf = e.EditMessage.TargetSentTimestamp
	}
	dm := e.content()
	if dm == nil {
		return m, nil
	}
	m.Text = dm.Message
	if q := dm.Quote; q != nil {
		m.Quote = &Quote{Timestamp: q.ID, Author: author(q.Author, q.AuthorNumber, q.AuthorUUID), Text: q.Text}
	}
	for _, mt := range dm.Mentions {
		m.Mentions = append(m.Mentions, Mention{Start: mt.Start, Length: mt.Length, Number: mt.Number, UUID: mt.UUID, Name: mt.Name})
	}
	for _, a := range dm.Attachments {
		m.Attachments = append(m.Attachments, Attachment{ID: a.ID, ContentType: a.ContentType, Size: a.Size, Filename: a.Filename, Caption: a.Caption})
	}
	if r := dm.Reaction; r != nil && m.Kind == KindReaction {
		m.Reaction = &Reaction{
			Emoji:           r.Emoji,
			TargetAuthor:    author(r.TargetAuthor, r.TargetAuthorNumber, r.TargetAuthorUUID),
			TargetTimestamp: r.TargetSentTimestamp,
			Remove:          r.IsRemove,
		}
	}
	if v := dm.PollVote; v != nil && m.Kind == KindPollVote {
		idx := v.OptionIndexes
		if idx == nil {
			idx = []int{}
		}
		m.PollVote = &PollVote{TargetTimestamp: v.TargetSentTimestamp, OptionIndexes: idx}
	}
	return m, nil
}
