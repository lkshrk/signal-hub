package message

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lkshrk/signal-hub/internal/groupid"
)

const (
	KindMessage  = "message"
	KindEdit     = "edit"
	KindReaction = "reaction"
	KindPollVote = "poll_vote"
	KindReceipt  = "receipt"
	KindTyping   = "typing"
	KindSync     = "sync"
	KindOther    = "other"
)

var Deliverable = []string{KindMessage, KindEdit, KindReaction, KindPollVote}

var ErrNoEnvelope = errors.New("frame has no envelope")

type AttachmentRef struct {
	ID          string
	ContentType string
	Filename    string
	Size        int64
}

type Parsed struct {
	Kind         string
	SignalTS     int64
	Source       string
	SourceNumber string
	SourceUUID   string
	GroupID      string
	Attachments  []AttachmentRef
}

type frame struct {
	Envelope *envelope `json:"envelope"`
}

type envelope struct {
	Source        string           `json:"source"`
	SourceNumber  string           `json:"sourceNumber"`
	SourceUUID    string           `json:"sourceUuid"`
	SourceName    string           `json:"sourceName"`
	SourceDevice  int              `json:"sourceDevice"`
	Timestamp     int64            `json:"timestamp"`
	DataMessage   *dataMessage     `json:"dataMessage"`
	EditMessage   *editMessage     `json:"editMessage"`
	SyncMessage   *json.RawMessage `json:"syncMessage"`
	ReceiptMsg    *json.RawMessage `json:"receiptMessage"`
	TypingMessage *typingMessage   `json:"typingMessage"`
}

type dataMessage struct {
	Timestamp   int64        `json:"timestamp"`
	Message     string       `json:"message"`
	Mentions    []mention    `json:"mentions"`
	Quote       *quote       `json:"quote"`
	Attachments []attachment `json:"attachments"`
	GroupInfo   *groupInfo   `json:"groupInfo"`
	Reaction    *reaction    `json:"reaction"`
	PollVote    *pollVote    `json:"pollVote"`
}

type editMessage struct {
	TargetSentTimestamp int64        `json:"targetSentTimestamp"`
	DataMessage         *dataMessage `json:"dataMessage"`
}

type typingMessage struct {
	GroupID string `json:"groupId"`
}

type groupInfo struct {
	GroupID   string `json:"groupId"`
	GroupName string `json:"groupName"`
}

type mention struct {
	Name   string `json:"name"`
	Number string `json:"number"`
	UUID   string `json:"uuid"`
	Start  int    `json:"start"`
	Length int    `json:"length"`
}

type quote struct {
	ID           int64  `json:"id"`
	Author       string `json:"author"`
	AuthorNumber string `json:"authorNumber"`
	AuthorUUID   string `json:"authorUuid"`
	Text         string `json:"text"`
}

type attachment struct {
	ID          string `json:"id"`
	ContentType string `json:"contentType"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	Caption     string `json:"caption"`
}

type reaction struct {
	Emoji               string `json:"emoji"`
	TargetAuthor        string `json:"targetAuthor"`
	TargetAuthorNumber  string `json:"targetAuthorNumber"`
	TargetAuthorUUID    string `json:"targetAuthorUuid"`
	TargetSentTimestamp int64  `json:"targetSentTimestamp"`
	IsRemove            bool   `json:"isRemove"`
}

type pollVote struct {
	TargetSentTimestamp int64 `json:"targetSentTimestamp"`
	OptionIndexes       []int `json:"optionIndexes"`
}

func decode(raw []byte) (*envelope, error) {
	var f frame
	if err := json.Unmarshal(raw, &f); err != nil || f.Envelope == nil {
		return nil, fmt.Errorf("%w", ErrNoEnvelope)
	}
	return f.Envelope, nil
}

func (e *envelope) kind() string {
	switch {
	case e.EditMessage != nil:
		return KindEdit
	case e.DataMessage != nil && e.DataMessage.Reaction != nil:
		return KindReaction
	case e.DataMessage != nil && e.DataMessage.PollVote != nil:
		return KindPollVote
	case e.DataMessage != nil:
		return KindMessage
	case e.SyncMessage != nil:
		return KindSync
	case e.ReceiptMsg != nil:
		return KindReceipt
	case e.TypingMessage != nil:
		return KindTyping
	}
	return KindOther
}

func (e *envelope) content() *dataMessage {
	if e.EditMessage != nil {
		return e.EditMessage.DataMessage
	}
	return e.DataMessage
}

func (e *envelope) source() string {
	switch {
	case e.SourceUUID != "":
		return e.SourceUUID
	case e.SourceNumber != "":
		return e.SourceNumber
	}
	return e.Source
}

func (e *envelope) groupID() (id, name string) {
	var raw string
	if dm := e.content(); dm != nil && dm.GroupInfo != nil {
		raw, name = dm.GroupInfo.GroupID, dm.GroupInfo.GroupName
	} else if e.TypingMessage != nil {
		raw = e.TypingMessage.GroupID
	}
	if raw == "" {
		return "", ""
	}
	id, err := groupid.Normalize(raw)
	if err != nil {
		return "", ""
	}
	return id, name
}

func Parse(raw []byte) (Parsed, error) {
	e, err := decode(raw)
	if err != nil {
		return Parsed{}, err
	}
	gid, _ := e.groupID()
	p := Parsed{
		Kind:         e.kind(),
		SignalTS:     e.Timestamp,
		Source:       e.source(),
		SourceNumber: e.SourceNumber,
		SourceUUID:   e.SourceUUID,
		GroupID:      gid,
	}
	if dm := e.content(); dm != nil {
		for _, a := range dm.Attachments {
			p.Attachments = append(p.Attachments, AttachmentRef{ID: a.ID, ContentType: a.ContentType, Filename: a.Filename, Size: a.Size})
		}
	}
	return p, nil
}
