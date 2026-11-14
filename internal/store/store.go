package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/uptrace/bun"

	"github.com/lkshrk/signal-hub/internal/message"
)

const (
	ChannelEvents = "events"
	ChannelCompat = "compat"

	pruneBatch = 1000
)

var errDuplicate = errors.New("duplicate message")

type Entry struct {
	Parsed     message.Parsed
	Consumer   string
	ReceivedAt time.Time
	Raw        []byte
}

type AttachmentMeta struct {
	ID          string
	ContentType string
	Filename    string
	Size        int64
	Seq         int64
}

type Store struct {
	wdb *bun.DB
	rdb *bun.DB

	mu      sync.Mutex
	changed chan struct{}
}

func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	wdb, rdb, err := openDatabases(path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := migrateUp(ctx, wdb); err != nil {
		_ = rdb.Close()
		_ = wdb.Close()
		return nil, err
	}
	return &Store{wdb: wdb, rdb: rdb, changed: make(chan struct{})}, nil
}

func (s *Store) Close() error {
	return errors.Join(s.rdb.Close(), s.wdb.Close())
}

func nullable(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func (s *Store) Append(ctx context.Context, e Entry) (int64, bool, error) {
	row := messageRow{
		ReceivedAt: e.ReceivedAt.UnixMilli(),
		SignalTS:   e.Parsed.SignalTS,
		Kind:       e.Parsed.Kind,
		Source:     e.Parsed.Source,
		GroupID:    nullable(e.Parsed.GroupID),
		Consumer:   nullable(e.Consumer),
		Raw:        e.Raw,
	}
	if e.Parsed.SignalTS != 0 {
		key := e.Parsed.Kind + "|" + e.Parsed.Source + "|" + strconv.FormatInt(e.Parsed.SignalTS, 10)
		row.DedupeKey = &key
	}
	if row.Raw == nil {
		row.Raw = []byte{}
	}
	err := s.wdb.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.NewInsert().Model(&row).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return errDuplicate
			}
			return err
		}
		if len(e.Parsed.Attachments) == 0 {
			return nil
		}
		rows := make([]attachmentRow, 0, len(e.Parsed.Attachments))
		seen := map[string]bool{}
		for _, a := range e.Parsed.Attachments {
			if seen[a.ID] {
				continue
			}
			seen[a.ID] = true
			rows = append(rows, attachmentRow{ID: a.ID, Seq: row.Seq, Consumer: row.Consumer, ContentType: a.ContentType, Filename: a.Filename, Size: a.Size})
		}
		_, err := tx.NewInsert().Model(&rows).Exec(ctx)
		return err
	})
	if errors.Is(err, errDuplicate) {
		return 0, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	return row.Seq, false, nil
}

func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *Store) Head(ctx context.Context) (int64, error) {
	return headSeq(ctx, s.rdb)
}

func (s *Store) Oldest(ctx context.Context) (int64, error) {
	var lowest sql.NullInt64
	if err := s.rdb.NewSelect().Model((*messageRow)(nil)).ColumnExpr("MIN(seq)").Scan(ctx, &lowest); err != nil {
		return 0, err
	}
	if !lowest.Valid {
		return 0, nil
	}
	return lowest.Int64 - 1, nil
}

func (s *Store) Position(ctx context.Context, consumer, channel string) (int64, bool, error) {
	var row positionRow
	err := s.rdb.NewSelect().Model(&row).Where("consumer = ?", consumer).Where("channel = ?", channel).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return row.Seq, true, nil
}

func (s *Store) SetPosition(ctx context.Context, consumer, channel string, seq int64) error {
	if seq < 0 {
		return fmt.Errorf("position %d is negative", seq)
	}
	row := positionRow{Consumer: consumer, Channel: channel, Seq: seq, UpdatedAt: time.Now().UnixMilli()}
	_, err := s.wdb.NewInsert().Model(&row).
		On("CONFLICT (consumer, channel) DO UPDATE").
		Set("seq = EXCLUDED.seq").
		Set("updated_at = EXCLUDED.updated_at").
		Where("?TableAlias.seq < EXCLUDED.seq").
		Exec(ctx)
	return err
}

func (s *Store) Prune(ctx context.Context, before time.Time) (int64, error) {
	var total int64
	cutoff := before.UnixMilli()
	for {
		batch := s.wdb.NewSelect().Model((*messageRow)(nil)).Column("seq").
			Where("received_at < ?", cutoff).Order("seq").Limit(pruneBatch)
		res, err := s.wdb.NewDelete().Model((*messageRow)(nil)).Where("seq IN (?)", batch).Exec(ctx)
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < pruneBatch {
			return total, nil
		}
	}
}

func (s *Store) Attachment(ctx context.Context, consumer, id string) (AttachmentMeta, bool, error) {
	var row attachmentRow
	sel := s.rdb.NewSelect().Model(&row).Where("id = ?", id)
	if consumer == "" {
		sel = sel.Where("consumer IS NULL")
	} else {
		sel = sel.Where("consumer = ?", consumer)
	}
	err := sel.Order("seq DESC").Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AttachmentMeta{}, false, nil
	}
	if err != nil {
		return AttachmentMeta{}, false, err
	}
	return AttachmentMeta{ID: row.ID, ContentType: row.ContentType, Filename: row.Filename, Size: row.Size, Seq: row.Seq}, true, nil
}

func (s *Store) Writable(ctx context.Context) error {
	return probeWritable(ctx, s.wdb)
}
