package store

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

var migrations = migrate.NewMigrations()

type messageRow struct {
	bun.BaseModel `bun:"table:messages"`

	Seq        int64   `bun:"seq,pk,autoincrement"`
	ReceivedAt int64   `bun:"received_at,notnull"`
	SignalTS   int64   `bun:"signal_ts,notnull"`
	Kind       string  `bun:"kind,notnull"`
	Source     string  `bun:"source,notnull"`
	GroupID    *string `bun:"group_id"`
	Consumer   *string `bun:"consumer"`
	DedupeKey  *string `bun:"dedupe_key,unique"`
	Raw        []byte  `bun:"raw,notnull"`
}

type attachmentRow struct {
	bun.BaseModel `bun:"table:attachments"`

	ID          string  `bun:"id,pk,notnull"`
	Seq         int64   `bun:"seq,pk,notnull"`
	Consumer    *string `bun:"consumer"`
	ContentType string  `bun:"content_type"`
	Filename    string  `bun:"filename"`
	Size        int64   `bun:"size"`
}

type positionRow struct {
	bun.BaseModel `bun:"table:positions"`

	Consumer  string `bun:"consumer,pk,notnull"`
	Channel   string `bun:"channel,pk,notnull"`
	Seq       int64  `bun:"seq,notnull"`
	UpdatedAt int64  `bun:"updated_at,notnull"`
}

func migrateUp(ctx context.Context, db *bun.DB) error {
	m := migrate.NewMigrator(db, migrations)
	if err := m.Init(ctx); err != nil {
		return fmt.Errorf("init migrations: %w", err)
	}
	if _, err := m.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}
