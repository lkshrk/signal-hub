package store

import (
	"context"

	"github.com/uptrace/bun"
)

func init() {
	migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if _, err := tx.NewCreateTable().Model((*messageRow)(nil)).IfNotExists().Exec(ctx); err != nil {
				return err
			}
			if _, err := tx.NewCreateTable().Model((*attachmentRow)(nil)).IfNotExists().
				ForeignKey(`("seq") REFERENCES "messages" ("seq") ON DELETE CASCADE`).Exec(ctx); err != nil {
				return err
			}
			if _, err := tx.NewCreateTable().Model((*positionRow)(nil)).IfNotExists().Exec(ctx); err != nil {
				return err
			}
			if _, err := tx.NewCreateIndex().Model((*messageRow)(nil)).Index("messages_consumer_seq").
				Column("consumer", "seq").IfNotExists().Exec(ctx); err != nil {
				return err
			}
			_, err := tx.NewCreateIndex().Model((*messageRow)(nil)).Index("messages_received_at").
				Column("received_at").IfNotExists().Exec(ctx)
			return err
		})
	}, func(ctx context.Context, db *bun.DB) error {
		for _, table := range []string{"positions", "attachments", "messages"} {
			if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS ?", bun.Ident(table)); err != nil {
				return err
			}
		}
		return nil
	})
}
