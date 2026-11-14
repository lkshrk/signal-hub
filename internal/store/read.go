package store

import (
	"context"
	"database/sql"
	"slices"
	"time"

	"github.com/uptrace/bun"
)

const maxScan = 5000

type Record struct {
	Seq        int64
	ReceivedAt time.Time
	Kind       string
	Consumer   string
	Raw        []byte
}

type Query struct {
	Consumer  string
	After     int64
	Kinds     []string
	NotBefore time.Time
	Limit     int
}

type Page struct {
	Records []Record
	Next    int64
	Missed  int
}

func (s *Store) Read(ctx context.Context, q Query) (Page, error) {
	var page Page
	err := s.rdb.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		var err error
		page, err = readPage(ctx, tx, q)
		return err
	})
	if err != nil {
		return Page{}, err
	}
	return page, nil
}

func readPage(ctx context.Context, db bun.IDB, q Query) (Page, error) {
	page := Page{Records: []Record{}, Next: q.After}
	sel := db.NewSelect().Model((*messageRow)(nil)).
		Column("seq", "received_at", "kind", "consumer").
		Where("seq > ?", q.After).Order("seq").Limit(maxScan)
	if q.Consumer == "" {
		sel = sel.Where("consumer IS NULL")
	} else {
		sel = sel.Where("consumer = ?", q.Consumer)
	}
	var rows []messageRow
	if err := sel.Scan(ctx, &rows); err != nil {
		return Page{}, err
	}
	notBefore := q.NotBefore.UnixMilli()
scan:
	for _, r := range rows {
		page.Next = r.Seq
		switch {
		case !q.NotBefore.IsZero() && r.ReceivedAt < notBefore:
			page.Missed++
		case q.Kinds != nil && !slices.Contains(q.Kinds, r.Kind):
		default:
			rec := Record{Seq: r.Seq, ReceivedAt: time.UnixMilli(r.ReceivedAt).UTC(), Kind: r.Kind}
			if r.Consumer != nil {
				rec.Consumer = *r.Consumer
			}
			page.Records = append(page.Records, rec)
			if q.Limit > 0 && len(page.Records) >= q.Limit {
				break scan
			}
		}
	}
	return page, loadRaw(ctx, db, &page)
}

func loadRaw(ctx context.Context, db bun.IDB, page *Page) error {
	if len(page.Records) == 0 {
		return nil
	}
	ids := make([]int64, len(page.Records))
	for i, r := range page.Records {
		ids[i] = r.Seq
	}
	var rows []messageRow
	if err := db.NewSelect().Model(&rows).Column("seq", "raw").Where("seq IN (?)", bun.List(ids)).Scan(ctx); err != nil {
		return err
	}
	raw := make(map[int64][]byte, len(rows))
	for _, r := range rows {
		raw[r.Seq] = r.Raw
	}
	kept := page.Records[:0]
	for _, rec := range page.Records {
		if b, ok := raw[rec.Seq]; ok {
			rec.Raw = b
			kept = append(kept, rec)
		}
	}
	page.Records = kept
	return nil
}
