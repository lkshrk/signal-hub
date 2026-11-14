package store

import (
	"context"
	"slices"
	"time"
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
	page := Page{Records: []Record{}, Next: q.After}
	sel := s.rdb.NewSelect().Model((*messageRow)(nil)).
		Column("seq", "received_at", "kind", "consumer", "raw").
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
	for _, r := range rows {
		page.Next = r.Seq
		switch {
		case !q.NotBefore.IsZero() && r.ReceivedAt < notBefore:
			page.Missed++
		case q.Kinds != nil && !slices.Contains(q.Kinds, r.Kind):
		default:
			rec := Record{Seq: r.Seq, ReceivedAt: time.UnixMilli(r.ReceivedAt).UTC(), Kind: r.Kind, Raw: r.Raw}
			if r.Consumer != nil {
				rec.Consumer = *r.Consumer
			}
			page.Records = append(page.Records, rec)
			if q.Limit > 0 && len(page.Records) >= q.Limit {
				return page, nil
			}
		}
	}
	return page, nil
}
