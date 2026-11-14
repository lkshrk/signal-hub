package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/message"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func entry(consumer, kind string, ts int64, at time.Time) Entry {
	return Entry{
		Parsed:     message.Parsed{Kind: kind, SignalTS: ts, Source: "+4912345"},
		Consumer:   consumer,
		ReceivedAt: at,
		Raw:        []byte(`{"envelope":{}}`),
	}
}

func mustAppend(t *testing.T, s *Store, e Entry) int64 {
	t.Helper()
	seq, dup, err := s.Append(context.Background(), e)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if dup {
		t.Fatalf("Append reported a duplicate")
	}
	return seq
}

func seqs(p Page) []int64 {
	out := []int64{}
	for _, r := range p.Records {
		out = append(out, r.Seq)
	}
	return out
}

func equalSeqs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestOpenCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "hub.db")
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("parent mode = %v, want 0700", info.Mode().Perm())
	}
}

func TestAppendAssignsIncreasingSeq(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	for i := int64(1); i <= 3; i++ {
		if got := mustAppend(t, s, entry("dsh", message.KindMessage, i, now)); got != i {
			t.Fatalf("seq = %d, want %d", got, i)
		}
	}
	head, err := s.Head(context.Background())
	if err != nil || head != 3 {
		t.Fatalf("Head = %d, %v", head, err)
	}
}

func TestAppendDedupesSameSourceTimestampKind(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	now := time.Now()
	mustAppend(t, s, entry("dsh", message.KindMessage, 100, now))
	seq, dup, err := s.Append(ctx, entry("dsh", message.KindMessage, 100, now))
	if err != nil || !dup || seq != 0 {
		t.Fatalf("second append = %d, %v, %v", seq, dup, err)
	}
	if head, _ := s.Head(ctx); head != 1 {
		t.Fatalf("Head = %d, want 1", head)
	}
	if got := mustAppend(t, s, entry("dsh", message.KindReceipt, 100, now)); got != 2 {
		t.Fatalf("other kind seq = %d, want 2", got)
	}
}

func TestAppendZeroTimestampNeverDedupes(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	mustAppend(t, s, entry("dsh", message.KindTyping, 0, now))
	mustAppend(t, s, entry("dsh", message.KindTyping, 0, now))
	if head, _ := s.Head(context.Background()); head != 2 {
		t.Fatalf("Head = %d, want 2", head)
	}
}

func TestReadFiltersConsumerAndKinds(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	mustAppend(t, s, entry("dsh", message.KindMessage, 1, now))
	mustAppend(t, s, entry("hermes", message.KindMessage, 2, now))
	mustAppend(t, s, entry("dsh", message.KindReceipt, 3, now))
	mustAppend(t, s, entry("dsh", message.KindMessage, 4, now))
	p, err := s.Read(context.Background(), Query{Consumer: "dsh", Kinds: message.Deliverable, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := seqs(p); !equalSeqs(got, []int64{1, 4}) || p.Next != 4 {
		t.Fatalf("seqs = %v next = %d", got, p.Next)
	}
	if p.Records[0].Kind != message.KindMessage || p.Records[0].Consumer != "dsh" || string(p.Records[0].Raw) != `{"envelope":{}}` {
		t.Fatalf("record = %+v", p.Records[0])
	}
}

func TestReadNothingKeepsAfter(t *testing.T) {
	s := openStore(t)
	p, err := s.Read(context.Background(), Query{Consumer: "dsh", After: 7, Limit: 10})
	if err != nil || len(p.Records) != 0 || p.Next != 7 {
		t.Fatalf("page = %+v, %v", p, err)
	}
}

func TestReadCountsMissedByReceivedAt(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	a := entry("dsh", message.KindMessage, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), now.Add(-2*time.Hour))
	b := entry("dsh", message.KindMessage, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), now)
	mustAppend(t, s, a)
	seqB := mustAppend(t, s, b)
	p, err := s.Read(context.Background(), Query{Consumer: "dsh", Kinds: message.Deliverable, NotBefore: now.Add(-time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := seqs(p); !equalSeqs(got, []int64{seqB}) || p.Missed != 1 || p.Next != seqB {
		t.Fatalf("seqs = %v missed = %d next = %d", got, p.Missed, p.Next)
	}
}

func TestReadLimit(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	for i := int64(1); i <= 5; i++ {
		mustAppend(t, s, entry("dsh", message.KindMessage, i, now))
	}
	p, err := s.Read(context.Background(), Query{Consumer: "dsh", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := seqs(p); !equalSeqs(got, []int64{1, 2}) || p.Next != 2 {
		t.Fatalf("seqs = %v next = %d", got, p.Next)
	}
}

func TestChangedClosesOnInsertNotOnDuplicate(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	mustAppend(t, s, entry("dsh", message.KindMessage, 1, now))
	ch := s.Changed()
	if _, dup, err := s.Append(context.Background(), entry("dsh", message.KindMessage, 1, now)); err != nil || !dup {
		t.Fatalf("dup append = %v, %v", dup, err)
	}
	select {
	case <-ch:
		t.Fatal("channel closed by a duplicate")
	case <-time.After(50 * time.Millisecond):
	}
	mustAppend(t, s, entry("dsh", message.KindMessage, 2, now))
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel not closed by an insert")
	}
}

func TestPositionOnlyMovesForward(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, ok, err := s.Position(ctx, "dsh", ChannelEvents); err != nil || ok {
		t.Fatalf("unknown position = %v, %v", ok, err)
	}
	if err := s.SetPosition(ctx, "dsh", ChannelEvents, 5); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPosition(ctx, "dsh", ChannelEvents, 3); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := s.Position(ctx, "dsh", ChannelEvents); err != nil || !ok || seq != 5 {
		t.Fatalf("position = %d, %v, %v", seq, ok, err)
	}
	if _, ok, _ := s.Position(ctx, "dsh", ChannelCompat); ok {
		t.Fatal("compat position should be unknown")
	}
}

func attachmentEntry(consumer, kind string, ts int64, at time.Time, id string) Entry {
	e := entry(consumer, kind, ts, at)
	e.Parsed.Attachments = []message.AttachmentRef{{ID: id, ContentType: "image/png", Filename: "a.png", Size: 42}}
	return e
}

func TestPruneByReceivedAtCascadesAttachments(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	now := time.Now()
	mustAppend(t, s, attachmentEntry("dsh", message.KindMessage, now.UnixMilli(), now.Add(-15*24*time.Hour), "old"))
	mustAppend(t, s, attachmentEntry("dsh", message.KindMessage, now.Add(-30*24*time.Hour).UnixMilli(), now, "new"))
	n, err := s.Prune(ctx, now.Add(-14*24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("Prune = %d, %v", n, err)
	}
	if _, ok, _ := s.Attachment(ctx, "dsh", "old"); ok {
		t.Fatal("old attachment survived")
	}
	if _, ok, _ := s.Attachment(ctx, "dsh", "new"); !ok {
		t.Fatal("new attachment lost")
	}
	if oldest, _ := s.Oldest(ctx); oldest != 1 {
		t.Fatalf("Oldest = %d, want 1", oldest)
	}
}

func TestPruneBatches(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	old := time.Now().Add(-time.Hour)
	for i := int64(1); i <= 2500; i++ {
		mustAppend(t, s, entry("dsh", message.KindMessage, i, old))
	}
	n, err := s.Prune(ctx, time.Now())
	if err != nil || n != 2500 {
		t.Fatalf("Prune = %d, %v", n, err)
	}
}

func TestSeqNeverReusedAfterPrune(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	now := time.Now()
	mustAppend(t, s, entry("dsh", message.KindMessage, 1, now))
	mustAppend(t, s, entry("dsh", message.KindMessage, 2, now))
	before, _ := s.Head(ctx)
	if _, err := s.Prune(ctx, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if head, _ := s.Head(ctx); head != before {
		t.Fatalf("Head after prune = %d, want %d", head, before)
	}
	if oldest, _ := s.Oldest(ctx); oldest != 0 {
		t.Fatalf("Oldest on empty log = %d", oldest)
	}
	if got := mustAppend(t, s, entry("dsh", message.KindMessage, 3, now)); got <= before {
		t.Fatalf("seq %d reused, head was %d", got, before)
	}
}

func TestMessagesTableUsesAutoincrement(t *testing.T) {
	s := openStore(t)
	var ddl string
	if err := s.wdb.QueryRowContext(context.Background(), `SELECT sql FROM sqlite_master WHERE name = 'messages'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(ddl), "AUTOINCREMENT") {
		t.Fatalf("messages DDL has no AUTOINCREMENT: %s", ddl)
	}
}

func TestAttachmentLookupScopedToConsumer(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	seq := mustAppend(t, s, attachmentEntry("dsh", message.KindMessage, 1, time.Now(), "att1"))
	meta, ok, err := s.Attachment(ctx, "dsh", "att1")
	if err != nil || !ok {
		t.Fatalf("Attachment = %v, %v", ok, err)
	}
	want := AttachmentMeta{ID: "att1", ContentType: "image/png", Filename: "a.png", Size: 42, Seq: seq}
	if meta != want {
		t.Fatalf("meta = %+v, want %+v", meta, want)
	}
	if _, ok, _ := s.Attachment(ctx, "hermes", "att1"); ok {
		t.Fatal("other consumer can see the attachment")
	}
}

func TestReopenKeepsData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hub.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, entry("dsh", message.KindMessage, 1, time.Now()))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if head, _ := s.Head(ctx); head != 1 {
		t.Fatalf("Head = %d, want 1", head)
	}
	var applied int
	if err := s.wdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM bun_migrations`).Scan(&applied); err != nil || applied != 1 {
		t.Fatalf("applied migrations = %d, %v", applied, err)
	}
}

func TestWritableFailsAfterClose(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	if err := s.Writable(ctx); err != nil {
		t.Fatalf("Writable: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Writable(ctx); err == nil {
		t.Fatal("Writable succeeded after Close")
	}
}

func TestConcurrentIngestPullPruneNoBusy(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	const total = 2000
	var done sync.WaitGroup
	var bg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 64)
	var pruned atomic.Int64
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}

	done.Add(1)
	go func() {
		defer done.Done()
		old := time.Now().Add(-time.Hour)
		for i := int64(1); i <= total; i++ {
			if _, _, err := s.Append(ctx, entry("dsh", message.KindMessage, i, old)); err != nil {
				report(err)
				return
			}
		}
	}()
	for range 8 {
		bg.Add(1)
		go func() {
			defer bg.Done()
			var after int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, err := s.Read(ctx, Query{Consumer: "dsh", After: after, Kinds: message.Deliverable, Limit: 50})
				if err != nil {
					report(err)
					return
				}
				prev := after
				for _, r := range p.Records {
					if len(r.Raw) == 0 {
						report(fmt.Errorf("seq %d returned without raw", r.Seq))
						return
					}
					if r.Seq <= prev {
						report(fmt.Errorf("seq %d not above %d", r.Seq, prev))
						return
					}
					prev = r.Seq
				}
				if p.Next < after {
					report(fmt.Errorf("next went back from %d to %d", after, p.Next))
					return
				}
				after = p.Next
			}
		}()
	}
	bg.Add(2)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			n, err := s.Prune(ctx, time.Now())
			if err != nil {
				report(err)
				return
			}
			pruned.Add(n)
		}
	}()
	go func() {
		defer bg.Done()
		var n int64
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			n++
			if err := s.SetPosition(ctx, "dsh", ChannelEvents, n); err != nil {
				report(err)
				return
			}
		}
	}()

	done.Wait()
	close(stop)
	bg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
	if pruned.Load() == 0 {
		t.Error("prune never deleted a row")
	}
	if head, err := s.Head(ctx); err != nil || head != total {
		t.Fatalf("Head = %d, %v", head, err)
	}
	if seq := mustAppend(t, s, entry("dsh", message.KindMessage, total+1, time.Now())); seq <= total {
		t.Fatalf("fresh append got seq %d", seq)
	}
}

func TestAttachmentEmptyConsumerMeansUnclaimed(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	mustAppend(t, s, attachmentEntry("", message.KindMessage, 1, time.Now(), "free"))
	mustAppend(t, s, attachmentEntry("dsh", message.KindMessage, 2, time.Now(), "owned"))
	if _, ok, err := s.Attachment(ctx, "", "free"); err != nil || !ok {
		t.Fatalf("unclaimed lookup = %v, %v", ok, err)
	}
	if _, ok, _ := s.Attachment(ctx, "", "owned"); ok {
		t.Fatal("empty consumer can see a claimed attachment")
	}
}

func TestAppendDuplicateAttachmentIDsStoreFirst(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	e := entry("dsh", message.KindMessage, 1, time.Now())
	e.Parsed.Attachments = []message.AttachmentRef{
		{ID: "x", ContentType: "image/png", Filename: "first.png", Size: 1},
		{ID: "x", ContentType: "image/jpeg", Filename: "second.jpg", Size: 2},
	}
	mustAppend(t, s, e)
	meta, ok, err := s.Attachment(ctx, "dsh", "x")
	if err != nil || !ok || meta.Filename != "first.png" {
		t.Fatalf("meta = %+v, %v, %v", meta, ok, err)
	}
}

func TestSetPositionRejectsNegative(t *testing.T) {
	s := openStore(t)
	if err := s.SetPosition(context.Background(), "dsh", ChannelEvents, -1); err == nil {
		t.Fatal("negative position accepted")
	}
}

func TestReadLimitLoadsRawOnlyForReturned(t *testing.T) {
	s := openStore(t)
	now := time.Now()
	for i := int64(1); i <= 4; i++ {
		mustAppend(t, s, entry("dsh", message.KindMessage, i, now))
	}
	p, err := s.Read(context.Background(), Query{Consumer: "dsh", Limit: 2})
	if err != nil || len(p.Records) != 2 {
		t.Fatalf("page = %+v, %v", p, err)
	}
	for _, r := range p.Records {
		if string(r.Raw) != `{"envelope":{}}` {
			t.Fatalf("raw = %q", r.Raw)
		}
	}
}
