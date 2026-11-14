package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

const sqliteConstraintUnique = 2067

const sqlitePragmas = "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"

var dsnEscaper = strings.NewReplacer("%", "%25", "?", "%3F", "#", "%23")

func sqliteDSN(path string, readOnly bool) string {
	dsn := "file:" + dsnEscaper.Replace(path) + "?" + sqlitePragmas
	if readOnly {
		dsn += "&_pragma=query_only(1)"
	}
	return dsn
}

func openSQLite(path string, readOnly bool, maxConns int) (*bun.DB, error) {
	sqldb, err := sql.Open("sqlite", sqliteDSN(path, readOnly))
	if err != nil {
		return nil, err
	}
	sqldb.SetMaxOpenConns(maxConns)
	return bun.NewDB(sqldb, sqlitedialect.New()), nil
}

func openDatabases(path string) (writer, reader *bun.DB, err error) {
	writer, err = openSQLite(path, false, 1)
	if err != nil {
		return nil, nil, err
	}
	reader, err = openSQLite(path, true, 4)
	if err != nil {
		_ = writer.Close()
		return nil, nil, err
	}
	return writer, reader, nil
}

func isUniqueViolation(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) && coded.Code() == sqliteConstraintUnique
}

func headSeq(ctx context.Context, db *bun.DB) (int64, error) {
	var seq int64
	err := db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = 'messages'`).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func probeWritable(ctx context.Context, db *bun.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}
	return nil
}
