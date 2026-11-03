// Package store persists Vigil's state in SQLite through a pure-Go driver,
// so the binary cross-compiles without cgo.
//
// SQLite allows one writer at a time. Rather than letting writers race and
// retry on SQLITE_BUSY, the store keeps a dedicated single-connection pool
// for writes (transactions begin IMMEDIATE) and a separate pool for reads,
// which WAL mode lets run concurrently with the writer.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store is the database handle.
type Store struct {
	w *sql.DB
	r *sql.DB
}

// Open opens (creating if needed) the database at path and applies pending
// migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	pragmas := url.Values{}
	for _, p := range []string{"journal_mode(WAL)", "busy_timeout(5000)", "foreign_keys(1)", "synchronous(NORMAL)"} {
		pragmas.Add("_pragma", p)
	}
	writeDSN := "file:" + path + "?" + pragmas.Encode() + "&_txlock=immediate"
	readDSN := "file:" + path + "?" + pragmas.Encode()

	w, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	r, err := sql.Open("sqlite", readDSN)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(max(4, runtime.NumCPU()))

	s := &Store{w: w, r: r}
	if err := s.migrate(ctx); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("migrating %s: %w", path, err)
	}
	return s, nil
}

// Close closes both pools.
func (s *Store) Close() error {
	return errors.Join(s.r.Close(), s.w.Close())
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.w.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return err
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		name := strings.TrimPrefix(f, "migrations/")
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("migration %s: name must start with a number", name)
		}
		var applied int
		if err := s.w.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations WHERE version = ?`, version).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			continue
		}
		body, err := migrations.ReadFile(f)
		if err != nil {
			return err
		}
		err = s.Tx(ctx, func(tx *Tx) error {
			if _, err := tx.ExecContext(ctx, string(body)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, version, ms(time.Now()))
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Tx is a write transaction.
type Tx struct{ *sql.Tx }

// Tx runs fn in a write transaction, committing if it returns nil.
func (s *Store) Tx(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&Tx{tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Meta reads a value from the key-value table; ok is false when unset.
func (s *Store) Meta(ctx context.Context, key string) (value string, ok bool, err error) {
	err = s.r.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

// SetMeta writes a value to the key-value table.
func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.w.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// Ping checks that the database answers, for readiness probes.
func (s *Store) Ping(ctx context.Context) error { return s.r.PingContext(ctx) }

func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromMS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

func nullMS(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return fromMS(v.Int64)
}
