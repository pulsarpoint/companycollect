package planstore

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"cc-download-worker/rangeplanner"
	"github.com/cockroachdb/errors"
	_ "modernc.org/sqlite"
)

const (
	committedState      = 1
	pageInsertBatchSize = 500
	schemaVersion       = 1
)

type pageInsert struct {
	part            int
	worklistOrdinal int64
	chunk           int
	warcID          int64
	warcOffset      int64
	warcLength      int64
}

type Store struct {
	database *sql.DB
	path     string
}

type Stats struct {
	Parts                     int
	Chunks                    int64
	CommittedChunks           int64
	Pages                     int64
	PendingPages              int64
	PendingBytes              int64
	WARCObjects               int
	WholeWARCObjects          int
	ExactWARCObjects          int
	WholeWARCPages            int64
	ExactPages                int64
	WholeWARCSelectedBytes    int64
	ExactSelectedBytes        int64
	WholeWARCDownloadBytes    int64
	EstimatedRequests         int64
	EstimatedSourceBytes      int64
	WholeWARCThresholdPercent float64
}

type UtilizationBucket struct {
	FromPercent   int
	ToPercent     int
	WARCObjects   int64
	Pages         int64
	SelectedBytes int64
	ObjectBytes   int64
	JunkBytes     int64
}

type WARCStats struct {
	Filename        string
	ObjectBytes     int64
	PendingPages    int64
	PendingBytes    int64
	SelectedPercent float64
	Strategy        string
	CacheState      string
	LocalPath       string
}

type CommittedChunk struct {
	Part        int
	Chunk       int
	ManifestKey string
}

type PendingPage struct {
	WorklistOrdinal int64
	WARCFile        string
	WARCOffset      int64
	WARCLength      int64
	Strategy        string
	CacheState      string
	LocalPath       string
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("plan database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, errors.Wrap(err, "create plan database directory")
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "open plan database")
	}
	database.SetMaxOpenConns(1)
	store := &Store{database: database, path: path}
	if err := store.initialize(context.Background()); err != nil {
		_ = database.Close()
		return nil, err
	}
	return store, nil
}

func OpenReadOnly(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("plan database path is required")
	}
	databaseURL := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	database, err := sql.Open("sqlite", databaseURL.String())
	if err != nil {
		return nil, errors.Wrap(err, "open read-only plan database")
	}
	database.SetMaxOpenConns(1)
	store := &Store{database: database, path: path}
	if err := store.validateSchemaVersion(context.Background()); err != nil {
		_ = database.Close()
		return nil, err
	}
	return store, nil
}

func (store *Store) Close() error {
	return store.database.Close()
}

func (store *Store) Path() string {
	return store.path
}

func (store *Store) Parts(ctx context.Context) ([]int, error) {
	rows, err := store.database.QueryContext(ctx, `SELECT part FROM parts ORDER BY part`)
	if err != nil {
		return nil, errors.Wrap(err, "query plan parts")
	}
	defer rows.Close()
	var parts []int
	for rows.Next() {
		var part int
		if err := rows.Scan(&part); err != nil {
			return nil, errors.Wrap(err, "read plan part")
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate plan parts")
	}
	return parts, nil
}

func (store *Store) initialize(ctx context.Context) error {
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`PRAGMA foreign_keys=ON`,
		`PRAGMA busy_timeout=30000`,
		`CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS parts (
			part INTEGER PRIMARY KEY,
			worklist_checksum TEXT NOT NULL,
			record_count INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS warcs (
			warc_id INTEGER PRIMARY KEY,
			filename TEXT NOT NULL UNIQUE,
			object_bytes INTEGER NOT NULL DEFAULT 0,
			pending_pages INTEGER NOT NULL DEFAULT 0,
			pending_bytes INTEGER NOT NULL DEFAULT 0,
			strategy TEXT NOT NULL DEFAULT 'exact_ranges' CHECK (strategy IN ('exact_ranges', 'whole_warc')),
			cache_state TEXT NOT NULL DEFAULT 'missing' CHECK (cache_state IN ('missing', 'downloading', 'ready')),
			local_path TEXT NOT NULL DEFAULT ''
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS chunks (
			part INTEGER NOT NULL,
			chunk INTEGER NOT NULL,
			first_ordinal INTEGER NOT NULL,
			record_count INTEGER NOT NULL,
			state INTEGER NOT NULL DEFAULT 0 CHECK (state IN (0, 1)),
			manifest_key TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (part, chunk),
			FOREIGN KEY (part) REFERENCES parts(part) ON DELETE CASCADE
		) STRICT`,
		`CREATE TABLE IF NOT EXISTS pages (
			part INTEGER NOT NULL,
			worklist_ordinal INTEGER NOT NULL,
			chunk INTEGER NOT NULL,
			warc_id INTEGER NOT NULL,
			warc_offset INTEGER NOT NULL,
			warc_length INTEGER NOT NULL,
			state INTEGER NOT NULL DEFAULT 0 CHECK (state IN (0, 1)),
			PRIMARY KEY (part, worklist_ordinal),
			FOREIGN KEY (part, chunk) REFERENCES chunks(part, chunk) ON DELETE CASCADE,
			FOREIGN KEY (warc_id) REFERENCES warcs(warc_id)
		) STRICT`,
		`CREATE INDEX IF NOT EXISTS pages_warc_state ON pages(warc_id, state)`,
		`CREATE INDEX IF NOT EXISTS pages_part_chunk_state ON pages(part, chunk, state)`,
		`CREATE VIEW IF NOT EXISTS page_plan AS
		 SELECT pages.part, pages.chunk, pages.worklist_ordinal,
		        warcs.filename AS warc_filename, pages.warc_offset, pages.warc_length,
		        CASE pages.state WHEN 1 THEN 'committed' ELSE 'pending' END AS state,
		        warcs.strategy, warcs.cache_state, warcs.local_path
		 FROM pages
		 JOIN warcs ON warcs.warc_id = pages.warc_id`,
		`CREATE VIEW IF NOT EXISTS warc_plan AS
		 SELECT filename AS warc_filename, object_bytes, pending_pages, pending_bytes,
		        CASE WHEN object_bytes > 0 THEN 100.0 * pending_bytes / object_bytes ELSE 0 END AS selected_percent,
		        strategy, cache_state, local_path
		 FROM warcs`,
	} {
		if _, err := store.database.ExecContext(ctx, statement); err != nil {
			return errors.Wrap(err, "initialize plan database")
		}
	}
	if _, err := store.database.ExecContext(ctx,
		`INSERT INTO metadata(key, value) VALUES ('schema_version', ?)
		 ON CONFLICT(key) DO NOTHING`,
		fmt.Sprint(schemaVersion),
	); err != nil {
		return errors.Wrap(err, "record plan schema version")
	}
	return store.validateSchemaVersion(ctx)
}

func (store *Store) validateSchemaVersion(ctx context.Context) error {
	var version int
	if err := store.database.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'schema_version'`).Scan(&version); err != nil {
		return errors.Wrap(err, "read plan schema version")
	}
	if version != schemaVersion {
		return errors.Newf("unsupported plan schema version %d; want %d", version, schemaVersion)
	}
	return nil
}

func (store *Store) SyncParts(ctx context.Context, parts []int) error {
	desired := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		desired[part] = struct{}{}
	}
	rows, err := store.database.QueryContext(ctx, `SELECT part FROM parts`)
	if err != nil {
		return errors.Wrap(err, "list planned parts")
	}
	var obsolete []int
	for rows.Next() {
		var part int
		if err := rows.Scan(&part); err != nil {
			_ = rows.Close()
			return errors.Wrap(err, "read planned part")
		}
		if _, exists := desired[part]; !exists {
			obsolete = append(obsolete, part)
		}
	}
	if err := rows.Close(); err != nil {
		return errors.Wrap(err, "close planned parts")
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterate planned parts")
	}
	for _, part := range obsolete {
		if _, err := store.database.ExecContext(ctx, `DELETE FROM parts WHERE part = ?`, part); err != nil {
			return errors.Wrapf(err, "remove obsolete part %d", part)
		}
	}
	return nil
}

func (store *Store) ImportPart(ctx context.Context, part int, checksum string, worklist rangeplanner.Worklist) (bool, error) {
	if part < 0 || checksum == "" || len(worklist.Records) == 0 || len(worklist.OutputChunks) == 0 {
		return false, errors.New("part, checksum, records, and chunks are required")
	}
	var currentChecksum string
	err := store.database.QueryRowContext(ctx, `SELECT worklist_checksum FROM parts WHERE part = ?`, part).Scan(&currentChecksum)
	if err == nil && currentChecksum == checksum {
		return true, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, errors.Wrap(err, "read imported part")
	}

	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.Wrap(err, "begin part import")
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `DELETE FROM parts WHERE part = ?`, part); err != nil {
		return false, errors.Wrap(err, "replace imported part")
	}
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO parts(part, worklist_checksum, record_count, chunk_count) VALUES (?, ?, ?, ?)`,
		part, checksum, len(worklist.Records), len(worklist.OutputChunks),
	); err != nil {
		return false, errors.Wrap(err, "insert imported part")
	}

	warcIDs, err := loadWARCIDs(ctx, transaction)
	if err != nil {
		return false, err
	}
	insertWARC, err := transaction.PrepareContext(ctx, `INSERT INTO warcs(filename) VALUES (?)`)
	if err != nil {
		return false, errors.Wrap(err, "prepare WARC insert")
	}
	defer insertWARC.Close()
	insertChunk, err := transaction.PrepareContext(ctx,
		`INSERT INTO chunks(part, chunk, first_ordinal, record_count) VALUES (?, ?, ?, ?)`,
	)
	if err != nil {
		return false, errors.Wrap(err, "prepare chunk insert")
	}
	defer insertChunk.Close()
	pageBatch := make([]pageInsert, 0, pageInsertBatchSize)
	for chunkNumber, records := range worklist.OutputChunks {
		if _, err := insertChunk.ExecContext(ctx, part, chunkNumber, records[0].ID, len(records)); err != nil {
			return false, errors.Wrapf(err, "insert part %d chunk %d", part, chunkNumber)
		}
		for _, record := range records {
			warcID, exists := warcIDs[record.WARCFile]
			if !exists {
				result, err := insertWARC.ExecContext(ctx, record.WARCFile)
				if err != nil {
					return false, errors.Wrap(err, "insert WARC")
				}
				warcID, err = result.LastInsertId()
				if err != nil {
					return false, errors.Wrap(err, "read inserted WARC ID")
				}
				warcIDs[record.WARCFile] = warcID
			}
			pageBatch = append(pageBatch, pageInsert{
				part:            part,
				worklistOrdinal: record.ID,
				chunk:           chunkNumber,
				warcID:          warcID,
				warcOffset:      record.Offset,
				warcLength:      record.Length,
			})
			if len(pageBatch) == pageInsertBatchSize {
				if err := insertPageBatch(ctx, transaction, pageBatch); err != nil {
					return false, err
				}
				pageBatch = pageBatch[:0]
			}
		}
	}
	if err := insertPageBatch(ctx, transaction, pageBatch); err != nil {
		return false, err
	}
	if err := transaction.Commit(); err != nil {
		return false, errors.Wrap(err, "commit part import")
	}
	return false, nil
}

func insertPageBatch(ctx context.Context, transaction *sql.Tx, pages []pageInsert) error {
	if len(pages) == 0 {
		return nil
	}
	var query strings.Builder
	query.WriteString(`INSERT INTO pages(part, worklist_ordinal, chunk, warc_id, warc_offset, warc_length) VALUES `)
	arguments := make([]any, 0, len(pages)*6)
	for index, page := range pages {
		if index > 0 {
			query.WriteByte(',')
		}
		query.WriteString(`(?, ?, ?, ?, ?, ?)`)
		arguments = append(arguments,
			page.part,
			page.worklistOrdinal,
			page.chunk,
			page.warcID,
			page.warcOffset,
			page.warcLength,
		)
	}
	if _, err := transaction.ExecContext(ctx, query.String(), arguments...); err != nil {
		first := pages[0]
		last := pages[len(pages)-1]
		return errors.Wrapf(err, "insert part %d pages %d-%d", first.part, first.worklistOrdinal, last.worklistOrdinal)
	}
	return nil
}

func loadWARCIDs(ctx context.Context, transaction *sql.Tx) (map[string]int64, error) {
	rows, err := transaction.QueryContext(ctx, `SELECT warc_id, filename FROM warcs`)
	if err != nil {
		return nil, errors.Wrap(err, "list WARC IDs")
	}
	defer rows.Close()
	warcIDs := make(map[string]int64)
	for rows.Next() {
		var warcID int64
		var filename string
		if err := rows.Scan(&warcID, &filename); err != nil {
			return nil, errors.Wrap(err, "read WARC ID")
		}
		warcIDs[filename] = warcID
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate WARC IDs")
	}
	return warcIDs, nil
}

func (store *Store) SetObjectSizes(ctx context.Context, sizes map[string]int64) error {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin WARC size update")
	}
	defer transaction.Rollback()
	statement, err := transaction.PrepareContext(ctx, `UPDATE warcs SET object_bytes = ? WHERE filename = ?`)
	if err != nil {
		return errors.Wrap(err, "prepare WARC size update")
	}
	defer statement.Close()
	for filename, size := range sizes {
		if size <= 0 {
			continue
		}
		if _, err := statement.ExecContext(ctx, size, filename); err != nil {
			return errors.Wrapf(err, "update WARC size %s", filename)
		}
	}
	if err := transaction.Commit(); err != nil {
		return errors.Wrap(err, "commit WARC size update")
	}
	return nil
}

func (store *Store) RefreshStrategies(ctx context.Context, thresholdPercent float64) error {
	if math.IsNaN(thresholdPercent) || math.IsInf(thresholdPercent, 0) || thresholdPercent < 0 || thresholdPercent > 100 {
		return errors.Newf("whole-WARC threshold must be between 0 and 100, got %f", thresholdPercent)
	}
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin strategy refresh")
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `UPDATE warcs SET pending_pages = 0, pending_bytes = 0`); err != nil {
		return errors.Wrap(err, "reset WARC pending totals")
	}
	if _, err := transaction.ExecContext(ctx, `
		UPDATE warcs
		SET pending_pages = COALESCE((SELECT count(*) FROM pages WHERE pages.warc_id = warcs.warc_id AND pages.state = 0), 0),
		    pending_bytes = COALESCE((SELECT sum(warc_length) FROM pages WHERE pages.warc_id = warcs.warc_id AND pages.state = 0), 0)
	`); err != nil {
		return errors.Wrap(err, "calculate WARC pending totals")
	}
	if _, err := transaction.ExecContext(ctx, `DELETE FROM warcs WHERE pending_pages = 0 AND NOT EXISTS (SELECT 1 FROM pages WHERE pages.warc_id = warcs.warc_id)`); err != nil {
		return errors.Wrap(err, "remove unused WARCs")
	}
	if _, err := transaction.ExecContext(ctx, `
		UPDATE warcs
		SET strategy = CASE
			WHEN object_bytes > 0 AND 100.0 * pending_bytes / object_bytes >= ? THEN 'whole_warc'
			ELSE 'exact_ranges'
		END
	`, thresholdPercent); err != nil {
		return errors.Wrap(err, "choose WARC strategies")
	}
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO metadata(key, value) VALUES ('whole_warc_threshold_percent', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		fmt.Sprintf("%g", thresholdPercent),
	); err != nil {
		return errors.Wrap(err, "record WARC threshold")
	}
	if err := transaction.Commit(); err != nil {
		return errors.Wrap(err, "commit strategy refresh")
	}
	return nil
}

func (store *Store) MarkChunkCommitted(ctx context.Context, chunk CommittedChunk, thresholdPercent float64) error {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin chunk commit")
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx,
		`UPDATE chunks SET state = ?, manifest_key = ? WHERE part = ? AND chunk = ?`,
		committedState, chunk.ManifestKey, chunk.Part, chunk.Chunk,
	)
	if err != nil {
		return errors.Wrap(err, "mark chunk committed")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "read chunk update count")
	}
	if rows != 1 {
		return errors.Newf("chunk part=%d chunk=%d is not present in plan", chunk.Part, chunk.Chunk)
	}
	if _, err := transaction.ExecContext(ctx,
		`UPDATE pages SET state = ? WHERE part = ? AND chunk = ?`,
		committedState, chunk.Part, chunk.Chunk,
	); err != nil {
		return errors.Wrap(err, "mark chunk pages committed")
	}
	if err := transaction.Commit(); err != nil {
		return errors.Wrap(err, "commit chunk state")
	}
	return store.RefreshStrategies(ctx, thresholdPercent)
}

func (store *Store) ReconcileCommittedChunks(ctx context.Context, chunks []CommittedChunk, thresholdPercent float64) error {
	transaction, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin chunk reconciliation")
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `UPDATE chunks SET state = 0, manifest_key = ''`); err != nil {
		return errors.Wrap(err, "reset planned chunks")
	}
	if _, err := transaction.ExecContext(ctx, `UPDATE pages SET state = 0`); err != nil {
		return errors.Wrap(err, "reset planned pages")
	}
	for _, chunk := range chunks {
		if chunk.ManifestKey == "" {
			return errors.Newf("committed chunk part=%d chunk=%d is missing manifest key", chunk.Part, chunk.Chunk)
		}
		result, err := transaction.ExecContext(ctx,
			`UPDATE chunks SET state = 1, manifest_key = ? WHERE part = ? AND chunk = ?`,
			chunk.ManifestKey, chunk.Part, chunk.Chunk,
		)
		if err != nil {
			return errors.Wrap(err, "reconcile committed chunk")
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return errors.Wrap(err, "read reconciled chunk count")
		}
		if updated != 1 {
			return errors.Newf("committed chunk part=%d chunk=%d is not present in plan", chunk.Part, chunk.Chunk)
		}
		if _, err := transaction.ExecContext(ctx,
			`UPDATE pages SET state = 1 WHERE part = ? AND chunk = ?`,
			chunk.Part, chunk.Chunk,
		); err != nil {
			return errors.Wrap(err, "reconcile committed pages")
		}
	}
	if err := transaction.Commit(); err != nil {
		return errors.Wrap(err, "commit chunk reconciliation")
	}
	return store.RefreshStrategies(ctx, thresholdPercent)
}

func (store *Store) SetWARCCache(ctx context.Context, filename, state, localPath string) error {
	if filename == "" {
		return errors.New("WARC filename is required")
	}
	if state != "missing" && state != "downloading" && state != "ready" {
		return errors.Newf("invalid WARC cache state %q", state)
	}
	if state == "ready" && localPath == "" {
		return errors.New("ready WARC cache requires a local path")
	}
	if state == "missing" {
		localPath = ""
	}
	result, err := store.database.ExecContext(ctx,
		`UPDATE warcs SET cache_state = ?, local_path = ? WHERE filename = ?`,
		state, localPath, filename,
	)
	if err != nil {
		return errors.Wrap(err, "update WARC cache state")
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "read WARC cache update count")
	}
	if updated != 1 {
		return errors.Newf("WARC %s is not present in plan", filename)
	}
	return nil
}

func (store *Store) PendingPagesForChunk(ctx context.Context, part, chunk int) ([]PendingPage, error) {
	rows, err := store.database.QueryContext(ctx, `
		SELECT pages.worklist_ordinal, warcs.filename, pages.warc_offset, pages.warc_length,
		       warcs.strategy, warcs.cache_state, warcs.local_path
		FROM pages
		JOIN warcs ON warcs.warc_id = pages.warc_id
		WHERE pages.part = ? AND pages.chunk = ? AND pages.state = 0
		ORDER BY pages.worklist_ordinal
	`, part, chunk)
	if err != nil {
		return nil, errors.Wrap(err, "query pending chunk pages")
	}
	defer rows.Close()
	var pages []PendingPage
	for rows.Next() {
		var page PendingPage
		if err := rows.Scan(
			&page.WorklistOrdinal,
			&page.WARCFile,
			&page.WARCOffset,
			&page.WARCLength,
			&page.Strategy,
			&page.CacheState,
			&page.LocalPath,
		); err != nil {
			return nil, errors.Wrap(err, "read pending chunk page")
		}
		pages = append(pages, page)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate pending chunk pages")
	}
	return pages, nil
}

func (store *Store) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := store.database.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM parts),
			(SELECT count(*) FROM chunks),
			(SELECT count(*) FROM chunks WHERE state = 1),
			(SELECT count(*) FROM pages),
			COALESCE((SELECT sum(pending_pages) FROM warcs), 0),
			COALESCE((SELECT sum(pending_bytes) FROM warcs), 0),
			(SELECT count(*) FROM warcs),
			(SELECT count(*) FROM warcs WHERE strategy = 'whole_warc' AND pending_pages > 0),
			(SELECT count(*) FROM warcs WHERE strategy = 'exact_ranges' AND pending_pages > 0),
			COALESCE((SELECT sum(pending_pages) FROM warcs WHERE strategy = 'whole_warc'), 0),
			COALESCE((SELECT sum(pending_pages) FROM warcs WHERE strategy = 'exact_ranges'), 0),
			COALESCE((SELECT sum(pending_bytes) FROM warcs WHERE strategy = 'whole_warc'), 0),
			COALESCE((SELECT sum(pending_bytes) FROM warcs WHERE strategy = 'exact_ranges'), 0),
			COALESCE((SELECT sum(object_bytes) FROM warcs WHERE strategy = 'whole_warc' AND pending_pages > 0), 0),
			COALESCE((SELECT sum(CASE WHEN strategy = 'whole_warc' AND pending_pages > 0 THEN 1 ELSE pending_pages END) FROM warcs), 0),
			COALESCE((SELECT sum(CASE WHEN strategy = 'whole_warc' AND pending_pages > 0 THEN object_bytes ELSE pending_bytes END) FROM warcs), 0),
			COALESCE((SELECT CAST(value AS REAL) FROM metadata WHERE key = 'whole_warc_threshold_percent'), 0)
	`).Scan(
		&stats.Parts,
		&stats.Chunks,
		&stats.CommittedChunks,
		&stats.Pages,
		&stats.PendingPages,
		&stats.PendingBytes,
		&stats.WARCObjects,
		&stats.WholeWARCObjects,
		&stats.ExactWARCObjects,
		&stats.WholeWARCPages,
		&stats.ExactPages,
		&stats.WholeWARCSelectedBytes,
		&stats.ExactSelectedBytes,
		&stats.WholeWARCDownloadBytes,
		&stats.EstimatedRequests,
		&stats.EstimatedSourceBytes,
		&stats.WholeWARCThresholdPercent,
	)
	if err != nil {
		return Stats{}, errors.Wrap(err, "read plan statistics")
	}
	return stats, nil
}

func (store *Store) UtilizationBuckets(ctx context.Context) ([]UtilizationBucket, error) {
	rows, err := store.database.QueryContext(ctx, `
		WITH utilized AS (
			SELECT pending_pages, pending_bytes, object_bytes,
			       MIN(90, CAST((100.0 * pending_bytes / object_bytes) / 10 AS INTEGER) * 10) AS bucket
			FROM warcs
			WHERE strategy = 'whole_warc' AND pending_pages > 0 AND object_bytes > 0
		)
		SELECT bucket, bucket + 10, count(*), sum(pending_pages), sum(pending_bytes), sum(object_bytes), sum(object_bytes - pending_bytes)
		FROM utilized
		GROUP BY bucket
		ORDER BY bucket
	`)
	if err != nil {
		return nil, errors.Wrap(err, "query WARC utilization buckets")
	}
	defer rows.Close()
	var buckets []UtilizationBucket
	for rows.Next() {
		var bucket UtilizationBucket
		if err := rows.Scan(
			&bucket.FromPercent,
			&bucket.ToPercent,
			&bucket.WARCObjects,
			&bucket.Pages,
			&bucket.SelectedBytes,
			&bucket.ObjectBytes,
			&bucket.JunkBytes,
		); err != nil {
			return nil, errors.Wrap(err, "read WARC utilization bucket")
		}
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate WARC utilization buckets")
	}
	return buckets, nil
}

func (store *Store) WARCStats(ctx context.Context, limit int) ([]WARCStats, error) {
	if limit <= 0 {
		return nil, errors.New("WARC statistics limit must be positive")
	}
	rows, err := store.database.QueryContext(ctx, `
		SELECT filename, object_bytes, pending_pages, pending_bytes,
		       CASE WHEN object_bytes > 0 THEN 100.0 * pending_bytes / object_bytes ELSE 0 END,
		       strategy, cache_state, local_path
		FROM warcs
		WHERE pending_pages > 0
		ORDER BY CASE WHEN object_bytes > 0 THEN 1.0 * pending_bytes / object_bytes ELSE 0 END DESC, filename
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, errors.Wrap(err, "query WARC statistics")
	}
	defer rows.Close()
	stats := make([]WARCStats, 0, limit)
	for rows.Next() {
		var warc WARCStats
		if err := rows.Scan(
			&warc.Filename,
			&warc.ObjectBytes,
			&warc.PendingPages,
			&warc.PendingBytes,
			&warc.SelectedPercent,
			&warc.Strategy,
			&warc.CacheState,
			&warc.LocalPath,
		); err != nil {
			return nil, errors.Wrap(err, "read WARC statistics")
		}
		stats = append(stats, warc)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate WARC statistics")
	}
	return stats, nil
}
