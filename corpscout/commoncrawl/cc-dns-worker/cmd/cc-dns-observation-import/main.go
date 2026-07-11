// Command cc-dns-observation-import performs the one-time, resumable import of the 2026-07-07
// scanner's SQLite record outbox into the retry-safe ClickHouse observation table.
package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cockroachdb/errors"
	_ "modernc.org/sqlite"
)

const observationInsert = `INSERT INTO corpscout.commoncrawl_domain_dns_record_observations
    (root_domain, name, record_type, slot, value, source, discovery, scan_id,
     ttl, priority, rcode, observed_at, loaded_at)`

var importVersionBase = time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)

type config struct {
	databasePath    string
	checkpointPath  string
	expectedScanID  string
	batchSize       int
	maxRows         int64
	dryRun          bool
	checkClickHouse bool
}

type checkpoint struct {
	DatabasePath string `json:"database_path"`
	ScanID       string `json:"scan_id"`
	LastRowID    int64  `json:"last_row_id"`
	RowsWritten  int64  `json:"rows_written"`
}

type sqliteRecord struct {
	rowID      int64
	scanID     string
	rootDomain string
	name       string
	recordType string
	slot       string
	value      string
	ttl        uint32
	priority   uint16
	rcode      string
	observedAt time.Time
	loadedAt   time.Time
}

func main() {
	configuration := parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := importObservations(ctx, configuration); err != nil {
		slog.Error("observation import failed", "error", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	databasePath := flag.String("db", "", "source SQLite database path")
	checkpointPath := flag.String("checkpoint", "", "sidecar checkpoint path")
	expectedScanID := flag.String("scan-id", "2026-07-07", "required SQLite scan_id")
	batchSize := flag.Int("batch-size", 20000, "rows per ClickHouse insert")
	maxRows := flag.Int64("max-rows", 0, "stop after this many rows in this invocation (0 = all)")
	dryRun := flag.Bool("dry-run", false, "parse and validate one batch without writing")
	checkClickHouse := flag.Bool("check-clickhouse", false, "ping ClickHouse without reading or writing records")
	flag.Parse()
	if *databasePath == "" {
		fmt.Fprintln(os.Stderr, "--db is required")
		os.Exit(2)
	}
	if *checkpointPath == "" {
		*checkpointPath = *databasePath + ".observations-import.json"
	}
	if *batchSize <= 0 {
		fmt.Fprintln(os.Stderr, "--batch-size must be positive")
		os.Exit(2)
	}
	return config{
		databasePath: *databasePath, checkpointPath: *checkpointPath,
		expectedScanID: *expectedScanID, batchSize: *batchSize,
		maxRows: *maxRows, dryRun: *dryRun,
		checkClickHouse: *checkClickHouse,
	}
}

func importObservations(ctx context.Context, configuration config) error {
	if configuration.checkClickHouse {
		destination, err := openClickHouse(ctx)
		if err != nil {
			return err
		}
		defer destination.Close()
		slog.Info("ClickHouse connectivity check passed")
		return nil
	}
	source, err := openSQLiteReadOnly(configuration.databasePath)
	if err != nil {
		return err
	}
	defer source.Close()

	progress, err := readCheckpoint(configuration)
	if err != nil {
		return err
	}
	if configuration.dryRun {
		progress = checkpoint{DatabasePath: configuration.databasePath, ScanID: configuration.expectedScanID}
	}

	var destination driver.Conn
	if !configuration.dryRun {
		destination, err = openClickHouse(ctx)
		if err != nil {
			return err
		}
		defer destination.Close()
	}

	startedAt := time.Now()
	rowsThisRun := int64(0)
	for {
		limit := configuration.batchSize
		if configuration.maxRows > 0 {
			remaining := configuration.maxRows - rowsThisRun
			if remaining <= 0 {
				break
			}
			limit = min(limit, int(remaining))
		}
		records, err := readRecordBatch(ctx, source, progress.LastRowID, limit, configuration.expectedScanID)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			break
		}
		if configuration.dryRun {
			slog.Info("dry-run batch validated", "rows", len(records),
				"first_rowid", records[0].rowID, "last_rowid", records[len(records)-1].rowID,
				"first_observed_at", records[0].observedAt,
			)
			return nil
		}
		if err := insertRecordBatch(ctx, destination, records); err != nil {
			return err
		}
		progress.LastRowID = records[len(records)-1].rowID
		progress.RowsWritten += int64(len(records))
		rowsThisRun += int64(len(records))
		if err := writeCheckpoint(configuration.checkpointPath, progress); err != nil {
			return err
		}
		if progress.RowsWritten%1_000_000 < int64(len(records)) {
			slog.Info("observation import progress", "last_rowid", progress.LastRowID,
				"rows_written", progress.RowsWritten, "elapsed", time.Since(startedAt).Round(time.Second),
			)
		}
	}
	slog.Info("observation import invocation complete", "last_rowid", progress.LastRowID,
		"rows_written", progress.RowsWritten, "rows_this_run", rowsThisRun,
		"elapsed", time.Since(startedAt).Round(time.Second),
	)
	return nil
}

func openSQLiteReadOnly(path string) (*sql.DB, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.Wrap(err, "resolve SQLite path")
	}
	if _, err := os.Stat(absolutePath); err != nil {
		return nil, errors.Wrap(err, "stat SQLite source")
	}
	dsn := (&url.URL{Scheme: "file", Path: absolutePath}).String() + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "open SQLite source")
	}
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		_ = database.Close()
		return nil, errors.Wrap(err, "ping SQLite source")
	}
	return database, nil
}

func openClickHouse(ctx context.Context) (driver.Conn, error) {
	host := os.Getenv("CLICKHOUSE_HOST")
	port := os.Getenv("CLICKHOUSE_NATIVE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("CLICKHOUSE_HOST and CLICKHOUSE_NATIVE_PORT are required")
	}
	options := &clickhouse.Options{
		Addr: []string{net.JoinHostPort(host, port)},
		Auth: clickhouse.Auth{
			Database: os.Getenv("CLICKHOUSE_DATABASE"), Username: os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout: 10 * time.Second, ReadTimeout: 10 * time.Minute,
		MaxOpenConns: 1, MaxIdleConns: 1,
	}
	secure, err := strconv.ParseBool(os.Getenv("CLICKHOUSE_SECURE"))
	if err == nil && secure {
		options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	connection, err := clickhouse.Open(options)
	if err != nil {
		return nil, errors.Wrap(err, "open ClickHouse")
	}
	if err := connection.Ping(ctx); err != nil {
		_ = connection.Close()
		return nil, errors.Wrap(err, "ping ClickHouse")
	}
	return connection, nil
}

func readCheckpoint(configuration config) (checkpoint, error) {
	progress := checkpoint{DatabasePath: configuration.databasePath, ScanID: configuration.expectedScanID}
	data, err := os.ReadFile(configuration.checkpointPath)
	if os.IsNotExist(err) {
		return progress, nil
	}
	if err != nil {
		return checkpoint{}, errors.Wrap(err, "read import checkpoint")
	}
	if err := json.Unmarshal(data, &progress); err != nil {
		return checkpoint{}, errors.Wrap(err, "decode import checkpoint")
	}
	if progress.DatabasePath != configuration.databasePath || progress.ScanID != configuration.expectedScanID {
		return checkpoint{}, errors.New("checkpoint belongs to a different database or scan")
	}
	return progress, nil
}

func writeCheckpoint(path string, progress checkpoint) error {
	data, err := json.Marshal(progress)
	if err != nil {
		return errors.Wrap(err, "encode import checkpoint")
	}
	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, data, 0o600); err != nil {
		return errors.Wrap(err, "write import checkpoint")
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return errors.Wrap(err, "replace import checkpoint")
	}
	return nil
}

func readRecordBatch(ctx context.Context, database *sql.DB, afterRowID int64, limit int, expectedScanID string) ([]sqliteRecord, error) {
	rows, err := database.QueryContext(ctx, `SELECT rowid, scan_id, root_domain, name, record_type,
		COALESCE(slot, ''), value, COALESCE(ttl, 0), COALESCE(priority, 0), COALESCE(rcode, ''),
		COALESCE(source_run_id, ''), resolved_at
		FROM scan_records WHERE rowid > ? ORDER BY rowid LIMIT ?`, afterRowID, limit)
	if err != nil {
		return nil, errors.Wrap(err, "query SQLite record batch")
	}
	defer rows.Close()

	records := make([]sqliteRecord, 0, limit)
	for rows.Next() {
		var record sqliteRecord
		var ttl, priority int64
		var sourceRunID, resolvedAt string
		if err := rows.Scan(&record.rowID, &record.scanID, &record.rootDomain, &record.name,
			&record.recordType, &record.slot, &record.value, &ttl, &priority, &record.rcode,
			&sourceRunID, &resolvedAt); err != nil {
			return nil, errors.Wrap(err, "scan SQLite record")
		}
		if record.scanID != expectedScanID || sourceRunID != expectedScanID {
			return nil, errors.Newf("rowid %d has scan_id=%q source_run_id=%q", record.rowID, record.scanID, sourceRunID)
		}
		if ttl < 0 || ttl > int64(^uint32(0)) || priority < 0 || priority > int64(^uint16(0)) {
			return nil, errors.Newf("rowid %d has out-of-range ttl or priority", record.rowID)
		}
		record.ttl, record.priority = uint32(ttl), uint16(priority)
		record.observedAt, err = time.Parse(time.RFC3339Nano, resolvedAt)
		if err != nil {
			return nil, errors.Wrapf(err, "parse resolved_at for rowid %d", record.rowID)
		}
		record.loadedAt = importVersionBase.Add(time.Duration(record.rowID) * time.Millisecond)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "read SQLite record batch")
	}
	return records, nil
}

func insertRecordBatch(ctx context.Context, connection driver.Conn, records []sqliteRecord) error {
	batch, err := connection.PrepareBatch(ctx, observationInsert)
	if err != nil {
		return errors.Wrap(err, "prepare ClickHouse observation batch")
	}
	for _, record := range records {
		if err := batch.Append(
			record.rootDomain, record.name, record.recordType, record.slot, record.value,
			"query", "static", record.scanID, record.ttl, record.priority, record.rcode,
			record.observedAt, record.loadedAt,
		); err != nil {
			_ = batch.Abort()
			return errors.Wrapf(err, "append observation rowid %d", record.rowID)
		}
	}
	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "send ClickHouse observation batch")
	}
	return nil
}
