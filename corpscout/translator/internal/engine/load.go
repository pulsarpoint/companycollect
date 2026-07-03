package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// ClickHouseSource is the ClickHouse surface the engine needs; *ClickHouse
// satisfies it, tests use fakes.
type ClickHouseSource interface {
	QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error)
	QueryStaticInput(ctx context.Context, query string) ([]StaticInput, error)
	InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error)
}

type Options struct {
	QueuePath string
}

type LoadResult struct {
	QueuePath      string
	Created        bool
	RowsSeen       int
	RowsInserted   int
	StaticRowsSeen int
	StaticFlushed  int
}

// LoadInput scans ClickHouse for untranslated texts declared by the
// definition, queues LLM-column rows into the DuckDB queue, and flushes
// static-column translations directly to ClickHouse.
func LoadInput(ctx context.Context, source ClickHouseSource, def Definition, options Options) (LoadResult, error) {
	if source == nil {
		return LoadResult{}, errors.New("clickhouse source is required")
	}
	if err := def.Validate(); err != nil {
		return LoadResult{}, err
	}
	if options.QueuePath == "" {
		return LoadResult{}, errors.New("queue path is required")
	}

	created := false
	if _, err := os.Stat(options.QueuePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return LoadResult{}, fmt.Errorf("stat queue %q: %w", options.QueuePath, err)
		}
		created = true
	}

	if err := os.MkdirAll(filepath.Dir(options.QueuePath), 0o755); err != nil {
		return LoadResult{}, fmt.Errorf("create queue directory: %w", err)
	}

	db, err := sql.Open("duckdb", options.QueuePath)
	if err != nil {
		return LoadResult{}, fmt.Errorf("open queue duckdb: %w", err)
	}
	defer db.Close()

	return loadInputWithDB(ctx, source, def, db, options.QueuePath, created)
}

func loadInputWithDB(
	ctx context.Context,
	source ClickHouseSource,
	def Definition,
	db *sql.DB,
	queuePath string,
	created bool,
) (LoadResult, error) {
	if err := createQueueTables(ctx, db); err != nil {
		return LoadResult{}, err
	}

	before, err := countRows(ctx, db, "input_items")
	if err != nil {
		return LoadResult{}, err
	}

	rowsSeen := 0
	staticRowsSeen := 0
	staticFlushed := 0
	version := time.Now().Unix()

	for _, col := range def.Columns {
		query, err := ScanSQL(def, col)
		if err != nil {
			return LoadResult{}, err
		}

		if col.Static != nil {
			seen, flushed, err := flushStaticColumn(ctx, source, def, col, query, version)
			if err != nil {
				return LoadResult{}, err
			}
			staticRowsSeen += seen
			staticFlushed += flushed
			continue
		}

		rows, err := source.QueryTranslationInput(ctx, query)
		if err != nil {
			return LoadResult{}, fmt.Errorf("query translation input for %s.%s: %w", col.Table, col.Column, err)
		}
		rowsSeen += len(rows)
		if err := upsertInputItems(ctx, db, rows); err != nil {
			return LoadResult{}, err
		}
	}

	after, err := countRows(ctx, db, "input_items")
	if err != nil {
		return LoadResult{}, err
	}

	return LoadResult{
		QueuePath:      queuePath,
		Created:        created,
		RowsSeen:       rowsSeen,
		RowsInserted:   after - before,
		StaticRowsSeen: staticRowsSeen,
		StaticFlushed:  staticFlushed,
	}, nil
}

// flushStaticColumn translates one static column from its definition map and
// writes the results straight to ClickHouse; keys missing from the map are
// skipped, matching the previous legal-form behavior.
func flushStaticColumn(
	ctx context.Context,
	source ClickHouseSource,
	def Definition,
	col ColumnSpec,
	query string,
	version int64,
) (int, int, error) {
	rows, err := source.QueryStaticInput(ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("query static translations for %s.%s: %w", col.Table, col.Column, err)
	}

	translations := make([]TextTranslation, 0, len(rows))
	for _, row := range rows {
		translatedText := col.Static.Values[row.Key]
		if row.SourceText == "" || translatedText == "" {
			continue
		}

		translations = append(translations, TextTranslation{
			SourceTable:    col.Table,
			SourceColumn:   col.Column,
			SourceText:     row.SourceText,
			SourceTextHash: row.SourceTextHash,
			SourceLang:     def.SourceLang,
			TargetLang:     def.TargetLang,
			TranslatedText: translatedText,
			Provider:       "static",
			Model:          "static",
			Version:        version,
		})
	}

	if len(translations) == 0 {
		return len(rows), 0, nil
	}

	flushed, err := source.InsertTextTranslations(ctx, translations)
	if err != nil {
		return len(rows), 0, fmt.Errorf("insert static translations for %s.%s: %w", col.Table, col.Column, err)
	}
	return len(rows), flushed, nil
}

func createQueueTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		create table if not exists input_items (
			source_table text not null,
			source_column text not null,
			source_text text not null,
			source_text_hash ubigint not null,
			source_lang text not null,
			target_lang text not null,
			created_at timestamp not null,
			primary key (
				source_table,
				source_column,
				source_text_hash,
				source_lang,
				target_lang
			)
		)
	`); err != nil {
		return fmt.Errorf("create input_items: %w", err)
	}

	if _, err := db.ExecContext(ctx, `
		create table if not exists output_items (
			source_table text not null,
			source_column text not null,
			source_text text not null,
			source_text_hash ubigint not null,
			source_lang text not null,
			target_lang text not null,
			translated_text text not null,
			provider text not null,
			model text not null,
			completed_at timestamp not null,
			primary key (
				source_table,
				source_column,
				source_text_hash,
				source_lang,
				target_lang
			)
		)
	`); err != nil {
		return fmt.Errorf("create output_items: %w", err)
	}

	if _, err := db.ExecContext(ctx, `
		create table if not exists failed_items (
			source_table text not null,
			source_column text not null,
			source_text text not null,
			source_text_hash ubigint not null,
			source_lang text not null,
			target_lang text not null,
			error_message text not null,
			failed_at timestamp not null,
			primary key (
				source_table,
				source_column,
				source_text_hash,
				source_lang,
				target_lang
			)
		)
	`); err != nil {
		return fmt.Errorf("create failed_items: %w", err)
	}

	return nil
}

func upsertInputItems(ctx context.Context, db *sql.DB, rows []InputItem) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin input upsert: %w", err)
	}
	defer rollback(tx)

	stmt, err := tx.PrepareContext(ctx, `
		insert into input_items (
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			created_at
		)
		values (?, ?, ?, cast(? as ubigint), ?, ?, current_timestamp)
		on conflict (
			source_table,
			source_column,
			source_text_hash,
			source_lang,
			target_lang
		) do nothing
	`)
	if err != nil {
		return fmt.Errorf("prepare input upsert: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		if err := validateInput(row); err != nil {
			return err
		}
		if _, err := stmt.ExecContext(
			ctx,
			row.SourceTable,
			row.SourceColumn,
			row.SourceText,
			strconv.FormatUint(row.SourceTextHash, 10),
			row.SourceLang,
			row.TargetLang,
		); err != nil {
			return fmt.Errorf("upsert input item: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit input upsert: %w", err)
	}
	return nil
}

func validateInput(row InputItem) error {
	switch {
	case row.SourceTable == "":
		return errors.New("source_table is required")
	case row.SourceColumn == "":
		return errors.New("source_column is required")
	case row.SourceText == "":
		return errors.New("source_text is required")
	case row.SourceLang == "":
		return errors.New("source_lang is required")
	case row.TargetLang == "":
		return errors.New("target_lang is required")
	default:
		return nil
	}
}

func countRows(ctx context.Context, db *sql.DB, table string) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s: %w", table, err)
	}
	return count, nil
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
