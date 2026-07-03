package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/pulsarpoint/corpscout/translator/internal/queuedb"
)

type Queue struct {
	db          *sql.DB
	closeOnDone bool
}

type Item struct {
	ItemID             string
	SourceTable        string
	SourceColumn       string
	SourceText         string
	SourceTextHash     uint64
	SourceLang         string
	TargetLang         string
	SourceLanguageName string
	TargetLanguageName string
}

type TranslatedItem struct {
	Item
	TranslatedText string
	Provider       string
	Model          string
}

type FailedItem struct {
	Item
	ErrorMessage string
}

func Init(path string) (*Queue, error) {
	if path == "" {
		return nil, errors.New("queue duckdb path is required")
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("queue duckdb file does not exist: %s", path)
		}
		return nil, fmt.Errorf("stat queue duckdb file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("queue duckdb path is a directory: %s", path)
	}

	db, err := queuedb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open queue duckdb: %w", err)
	}
	q, err := New(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	q.closeOnDone = true
	return q, nil
}

func New(db *sql.DB) (*Queue, error) {
	if db == nil {
		return nil, errors.New("queue duckdb connection is required")
	}

	q := &Queue{db: db}
	if err := q.validateSchema(context.Background()); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) Close() error {
	if !q.closeOnDone {
		return nil
	}
	return q.db.Close()
}

// GetBatch returns up to limit pending items from exactly one language pair:
// the pair holding the oldest pending input_items row. Callers depend on
// batches being homogeneous, since a translation request carries a single
// source/target language pair for prompt framing.
func (q *Queue) GetBatch(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 {
		return nil, errors.New("batch size must be positive")
	}

	var srcLang, dstLang string
	err := q.db.QueryRowContext(ctx, `
		select source_lang, target_lang
		from pending_items
		order by created_at, source_lang, target_lang
		limit 1
	`).Scan(&srcLang, &dstLang)
	if errors.Is(err, sql.ErrNoRows) {
		return []Item{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pick queue language pair: %w", err)
	}

	rows, err := q.db.QueryContext(ctx, `
		select
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			source_language_name,
			target_language_name
		from pending_items
		where source_lang = ? and target_lang = ?
		order by created_at, source_table, source_column, source_text_hash
		limit ?
	`, srcLang, dstLang, limit)
	if err != nil {
		return nil, fmt.Errorf("query queue batch: %w", err)
	}
	defer rows.Close()

	items := make([]Item, 0, limit)
	for rows.Next() {
		var item Item
		var hashText string
		if err := rows.Scan(
			&item.SourceTable,
			&item.SourceColumn,
			&item.SourceText,
			&hashText,
			&item.SourceLang,
			&item.TargetLang,
			&item.SourceLanguageName,
			&item.TargetLanguageName,
		); err != nil {
			return nil, fmt.Errorf("scan queue item: %w", err)
		}

		sourceTextHash, err := strconv.ParseUint(hashText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse source_text_hash %q: %w", hashText, err)
		}
		item.SourceTextHash = sourceTextHash
		item.ItemID = itemID(item)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queue batch: %w", err)
	}
	return items, nil
}

func (q *Queue) SaveBatch(ctx context.Context, rows []TranslatedItem) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin output save: %w", err)
	}
	defer rollback(tx)

	stmt, err := tx.PrepareContext(ctx, `
		insert into output_items (
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			translated_text,
			provider,
			model,
			completed_at
		)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
		on conflict (
			source_table,
			source_column,
			source_text_hash,
			source_lang,
			target_lang
		) do update set
			source_text = excluded.source_text,
			translated_text = excluded.translated_text,
			provider = excluded.provider,
			model = excluded.model,
			completed_at = excluded.completed_at
	`)
	if err != nil {
		return fmt.Errorf("prepare output save: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		if err := validateTranslatedItem(row); err != nil {
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
			row.TranslatedText,
			row.Provider,
			row.Model,
		); err != nil {
			return fmt.Errorf("save output item: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit output save: %w", err)
	}
	return nil
}

func (q *Queue) SaveFailed(ctx context.Context, rows []FailedItem) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failed save: %w", err)
	}
	defer rollback(tx)

	stmt, err := tx.PrepareContext(ctx, `
		insert into failed_items (
			source_table,
			source_column,
			source_text,
			source_text_hash,
			source_lang,
			target_lang,
			error_message,
			failed_at
		)
		values (?, ?, ?, ?, ?, ?, ?, current_timestamp)
		on conflict (
			source_table,
			source_column,
			source_text_hash,
			source_lang,
			target_lang
		) do update set
			source_text = excluded.source_text,
			error_message = excluded.error_message,
			failed_at = excluded.failed_at
	`)
	if err != nil {
		return fmt.Errorf("prepare failed save: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		if err := validateFailedItem(row); err != nil {
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
			row.ErrorMessage,
		); err != nil {
			return fmt.Errorf("save failed item: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed save: %w", err)
	}
	return nil
}

func (q *Queue) validateSchema(ctx context.Context) error {
	required := []struct {
		table   string
		columns []string
	}{
		{
			table: "input_items",
			columns: []string{
				"source_table",
				"source_column",
				"source_text",
				"source_text_hash",
				"source_lang",
				"target_lang",
				"source_language_name",
				"target_language_name",
				"created_at",
			},
		},
		{
			table: "output_items",
			columns: []string{
				"source_table",
				"source_column",
				"source_text",
				"source_text_hash",
				"source_lang",
				"target_lang",
				"translated_text",
				"provider",
				"model",
				"completed_at",
			},
		},
		{
			table: "failed_items",
			columns: []string{
				"source_table",
				"source_column",
				"source_text",
				"source_text_hash",
				"source_lang",
				"target_lang",
				"error_message",
				"failed_at",
			},
		},
	}

	for _, table := range required {
		if err := validateTable(ctx, q.db, table.table, table.columns); err != nil {
			return err
		}
	}
	return nil
}

func validateTable(ctx context.Context, db *sql.DB, table string, requiredColumns []string) error {
	rows, err := db.QueryContext(ctx, "select name from pragma_table_info(?)", table)
	if err != nil {
		// Fallback: some driver/version combinations reject binding a
		// parameter into a table-valued function argument. table is always
		// one of this package's own constant table names (never user
		// input), so interpolating it directly here is safe.
		rows, err = db.QueryContext(ctx, fmt.Sprintf("select name from pragma_table_info(%q)", table))
	}
	if err != nil {
		return fmt.Errorf("read %s schema: %w", table, err)
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return fmt.Errorf("scan %s schema: %w", table, err)
		}
		columns[column] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s schema: %w", table, err)
	}
	if len(columns) == 0 {
		return fmt.Errorf("%s table is required in queue duckdb", table)
	}

	for _, column := range requiredColumns {
		if !columns[column] {
			return fmt.Errorf("%s.%s column is required in queue duckdb", table, column)
		}
	}
	return nil
}

func validateTranslatedItem(row TranslatedItem) error {
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
	case row.TranslatedText == "":
		return errors.New("translated_text is required")
	case row.Provider == "":
		return errors.New("provider is required")
	case row.Model == "":
		return errors.New("model is required")
	default:
		return nil
	}
}

func validateFailedItem(row FailedItem) error {
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
	case row.ErrorMessage == "":
		return errors.New("error_message is required")
	default:
		return nil
	}
}

func itemID(item Item) string {
	return fmt.Sprintf(
		"%s|%s|%d|%s|%s",
		item.SourceTable,
		item.SourceColumn,
		item.SourceTextHash,
		item.SourceLang,
		item.TargetLang,
	)
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
