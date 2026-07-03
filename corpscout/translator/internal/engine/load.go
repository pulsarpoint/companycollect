package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/pulsarpoint/corpscout/translator/internal/queuedb"
)

// ClickHouseSource is the ClickHouse surface the engine needs; *ClickHouse
// satisfies it, tests use fakes.
type ClickHouseSource interface {
	InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error)
}

func createQueueTables(ctx context.Context, db *sql.DB) error {
	return queuedb.CreateTables(ctx, db)
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
			source_language_name,
			target_language_name,
			created_at
		)
		values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
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
			row.SourceLanguageName,
			row.TargetLanguageName,
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
	case row.SourceLanguageName == "":
		return errors.New("source_language_name is required")
	case row.TargetLanguageName == "":
		return errors.New("target_language_name is required")
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
