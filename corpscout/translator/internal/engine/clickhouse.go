package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const textTranslationInsertBatchRows = 10_000

const insertTextTranslationsSQL = `
INSERT INTO corpscout.text_translations (
    source_table,
    source_column,
    source_text_hash,
    source_lang,
    target_lang,
    translated_text,
    provider,
    model,
    version
)`

type ClickHouse struct {
	conn clickhouse.Conn
}

func OpenClickHouse(ctx context.Context, nativeURL string) (*ClickHouse, error) {
	if nativeURL == "" {
		return nil, errors.New("clickhouse native URL is required")
	}

	options, err := clickhouse.ParseDSN(nativeURL)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse DSN: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

func (c *ClickHouse) Close() error {
	return c.conn.Close()
}

func (c *ClickHouse) QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error) {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query translation input: %w", err)
	}
	defer rows.Close()

	var items []InputItem
	for rows.Next() {
		var item InputItem
		if err := rows.Scan(
			&item.SourceTable,
			&item.SourceColumn,
			&item.SourceText,
			&item.SourceTextHash,
			&item.SourceLang,
			&item.TargetLang,
		); err != nil {
			return nil, fmt.Errorf("scan translation input: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read translation input: %w", err)
	}
	return items, nil
}

func (c *ClickHouse) QueryStaticInput(ctx context.Context, query string) ([]StaticInput, error) {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query static input: %w", err)
	}
	defer rows.Close()

	var items []StaticInput
	for rows.Next() {
		var item StaticInput
		if err := rows.Scan(
			&item.SourceText,
			&item.SourceTextHash,
			&item.Key,
		); err != nil {
			return nil, fmt.Errorf("scan static input: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read static input: %w", err)
	}
	return items, nil
}

func (c *ClickHouse) InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error) {
	return insertTextTranslationsBatched(ctx, rows, textTranslationInsertBatchRows, c.prepareTextTranslationBatch)
}

func (c *ClickHouse) prepareTextTranslationBatch(ctx context.Context) (textTranslationBatch, error) {
	return c.conn.PrepareBatch(ctx, insertTextTranslationsSQL)
}

type textTranslationBatch interface {
	Append(v ...any) error
	Send() error
	Abort() error
}

type prepareTextTranslationBatch func(context.Context) (textTranslationBatch, error)

func insertTextTranslationsBatched(
	ctx context.Context,
	rows []TextTranslation,
	batchRows int,
	prepare prepareTextTranslationBatch,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if batchRows <= 0 {
		return 0, errors.New("text translation insert batch size must be positive")
	}

	inserted := 0
	for start := 0; start < len(rows); start += batchRows {
		end := start + batchRows
		if end > len(rows) {
			end = len(rows)
		}

		batch, err := prepare(ctx)
		if err != nil {
			return inserted, fmt.Errorf("prepare text translations batch: %w", err)
		}

		chunkRows := 0
		for _, row := range rows[start:end] {
			if err := batch.Append(
				row.SourceTable,
				row.SourceColumn,
				row.SourceTextHash,
				row.SourceLang,
				row.TargetLang,
				row.TranslatedText,
				row.Provider,
				row.Model,
				row.Version,
			); err != nil {
				_ = batch.Abort()
				return inserted, fmt.Errorf("append text translation batch row: %w", err)
			}
			chunkRows++
		}

		if err := batch.Send(); err != nil {
			_ = batch.Abort()
			return inserted, fmt.Errorf("send text translation batch: %w", err)
		}
		inserted += chunkRows
	}
	return inserted, nil
}

// InputItem is one distinct untranslated text produced by an LLM-column scan
// query and queued for translation.
type InputItem struct {
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
}

// StaticInput is one distinct untranslated text produced by a static-column
// scan query; Key indexes the column's StaticSpec.Values map.
type StaticInput struct {
	SourceText     string
	SourceTextHash uint64
	Key            string
}

// TextTranslation is one row of corpscout.text_translations.
type TextTranslation struct {
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
	TranslatedText string
	Provider       string
	Model          string
	Version        int64
}
