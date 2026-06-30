package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/pulsarpoint/corpscout/translator/internal/translation"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type Queue struct {
	db          *sql.DB
	translator  translation.Translator
	closeOnDone bool
}

type Item struct {
	ItemID         string
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
}

type TranslatedItem struct {
	Item
	TranslatedText string
	Provider       string
	Model          string
}

func Init(path string, translator translation.Translator) (*Queue, error) {
	if path == "" {
		return nil, errors.New("queue duckdb path is required")
	}
	if translator == nil {
		return nil, errors.New("translator is required")
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

	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open queue duckdb: %w", err)
	}
	q, err := New(db, translator)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	q.closeOnDone = true
	return q, nil
}

func New(db *sql.DB, translator translation.Translator) (*Queue, error) {
	if db == nil {
		return nil, errors.New("queue duckdb connection is required")
	}
	if translator == nil {
		return nil, errors.New("translator is required")
	}

	q := &Queue{db: db, translator: translator}
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

func (q *Queue) GetBatch(ctx context.Context, limit int) ([]Item, error) {
	if limit <= 0 {
		return nil, errors.New("batch size must be positive")
	}

	rows, err := q.db.QueryContext(ctx, `
		select
			i.source_table,
			i.source_column,
			i.source_text,
			i.source_text_hash::varchar,
			i.source_lang,
			i.target_lang
		from input_items as i
		where not exists (
			select 1
			from output_items as o
			where o.source_table = i.source_table
				and o.source_column = i.source_column
				and o.source_text_hash = i.source_text_hash
				and o.source_lang = i.source_lang
				and o.target_lang = i.target_lang
		)
		order by
			i.source_table,
			i.source_column,
			i.source_text_hash,
			i.source_lang,
			i.target_lang
		limit ?
	`, limit)
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
		values (?, ?, ?, cast(? as ubigint), ?, ?, ?, ?, ?, current_timestamp)
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

func (q *Queue) ProcessBatch(
	ctx context.Context,
	limit int,
	timeoutSeconds int,
	provider string,
	model string,
) (int, error) {
	if provider == "" {
		return 0, errors.New("provider is required")
	}
	if model == "" {
		return 0, errors.New("model is required")
	}

	items, err := q.GetBatch(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	inputs := make([]translation.TranslationInput, 0, len(items))
	expectedItemIDs := make(map[string]bool, len(items))
	for _, item := range items {
		inputs = append(inputs, translation.TranslationInput{
			ItemID:     item.ItemID,
			SourceText: item.SourceText,
			SourceLang: item.SourceLang,
			TargetLang: item.TargetLang,
		})
		expectedItemIDs[item.ItemID] = true
	}

	results, err := q.translator.Translate(ctx, inputs, timeoutSeconds)
	if err != nil {
		return 0, fmt.Errorf("translate queue batch: %w", err)
	}

	resultsByID := make(map[string]string, len(results))
	for _, result := range results {
		if result.ItemID == "" {
			return 0, errors.New("translation result item_id is required")
		}
		if !expectedItemIDs[result.ItemID] {
			return 0, fmt.Errorf("unexpected translation result item_id: %s", result.ItemID)
		}
		if result.TranslatedText == "" {
			return 0, fmt.Errorf("translation result for %s has empty translated text", result.ItemID)
		}
		if _, exists := resultsByID[result.ItemID]; exists {
			return 0, fmt.Errorf("translation result for %s is duplicated", result.ItemID)
		}
		resultsByID[result.ItemID] = result.TranslatedText
	}

	output := make([]TranslatedItem, 0, len(items))
	for _, item := range items {
		translatedText, ok := resultsByID[item.ItemID]
		if !ok {
			return 0, fmt.Errorf("translation result for %s is missing", item.ItemID)
		}
		output = append(output, TranslatedItem{
			Item:           item,
			TranslatedText: translatedText,
			Provider:       provider,
			Model:          model,
		})
	}

	if err := q.SaveBatch(ctx, output); err != nil {
		return 0, err
	}
	return len(output), nil
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
	}

	for _, table := range required {
		if err := validateTable(ctx, q.db, table.table, table.columns); err != nil {
			return err
		}
	}
	return nil
}

func validateTable(ctx context.Context, db *sql.DB, table string, requiredColumns []string) error {
	rows, err := db.QueryContext(ctx, `
		select column_name
		from information_schema.columns
		where table_name = ?
	`, table)
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
