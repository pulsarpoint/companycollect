package brreg

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

const (
	SourceTable      = "corpscout.no_companies"
	TranslationTable = "corpscout.text_translations"
	SourceLang       = "no"
	TargetLang       = "en"

	ArticlesPurposeColumn = "articles_purpose_original"
	ActivityTextColumn    = "activity_text_original"

	LegalFormDescriptionColumn = "legal_form_description_original"
)

const articlesPurposeScanSQL = `
SELECT DISTINCT
    'corpscout.no_companies' AS source_table,
    'articles_purpose_original' AS source_column,
    c.articles_purpose_original AS source_text,
    cityHash64(c.articles_purpose_original) AS source_text_hash,
    'no' AS source_lang,
    'en' AS target_lang
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'articles_purpose_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.articles_purpose_original)
WHERE c.articles_purpose_original <> ''`

const activityTextScanSQL = `
SELECT DISTINCT
    'corpscout.no_companies' AS source_table,
    'activity_text_original' AS source_column,
    c.activity_text_original AS source_text,
    cityHash64(c.activity_text_original) AS source_text_hash,
    'no' AS source_lang,
    'en' AS target_lang
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.activity_text_original)
WHERE c.activity_text_original <> ''`

const legalFormDescriptionScanSQL = `
SELECT DISTINCT
    c.legal_form_description_original AS source_text,
    cityHash64(c.legal_form_description_original) AS source_text_hash,
    c.legal_form_code AS legal_form_code
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'legal_form_description_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.legal_form_description_original)
WHERE c.legal_form_description_original <> ''`

var legalFormDescriptionENByCode = map[string]string{
	"ADOS": "Administrative unit - public sector",
	"ANNA": "Other legal entity",
	"ANS":  "General partnership",
	"AS":   "Private limited company",
	"ASA":  "Public limited company",
	"BA":   "Company with limited liability",
	"BBL":  "Housing cooperative building association",
	"BO":   "Other estate",
	"BRL":  "Housing cooperative",
	"DA":   "General partnership with shared liability",
	"ENK":  "Sole proprietorship",
	"ESEK": "Condominium (owner-section co-ownership)",
	"FKF":  "County municipal enterprise",
	"FLI":  "Association/club/institution",
	"FYLK": "County authority",
	"GFS":  "Mutual insurance company",
	"IKS":  "Inter-municipal company",
	"KF":   "Municipal enterprise",
	"KBO":  "Bankruptcy estate",
	"KIRK": "Church of Norway",
	"KOMM": "Municipality",
	"KS":   "Limited partnership",
	"KTRF": "Office-sharing arrangement",
	"NUF":  "Norwegian-registered foreign company",
	"OPMV": "Separately divided unit (VAT Act section 2-2)",
	"ORGL": "Organisational subdivision",
	"PERS": "Other registered individuals",
	"PK":   "Pension fund",
	"PRE":  "Shipping partnership",
	"SA":   "Cooperative",
	"SAM":  "Co-ownership under property law",
	"SE":   "European company (SE)",
	"SF":   "State enterprise",
	"SPA":  "Savings bank",
	"STAT": "The State",
	"STI":  "Foundation",
	"SÆR":  "Other enterprise under special legislation",
	"TVAM": "Compulsorily registered for VAT",
	"UTLA": "Foreign entity",
	"VPFO": "Securities fund",
}

type ClickHouseSource interface {
	QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error)
	QueryStaticLegalForms(ctx context.Context, query string) ([]StaticLegalFormInput, error)
	InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error)
}

type Options struct {
	QueuePath string
}

type InitResult struct {
	QueuePath      string
	Created        bool
	RowsSeen       int
	RowsInserted   int
	StaticRowsSeen int
	StaticFlushed  int
}

type InputItem struct {
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
}

type StaticLegalFormInput struct {
	SourceText     string
	SourceTextHash uint64
	LegalFormCode  string
}

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

func InitializeTranslation(ctx context.Context, source ClickHouseSource, options Options) (InitResult, error) {
	if source == nil {
		return InitResult{}, errors.New("clickhouse source is required")
	}
	if options.QueuePath == "" {
		return InitResult{}, errors.New("queue path is required")
	}

	created := false
	if _, err := os.Stat(options.QueuePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return InitResult{}, fmt.Errorf("stat queue %q: %w", options.QueuePath, err)
		}
		created = true
	}

	if err := os.MkdirAll(filepath.Dir(options.QueuePath), 0o755); err != nil {
		return InitResult{}, fmt.Errorf("create queue directory: %w", err)
	}

	db, err := sql.Open("duckdb", options.QueuePath)
	if err != nil {
		return InitResult{}, fmt.Errorf("open queue duckdb: %w", err)
	}
	defer db.Close()

	return initializeTranslationWithDB(ctx, source, db, options.QueuePath, created)
}

func initializeTranslationWithDB(
	ctx context.Context,
	source ClickHouseSource,
	db *sql.DB,
	queuePath string,
	created bool,
) (InitResult, error) {
	if err := createQueueTables(ctx, db); err != nil {
		return InitResult{}, err
	}

	before, err := countRows(ctx, db, "input_items")
	if err != nil {
		return InitResult{}, err
	}

	rowsSeen := 0

	articlesPurposeRows, err := source.QueryTranslationInput(ctx, articlesPurposeScanSQL)
	if err != nil {
		return InitResult{}, fmt.Errorf("query brreg translation input for %s: %w", ArticlesPurposeColumn, err)
	}
	rowsSeen += len(articlesPurposeRows)
	if err := upsertInputItems(ctx, db, articlesPurposeRows); err != nil {
		return InitResult{}, err
	}

	activityTextRows, err := source.QueryTranslationInput(ctx, activityTextScanSQL)
	if err != nil {
		return InitResult{}, fmt.Errorf("query brreg translation input for %s: %w", ActivityTextColumn, err)
	}
	rowsSeen += len(activityTextRows)
	if err := upsertInputItems(ctx, db, activityTextRows); err != nil {
		return InitResult{}, err
	}

	after, err := countRows(ctx, db, "input_items")
	if err != nil {
		return InitResult{}, err
	}

	staticRowsSeen, staticFlushed, err := flushStaticLegalForms(ctx, source, time.Now().Unix())
	if err != nil {
		return InitResult{}, err
	}

	return InitResult{
		QueuePath:      queuePath,
		Created:        created,
		RowsSeen:       rowsSeen,
		RowsInserted:   after - before,
		StaticRowsSeen: staticRowsSeen,
		StaticFlushed:  staticFlushed,
	}, nil
}

func flushStaticLegalForms(ctx context.Context, source ClickHouseSource, version int64) (int, int, error) {
	rows, err := source.QueryStaticLegalForms(ctx, legalFormDescriptionScanSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("query static legal-form translations: %w", err)
	}

	translations := make([]TextTranslation, 0, len(rows))
	for _, row := range rows {
		translatedText := legalFormDescriptionENByCode[row.LegalFormCode]
		if row.SourceText == "" || translatedText == "" {
			continue
		}

		translations = append(translations, TextTranslation{
			SourceTable:    SourceTable,
			SourceColumn:   LegalFormDescriptionColumn,
			SourceText:     row.SourceText,
			SourceTextHash: row.SourceTextHash,
			SourceLang:     SourceLang,
			TargetLang:     TargetLang,
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
		return len(rows), 0, fmt.Errorf("insert static legal-form translations: %w", err)
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
