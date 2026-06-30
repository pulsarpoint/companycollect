package brreg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
)

func TestCreateInputQueueWithExistingClickHouseProducesNorwayBRREGDuckDBEntries(t *testing.T) {
	if os.Getenv("TRANSLATOR_INTEGRATION_TESTS") != "true" {
		t.Skip("set TRANSLATOR_INTEGRATION_TESTS=true and CLICKHOUSE_* environment variables to run")
	}

	cfg, _, err := config.LoadFromEnvironment()
	if err != nil {
		t.Fatalf("load translator config: %v", err)
	}
	if cfg.ClickHouse.NativeURL == "" {
		t.Fatal("clickhouse native URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("clickhouse", cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}

	if sourceRows := clickHouseTableCount(t, ctx, db, "corpscout.no_companies"); sourceRows == 0 {
		t.Fatal("corpscout.no_companies is empty")
	}

	expectedArticlesPurposeRows := clickHouseScanCount(t, ctx, db, articlesPurposeScanSQL)
	expectedActivityTextRows := clickHouseScanCount(t, ctx, db, activityTextScanSQL)
	expectedDynamicRows := expectedArticlesPurposeRows + expectedActivityTextRows
	beforeStaticRows := clickHouseStaticTranslationCount(t, ctx, db)

	source, err := OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open brreg clickhouse source: %v", err)
	}
	defer source.Close()

	queuePath := filepath.Join(t.TempDir(), "norway_brreg.duckdb")
	result, err := InitializeTranslation(ctx, source, Options{QueuePath: queuePath})
	if err != nil {
		t.Fatalf("initialize translation: %v", err)
	}

	if result.RowsSeen != expectedDynamicRows {
		t.Fatalf("expected %d dynamic rows, got %d", expectedDynamicRows, result.RowsSeen)
	}
	if result.RowsInserted != expectedDynamicRows {
		t.Fatalf("expected %d inserted dynamic rows, got %d", expectedDynamicRows, result.RowsInserted)
	}
	queueDB, err := sql.Open("duckdb", queuePath)
	if err != nil {
		t.Fatalf("open queue duckdb: %v", err)
	}
	defer queueDB.Close()

	if got := duckDBCount(t, queueDB, "select count(*) from input_items"); got != expectedDynamicRows {
		t.Fatalf("expected %d queue input rows, got %d", expectedDynamicRows, got)
	}
	if got := duckDBCount(t, queueDB, "select count(*) from output_items"); got != 0 {
		t.Fatalf("expected empty output queue, got %d rows", got)
	}
	if got := duckDBCount(t, queueDB, "select count(*) from input_items where source_column = 'articles_purpose_original'"); got != expectedArticlesPurposeRows {
		t.Fatalf("expected %d articles_purpose_original rows, got %d", expectedArticlesPurposeRows, got)
	}
	if got := duckDBCount(t, queueDB, "select count(*) from input_items where source_column = 'activity_text_original'"); got != expectedActivityTextRows {
		t.Fatalf("expected %d activity_text_original rows, got %d", expectedActivityTextRows, got)
	}
	if got := duckDBCount(t, queueDB, "select count(*) from input_items where source_column = 'legal_form_description_original'"); got != 0 {
		t.Fatalf("static legal-form rows must not enter input queue, got %d rows", got)
	}
	if got := duckDBCount(t, queueDB, `
		select count(*)
		from input_items
		where source_table <> 'corpscout.no_companies'
		   or source_lang <> 'no'
		   or target_lang <> 'en'
		   or source_text = ''
	`); got != 0 {
		t.Fatalf("queue has %d malformed rows", got)
	}
	if got := duckDBCount(t, queueDB, `
		select count(*)
		from input_items
		where source_column not in ('articles_purpose_original', 'activity_text_original')
	`); got != 0 {
		t.Fatalf("queue has %d rows for unexpected columns", got)
	}
	if got := duckDBCount(t, queueDB, `
		select count(*)
		from (
			select source_table, source_column, source_text_hash, source_lang, target_lang
			from input_items
			group by source_table, source_column, source_text_hash, source_lang, target_lang
			having count(*) > 1
		)
	`); got != 0 {
		t.Fatalf("queue has %d duplicate queue keys", got)
	}
	if afterStaticRows := clickHouseStaticTranslationCount(t, ctx, db); afterStaticRows < beforeStaticRows {
		t.Fatalf("static translation rows decreased from %d to %d", beforeStaticRows, afterStaticRows)
	}
	t.Logf(
		"queue_path=%s dynamic_rows=%d static_rows_seen=%d static_flushed=%d",
		queuePath,
		result.RowsInserted,
		result.StaticRowsSeen,
		result.StaticFlushed,
	)
}

func duckDBCount(t *testing.T, db *sql.DB, query string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("query duckdb count: %v\nSQL:\n%s", err, query)
	}
	return count
}

func TestInsertTextTranslationsWithExistingClickHouse(t *testing.T) {
	if os.Getenv("TRANSLATOR_INTEGRATION_TESTS") != "true" {
		t.Skip("set TRANSLATOR_INTEGRATION_TESTS=true and CLICKHOUSE_* environment variables to run")
	}

	cfg, _, err := config.LoadFromEnvironment()
	if err != nil {
		t.Fatalf("load translator config: %v", err)
	}
	if cfg.ClickHouse.NativeURL == "" {
		t.Fatal("clickhouse native URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("clickhouse", cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	deleteIntegrationRows := func() {
		_, _ = db.ExecContext(context.Background(), `
			ALTER TABLE corpscout.text_translations
			DELETE WHERE source_table = 'corpscout.integration_test'
			  AND source_column = 'translation_batch_test'
		`)
	}
	deleteIntegrationRows()
	t.Cleanup(deleteIntegrationRows)

	source, err := OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		t.Fatalf("open brreg clickhouse source: %v", err)
	}
	defer source.Close()

	inserted, err := source.InsertTextTranslations(ctx, []TextTranslation{
		{
			SourceTable:    "corpscout.integration_test",
			SourceColumn:   "translation_batch_test",
			SourceTextHash: 18_446_744_073_709_551_615,
			SourceLang:     SourceLang,
			TargetLang:     TargetLang,
			TranslatedText: "integration translation",
			Provider:       "integration-test",
			Model:          "integration-test",
			Version:        time.Now().Unix(),
		},
	})
	if err != nil {
		t.Fatalf("insert text translations: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("expected 1 inserted row, got %d", inserted)
	}

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count()
		FROM corpscout.text_translations
		WHERE source_table = 'corpscout.integration_test'
		  AND source_column = 'translation_batch_test'
		  AND source_text_hash = 18446744073709551615
		  AND translated_text = 'integration translation'
	`).Scan(&count); err != nil {
		t.Fatalf("count inserted integration row: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 inserted integration row, got %d", count)
	}
}

func clickHouseTableCount(t *testing.T, ctx context.Context, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count table %s: %v", table, err)
	}
	return count
}

func clickHouseScanCount(t *testing.T, ctx context.Context, db *sql.DB, query string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count() FROM ("+query+")").Scan(&count); err != nil {
		t.Fatalf("count scan rows: %v", err)
	}
	return count
}

func clickHouseStaticTranslationCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count()
		FROM corpscout.text_translations
		WHERE source_table = 'corpscout.no_companies'
		  AND source_column = 'legal_form_description_original'
	`).Scan(&count); err != nil {
		t.Fatalf("count static translation rows: %v", err)
	}
	return count
}
