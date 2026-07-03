package engine

import "testing"

// Golden copies of the hand-written Norway BRREG scan queries that the
// generated SQL must reproduce byte-for-byte.
const goldenArticlesPurposeScanSQL = `
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

const goldenActivityTextScanSQL = `
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

const goldenLegalFormScanSQL = `
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

func norwayDefinition() Definition {
	return Definition{
		Source:             "norway_brreg",
		SourceLang:         "no",
		TargetLang:         "en",
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
		Columns: []ColumnSpec{
			{Table: "corpscout.no_companies", Column: "articles_purpose_original"},
			{Table: "corpscout.no_companies", Column: "activity_text_original"},
			{
				Table:  "corpscout.no_companies",
				Column: "legal_form_description_original",
				Static: &StaticSpec{
					KeyColumn: "legal_form_code",
					Values:    map[string]string{"AS": "Private limited company"},
				},
			},
		},
	}
}

func TestScanSQLGeneratesGoldenLLMQueries(t *testing.T) {
	def := norwayDefinition()

	tests := []struct {
		name   string
		column ColumnSpec
		want   string
	}{
		{"articles_purpose", def.Columns[0], goldenArticlesPurposeScanSQL},
		{"activity_text", def.Columns[1], goldenActivityTextScanSQL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScanSQL(def, tt.column)
			if err != nil {
				t.Fatalf("scan sql: %v", err)
			}
			if got != tt.want {
				t.Fatalf("generated SQL does not match golden query\ngot:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestScanSQLGeneratesGoldenStaticQuery(t *testing.T) {
	def := norwayDefinition()

	got, err := ScanSQL(def, def.Columns[2])
	if err != nil {
		t.Fatalf("scan sql: %v", err)
	}
	if got != goldenLegalFormScanSQL {
		t.Fatalf("generated static SQL does not match golden query\ngot:\n%s\nwant:\n%s", got, goldenLegalFormScanSQL)
	}
}

func TestScanSQLPrefersCustomSQL(t *testing.T) {
	def := norwayDefinition()
	col := ColumnSpec{
		Table:     "corpscout.no_companies",
		Column:    "weird_case",
		CustomSQL: "SELECT DISTINCT 1 AS source_table",
	}

	got, err := ScanSQL(def, col)
	if err != nil {
		t.Fatalf("scan sql: %v", err)
	}
	if got != col.CustomSQL {
		t.Fatalf("expected custom SQL passthrough, got:\n%s", got)
	}
}
