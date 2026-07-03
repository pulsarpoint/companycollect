package engine

import (
	"bytes"
	"fmt"
	"text/template"
)

// The scan templates generate the ClickHouse queries that find distinct
// not-yet-translated texts for one column via an anti-join against
// corpscout.text_translations. The golden tests in scan_test.go pin the
// rendered output byte-for-byte to the hand-written queries the Norway BRREG
// source used before the engine was generalized.

const llmScanTemplateSource = `
SELECT DISTINCT
    '{{.Table}}' AS source_table,
    '{{.Column}}' AS source_column,
    c.{{.Column}} AS source_text,
    cityHash64(c.{{.Column}}) AS source_text_hash,
    '{{.SourceLang}}' AS source_lang,
    '{{.TargetLang}}' AS target_lang
FROM {{.Table}} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{{.Table}}' AND source_column = '{{.Column}}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{{.Column}})
WHERE c.{{.Column}} <> ''`

const staticScanTemplateSource = `
SELECT DISTINCT
    c.{{.Column}} AS source_text,
    cityHash64(c.{{.Column}}) AS source_text_hash,
    c.{{.KeyColumn}} AS {{.KeyColumn}}
FROM {{.Table}} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{{.Table}}' AND source_column = '{{.Column}}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{{.Column}})
WHERE c.{{.Column}} <> ''`

var (
	llmScanTemplate    = template.Must(template.New("llm-scan").Parse(llmScanTemplateSource))
	staticScanTemplate = template.Must(template.New("static-scan").Parse(staticScanTemplateSource))
)

type scanTemplateData struct {
	Table      string
	Column     string
	SourceLang string
	TargetLang string
	KeyColumn  string
}

// ScanSQL returns the ClickHouse scan query for one column: the custom SQL
// when the definition configures one, otherwise the generated anti-join
// query. LLM columns select (source_table, source_column, source_text,
// source_text_hash, source_lang, target_lang); static columns select
// (source_text, source_text_hash, key). Custom SQL must return the same
// columns as the query it replaces.
func ScanSQL(def Definition, col ColumnSpec) (string, error) {
	if col.CustomSQL != "" {
		return col.CustomSQL, nil
	}

	data := scanTemplateData{
		Table:      col.Table,
		Column:     col.Column,
		SourceLang: def.SourceLang,
		TargetLang: def.TargetLang,
	}
	tmpl := llmScanTemplate
	if col.Static != nil {
		data.KeyColumn = col.Static.KeyColumn
		tmpl = staticScanTemplate
	}

	var query bytes.Buffer
	if err := tmpl.Execute(&query, data); err != nil {
		return "", fmt.Errorf("render scan SQL for %s.%s: %w", col.Table, col.Column, err)
	}
	return query.String(), nil
}
