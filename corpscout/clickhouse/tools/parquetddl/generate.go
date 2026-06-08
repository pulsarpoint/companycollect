package main

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

func generateMigrations(cfg Config, exportDir string, describer Describer) (string, string, error) {
	var up strings.Builder
	var down strings.Builder

	for _, name := range sortedTableNames(cfg.Tables) {
		table := cfg.Tables[name]
		columns, err := describer.Describe(filepath.Join(exportDir, table.Parquet))
		if err != nil {
			return "", "", errors.Wrapf(err, "describe parquet table %s", name)
		}
		if len(columns) == 0 {
			return "", "", errors.Errorf("table %s has no columns", name)
		}

		up.WriteString("CREATE TABLE IF NOT EXISTS ")
		up.WriteString(quoteIdent(cfg.Database))
		up.WriteString(".")
		up.WriteString(quoteIdent(table.Table))
		up.WriteString(" (\n")

		rendered := make([]string, 0, len(columns)+len(table.InjectColumns))
		for _, column := range columns {
			rendered = append(rendered, "  "+quoteIdent(column.Name)+" "+column.Type)
		}
		for _, injected := range sortedKeys(table.InjectColumns) {
			rendered = append(rendered, "  "+quoteIdent(injected)+" "+table.InjectColumns[injected])
		}
		up.WriteString(strings.Join(rendered, ",\n"))
		up.WriteString("\n)\n")
		up.WriteString("ENGINE = ")
		up.WriteString(table.Engine)
		up.WriteString("\n")
		if table.PartitionBy != "" {
			up.WriteString("PARTITION BY ")
			up.WriteString(table.PartitionBy)
			up.WriteString("\n")
		}
		up.WriteString("ORDER BY (")
		up.WriteString(joinQuoted(table.OrderBy))
		up.WriteString(");\n\n")

		down.WriteString("DROP TABLE IF EXISTS ")
		down.WriteString(quoteIdent(cfg.Database))
		down.WriteString(".")
		down.WriteString(quoteIdent(table.Table))
		down.WriteString(";\n")
	}

	return up.String(), down.String(), nil
}

func quoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func joinQuoted(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteIdent(value))
	}
	return strings.Join(quoted, ", ")
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
