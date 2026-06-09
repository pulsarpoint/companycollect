package clickhouseclient

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/cockroachdb/errors"
)

type Insert struct {
	Database string
	Table    string
	Columns  []string
	Rows     []map[string]any
}

func BuildInsertQuery(database string, table string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " (" + strings.Join(quotedColumns, ", ") + ") FORMAT JSONEachRow"
}

func EncodeJSONEachRow(rows []map[string]any) ([]byte, error) {
	var body bytes.Buffer
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, errors.Wrap(err, "encode clickhouse JSONEachRow")
		}
		body.Write(encoded)
		body.WriteByte('\n')
	}
	return body.Bytes(), nil
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(value)
	return "`" + escaped + "`"
}
