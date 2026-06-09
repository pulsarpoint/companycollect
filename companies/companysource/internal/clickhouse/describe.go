package clickhouse

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Describer interface {
	Describe(path string) ([]Column, error)
}

type ClickHouseLocalDescriber struct {
	Binary string
}

func (d ClickHouseLocalDescriber) Describe(path string) ([]Column, error) {
	binary := strings.TrimSpace(d.Binary)
	if binary == "" {
		binary = "clickhouse-local"
	}
	query := "DESCRIBE TABLE file(" + clickHouseStringLiteral(path) + ", Parquet) FORMAT JSONEachRow"
	cmd := exec.Command(binary, "--query", query)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, errors.Wrapf(err, "describe parquet schema stderr=%s", stderr.String())
	}

	var columns []Column
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, errors.Wrap(err, "decode clickhouse describe row")
		}
		columns = append(columns, Column{Name: row.Name, Type: row.Type})
	}
	return columns, nil
}

func clickHouseStringLiteral(value string) string {
	escaped := strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
	).Replace(value)
	return "'" + escaped + "'"
}
