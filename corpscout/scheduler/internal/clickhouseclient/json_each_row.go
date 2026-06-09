package clickhouseclient

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
)

const DefaultClickHouseImage = "clickhouse/clickhouse-server:26.5"
const DefaultCompanycollectHostIP = "100.85.212.113"

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

func ExecuteInsert(ctx context.Context, nativeURL string, image string, insert Insert) error {
	target, err := ParseNativeURL(nativeURL)
	if err != nil {
		return err
	}
	if strings.TrimSpace(image) == "" {
		image = DefaultClickHouseImage
	}

	body, err := EncodeJSONEachRow(insert.Rows)
	if err != nil {
		return err
	}

	query := BuildInsertQuery(insert.Database, insert.Table, insert.Columns)
	cmd := exec.CommandContext(ctx, "docker", clickHouseClientDockerArgs(image, target, query)...)
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "run clickhouse insert stderr=%s", strings.TrimSpace(stderr.String()))
	}
	return nil
}

func clickHouseClientDockerArgs(image string, target Target, query string) []string {
	args := []string{"run", "--rm", "-i", "--add-host", "host.docker.internal:host-gateway"}
	if target.Host == "companycollect" {
		hostIP := strings.TrimSpace(os.Getenv("COMPANYCOLLECT_HOST_IP"))
		if hostIP == "" {
			hostIP = DefaultCompanycollectHostIP
		}
		args = append(args, "--add-host", "companycollect:"+hostIP)
	}
	args = append(args,
		image,
		"clickhouse-client",
		"--host", target.Host,
		"--port", target.Port,
		"--user", target.Username,
		"--database", target.Database,
	)
	if target.Password != "" {
		args = append(args, "--password", target.Password)
	}
	args = append(args, "--query", query)
	return args
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(value)
	return "`" + escaped + "`"
}
