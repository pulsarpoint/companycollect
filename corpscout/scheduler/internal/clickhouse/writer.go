package clickhouse

import (
	"context"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cockroachdb/errors"
)

type Target struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

type Insert struct {
	Table   string
	Columns []string
	Rows    []map[string]any
}

type Writer struct {
	conn     driver.Conn
	database string
}

func Open(ctx context.Context, rawURL string) (*Writer, error) {
	target, err := ParseNativeURL(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{net.JoinHostPort(target.Host, target.Port)},
		Auth: clickhouse.Auth{
			Database: target.Database,
			Username: target.Username,
			Password: target.Password,
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "open clickhouse connection")
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, errors.Wrap(err, "ping clickhouse")
	}
	return &Writer{conn: conn, database: target.Database}, nil
}

func (w *Writer) Close() error {
	if w == nil || w.conn == nil {
		return nil
	}
	return w.conn.Close()
}

func (w *Writer) Insert(ctx context.Context, insert Insert) error {
	if len(insert.Rows) == 0 {
		return nil
	}
	query := BuildInsertQuery(w.database, insert.Table, insert.Columns)
	batch, err := w.conn.PrepareBatch(ctx, query)
	if err != nil {
		return errors.Wrap(err, "prepare clickhouse insert batch")
	}
	sent := false
	defer func() {
		if !sent {
			_ = batch.Close()
		}
	}()
	for _, row := range insert.Rows {
		if err := batch.Append(insertValues(insert.Columns, row)...); err != nil {
			return errors.Wrap(err, "append clickhouse insert row")
		}
	}
	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "send clickhouse insert batch")
	}
	sent = true
	return nil
}

func ParseNativeURL(rawURL string) (Target, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return Target{}, errors.Wrap(err, "parse clickhouse native url")
	}
	if parsed.Scheme != "clickhouse" {
		return Target{}, errors.Errorf("clickhouse native url must use clickhouse scheme, got %q", parsed.Scheme)
	}
	target := Target{
		Host:     parsed.Hostname(),
		Port:     parsed.Port(),
		Username: parsed.Query().Get("username"),
		Password: parsed.Query().Get("password"),
		Database: parsed.Query().Get("database"),
	}
	if parsed.User != nil {
		if target.Username == "" {
			target.Username = parsed.User.Username()
		}
		if password, ok := parsed.User.Password(); ok && target.Password == "" {
			target.Password = password
		}
	}
	if target.Port == "" {
		target.Port = "9000"
	}
	if target.Username == "" {
		target.Username = "default"
	}
	if target.Database == "" {
		target.Database = strings.TrimPrefix(parsed.EscapedPath(), "/")
	}
	if target.Host == "" {
		return Target{}, errors.New("clickhouse native url host is required")
	}
	if target.Database == "" {
		return Target{}, errors.New("clickhouse native url database is required")
	}
	return target, nil
}

func BuildInsertQuery(database string, table string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}
	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table) + " (" + strings.Join(quotedColumns, ", ") + ")"
}

func insertValues(columns []string, row map[string]any) []any {
	values := make([]any, 0, len(columns))
	for _, column := range columns {
		values = append(values, row[column])
	}
	return values
}

func quoteIdent(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(value)
	return "`" + escaped + "`"
}
