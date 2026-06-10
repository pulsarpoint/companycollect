package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Reader struct {
	conn     driver.Conn
	database string
}

func OpenReader(ctx context.Context, rawURL string) (*Reader, error) {
	target, err := ParseNativeURL(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := openConn(ctx, target)
	if err != nil {
		return nil, err
	}
	return &Reader{conn: conn, database: target.Database}, nil
}

func (r *Reader) Close() error {
	if r == nil || r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

func (r *Reader) Database() string {
	if r == nil {
		return ""
	}
	return r.database
}

func (r *Reader) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return r.conn.Query(ctx, query, args...)
}

func (r *Reader) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return r.conn.QueryRow(ctx, query, args...)
}
