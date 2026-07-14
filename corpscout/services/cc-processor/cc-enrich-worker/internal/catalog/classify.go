package catalog

import (
	"context"
	"database/sql"
	"strings"

	"github.com/cockroachdb/errors"
	_ "github.com/duckdb/duckdb-go/v2"
)

// PartStats holds the aggregated selected-page count and byte total for one WARC part.
type PartStats struct {
	WarcIndex     uint32
	Pages         int64
	SelectedBytes int64
}

// LoadPartStats aggregates selected pages/bytes per part for warc_index in [lo,hi],
// from the SAME catalog.duckdb file LoadWARC reads. Parts absent from the range are
// simply not returned (they are "empty").
func LoadPartStats(ctx context.Context, path string, lo, hi uint32) ([]PartStats, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("catalog path is required")
	}
	if strings.Contains(path, "?") {
		return nil, errors.Newf("catalog path must not contain '?': %q", path)
	}

	database, err := sql.Open("duckdb", path+"?access_mode=read_only&threads=1")
	if err != nil {
		return nil, errors.Wrap(err, "open DuckDB catalog")
	}
	database.SetMaxOpenConns(1)
	defer database.Close()

	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect to DuckDB catalog")
	}
	defer connection.Close()

	return loadPartStats(ctx, connection, lo, hi)
}

func loadPartStats(ctx context.Context, connection *sql.Conn, lo, hi uint32) ([]PartStats, error) {
	rows, err := connection.QueryContext(
		ctx,
		`SELECT CAST(warc_index AS UBIGINT), CAST(count(*) AS BIGINT),
		        CAST(sum(warc_record_length) AS BIGINT)
		 FROM main.pages WHERE warc_index BETWEEN ? AND ?
		 GROUP BY warc_index ORDER BY warc_index`,
		lo, hi,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "query part stats for range [%d,%d]", lo, hi)
	}
	defer rows.Close()

	var stats []PartStats
	for rows.Next() {
		var warcIndex uint64
		var pages, selectedBytes int64
		if err := rows.Scan(&warcIndex, &pages, &selectedBytes); err != nil {
			return nil, errors.Wrapf(err, "scan part stats for range [%d,%d]", lo, hi)
		}
		stats = append(stats, PartStats{
			WarcIndex:     uint32(warcIndex),
			Pages:         pages,
			SelectedBytes: selectedBytes,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrapf(err, "read part stats for range [%d,%d]", lo, hi)
	}
	return stats, nil
}
