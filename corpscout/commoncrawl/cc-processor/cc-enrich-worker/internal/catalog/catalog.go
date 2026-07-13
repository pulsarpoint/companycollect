// Package catalog reads the immutable DuckDB catalog produced by cc-warc-index-builder.
package catalog

import (
	"context"
	"database/sql"
	"math"
	"strings"

	"github.com/cockroachdb/errors"
	_ "github.com/duckdb/duckdb-go/v2"

	"cc-enrich-worker/internal/model"
)

// Warc identifies one Common Crawl WARC object.
type Warc struct {
	WarcIndex    uint32
	WarcFilename string
}

// LoadWARC reads one WARC and its selected pages from a local DuckDB catalog.
func LoadWARC(ctx context.Context, path string, warcIndex uint32) (Warc, []model.WorklistItem, error) {
	if strings.TrimSpace(path) == "" {
		return Warc{}, nil, errors.New("catalog path is required")
	}
	if strings.Contains(path, "?") {
		return Warc{}, nil, errors.Newf("catalog path must not contain '?': %q", path)
	}

	database, err := sql.Open("duckdb", path+"?access_mode=read_only&threads=1")
	if err != nil {
		return Warc{}, nil, errors.Wrap(err, "open DuckDB catalog")
	}
	database.SetMaxOpenConns(1)
	defer database.Close()

	connection, err := database.Conn(ctx)
	if err != nil {
		return Warc{}, nil, errors.Wrap(err, "connect to DuckDB catalog")
	}
	defer connection.Close()

	warc, items, err := loadWARC(ctx, connection, "main", warcIndex)
	if err != nil {
		return Warc{}, nil, err
	}
	return warc, items, nil
}

func loadWARC(
	ctx context.Context,
	connection *sql.Conn,
	tablePrefix string,
	warcIndex uint32,
) (Warc, []model.WorklistItem, error) {
	if tablePrefix != "main" && tablePrefix != "catalog.main" {
		return Warc{}, nil, errors.Newf("unsupported DuckDB catalog prefix %q", tablePrefix)
	}

	qualifiedWarcs := tablePrefix + ".warcs"
	qualifiedPages := tablePrefix + ".pages"
	var loadedIndex uint64
	var filename string
	err := connection.QueryRowContext(
		ctx,
		"SELECT CAST(warc_index AS UBIGINT), warc_filename FROM "+qualifiedWarcs+" WHERE warc_index = ?",
		warcIndex,
	).Scan(&loadedIndex, &filename)
	if errors.Is(err, sql.ErrNoRows) {
		return Warc{}, nil, errors.Newf("WARC index %d is absent from the catalog", warcIndex)
	}
	if err != nil {
		return Warc{}, nil, errors.Wrapf(err, "query WARC index %d", warcIndex)
	}
	if loadedIndex != uint64(warcIndex) {
		return Warc{}, nil, errors.Newf("catalog returned WARC index %d for requested index %d", loadedIndex, warcIndex)
	}
	if strings.TrimSpace(filename) == "" {
		return Warc{}, nil, errors.Newf("WARC index %d has an empty filename", warcIndex)
	}
	warc := Warc{WarcIndex: warcIndex, WarcFilename: filename}

	rows, err := connection.QueryContext(
		ctx,
		`SELECT root_domain, url, CAST(domain_page_rank AS UBIGINT),
		        CAST(warc_record_offset AS UBIGINT), CAST(warc_record_length AS UBIGINT)
		 FROM `+qualifiedPages+`
		 WHERE warc_index = ?
		 ORDER BY warc_record_offset`,
		warcIndex,
	)
	if err != nil {
		return Warc{}, nil, errors.Wrapf(err, "query pages for WARC index %d", warcIndex)
	}
	defer rows.Close()

	var items []model.WorklistItem
	var previousOffset uint64
	for rowIndex := 0; rows.Next(); rowIndex++ {
		var rootDomain, pageURL string
		var rank, offset, length uint64
		if err := rows.Scan(&rootDomain, &pageURL, &rank, &offset, &length); err != nil {
			return Warc{}, nil, errors.Wrapf(err, "scan page %d for WARC index %d", rowIndex, warcIndex)
		}
		if strings.TrimSpace(rootDomain) == "" || strings.TrimSpace(pageURL) == "" {
			return Warc{}, nil, errors.Newf("page %d for WARC index %d is missing domain or URL", rowIndex, warcIndex)
		}
		if rank == 0 || rank > math.MaxUint16 {
			return Warc{}, nil, errors.Newf("page %d for WARC index %d has invalid domain rank %d", rowIndex, warcIndex, rank)
		}
		if rowIndex > 0 && offset < previousOffset {
			return Warc{}, nil, errors.Newf("pages for WARC index %d are not ordered by offset", warcIndex)
		}
		if length == 0 || offset > math.MaxInt64 || length > math.MaxInt64 || length-1 > math.MaxInt64-offset {
			return Warc{}, nil, errors.Newf(
				"page %d has invalid WARC coordinates: index=%d offset=%d length=%d",
				rowIndex,
				warcIndex,
				offset,
				length,
			)
		}
		previousOffset = offset
		items = append(items, model.WorklistItem{
			RootDomain:   rootDomain,
			URL:          pageURL,
			WarcIndex:    warcIndex,
			WarcFilename: filename,
			Offset:       int64(offset),
			Length:       int64(length),
			Primary:      rank == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return Warc{}, nil, errors.Wrapf(err, "read pages for WARC index %d", warcIndex)
	}
	return warc, items, nil
}
