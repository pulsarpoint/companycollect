// Package catalog reads the static Parquet catalog produced by cc-warc-index-builder.
package catalog

import (
	"fmt"
	"io"
	"math"
	"os"
	"reflect"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/model"
)

// Warc is one Common Crawl object and the selected-page totals stored in the catalog inventory.
type Warc struct {
	WarcIndex     uint32 `parquet:"warc_index"`
	WarcFilename  string `parquet:"warc_filename"`
	SelectedPages uint64 `parquet:"selected_pages"`
	SelectedBytes uint64 `parquet:"selected_bytes"`
}

type pageRow struct {
	WarcIndex        uint32  `parquet:"warc_index"`
	RootDomain       string  `parquet:"root_domain"`
	URL              string  `parquet:"url"`
	DomainPageRank   uint16  `parquet:"domain_page_rank"`
	ContentLanguages *string `parquet:"content_languages,optional"`
	WarcRecordOffset uint64  `parquet:"warc_record_offset"`
	WarcRecordLength uint64  `parquet:"warc_record_length"`
}

type requiredColumn struct {
	name     string
	typeName string
}

var warcColumns = []requiredColumn{
	{"warc_index", "uint32"},
	{"warc_filename", "string"},
	{"selected_pages", "uint64"},
	{"selected_bytes", "uint64"},
}

var pageColumns = []requiredColumn{
	{"warc_index", "uint32"},
	{"root_domain", "string"},
	{"url", "string"},
	{"domain_page_rank", "uint16"},
	{"content_languages", "string"},
	{"warc_record_offset", "uint64"},
	{"warc_record_length", "uint64"},
}

// LoadWarcs reads and validates the complete WARC inventory.
func LoadWarcs(path string) ([]Warc, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open WARC inventory")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "stat WARC inventory")
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "read WARC inventory footer")
	}
	if err := validateSchema(parquetFile.Schema(), warcColumns); err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "validate WARC inventory schema")
	}
	if err := file.Close(); err != nil {
		return nil, errors.Wrap(err, "close WARC inventory")
	}

	warcs, err := parquet.ReadFile[Warc](path)
	if err != nil {
		return nil, errors.Wrap(err, "read WARC inventory rows")
	}
	if len(warcs) == 0 {
		return nil, errors.New("WARC inventory is empty")
	}

	filenames := make(map[string]struct{}, len(warcs))
	for rowIndex, warc := range warcs {
		if uint64(warc.WarcIndex) != uint64(rowIndex) {
			return nil, errors.Newf(
				"WARC inventory row %d has index %d; indexes must be ordered and contiguous from zero",
				rowIndex, warc.WarcIndex,
			)
		}
		if warc.WarcFilename == "" {
			return nil, errors.Newf("WARC inventory row %d has an empty filename", rowIndex)
		}
		if _, exists := filenames[warc.WarcFilename]; exists {
			return nil, errors.Newf("WARC inventory contains duplicate filename %q", warc.WarcFilename)
		}
		if (warc.SelectedPages == 0) != (warc.SelectedBytes == 0) {
			return nil, errors.Newf(
				"WARC inventory index %d has inconsistent selected totals: pages=%d bytes=%d",
				warc.WarcIndex, warc.SelectedPages, warc.SelectedBytes,
			)
		}
		filenames[warc.WarcFilename] = struct{}{}
	}
	return warcs, nil
}

// LoadRange returns the inclusive WARC range and its selected pages. A WARC with no selected pages
// is a valid result with no WorklistItems.
func LoadRange(warcsPath, pagesPath string, firstWarc, lastWarc uint32) ([]Warc, []model.WorklistItem, error) {
	if lastWarc < firstWarc {
		return nil, nil, errors.Newf("invalid WARC range %d-%d", firstWarc, lastWarc)
	}

	allWarcs, err := LoadWarcs(warcsPath)
	if err != nil {
		return nil, nil, err
	}
	requestedCount := uint64(lastWarc) - uint64(firstWarc) + 1
	if requestedCount > uint64(len(allWarcs)) || uint64(lastWarc) >= uint64(len(allWarcs)) {
		return nil, nil, errors.Newf(
			"requested WARC range %d-%d exceeds inventory indexes 0-%d",
			firstWarc, lastWarc, len(allWarcs)-1,
		)
	}

	requested := append([]Warc(nil), allWarcs[int(firstWarc):int(lastWarc)+1]...)
	byIndex := make(map[uint32]Warc, len(allWarcs))
	for _, warc := range allWarcs {
		byIndex[warc.WarcIndex] = warc
	}

	items, err := loadPages(pagesPath, byIndex, firstWarc, lastWarc)
	if err != nil {
		return nil, nil, err
	}
	pageCounts := make(map[uint32]uint64, len(requested))
	byteCounts := make(map[uint32]uint64, len(requested))
	for _, item := range items {
		pageCounts[item.WarcIndex]++
		byteCounts[item.WarcIndex] += uint64(item.Length)
	}
	for _, warc := range requested {
		if pageCounts[warc.WarcIndex] != warc.SelectedPages || byteCounts[warc.WarcIndex] != warc.SelectedBytes {
			return nil, nil, errors.Newf(
				"WARC index %d page totals do not match inventory: pages=%d/%d bytes=%d/%d",
				warc.WarcIndex,
				pageCounts[warc.WarcIndex], warc.SelectedPages,
				byteCounts[warc.WarcIndex], warc.SelectedBytes,
			)
		}
	}
	return requested, items, nil
}

func loadPages(path string, warcs map[uint32]Warc, firstWarc, lastWarc uint32) ([]model.WorklistItem, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open page catalog")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat page catalog")
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return nil, errors.Wrap(err, "read page catalog footer")
	}
	if err := validateSchema(parquetFile.Schema(), pageColumns); err != nil {
		return nil, errors.Wrap(err, "validate page catalog schema")
	}
	warcColumn, _ := parquetFile.Schema().Lookup("warc_index")

	var items []model.WorklistItem
	var previous pageRow
	havePrevious := false
	for groupIndex, rowGroup := range parquetFile.RowGroups() {
		readGroup, err := rowGroupOverlaps(rowGroup, warcColumn.ColumnIndex, firstWarc, lastWarc)
		if err != nil {
			return nil, errors.Wrapf(err, "read page catalog row-group %d statistics", groupIndex)
		}
		if !readGroup {
			continue
		}

		reader := parquet.NewGenericRowGroupReader[pageRow](rowGroup)
		rows := make([]pageRow, 4096)
		for {
			n, readErr := reader.Read(rows)
			for rowIndex := 0; rowIndex < n; rowIndex++ {
				row := rows[rowIndex]
				if row.WarcIndex < firstWarc || row.WarcIndex > lastWarc {
					continue
				}
				if havePrevious && (row.WarcIndex < previous.WarcIndex ||
					(row.WarcIndex == previous.WarcIndex && row.WarcRecordOffset < previous.WarcRecordOffset)) {
					_ = reader.Close()
					return nil, errors.Newf("page catalog is not sorted at WARC index %d offset %d", row.WarcIndex, row.WarcRecordOffset)
				}
				havePrevious = true
				previous = row

				warc, exists := warcs[row.WarcIndex]
				if !exists {
					_ = reader.Close()
					return nil, errors.Newf("page references unknown WARC index %d", row.WarcIndex)
				}
				if row.RootDomain == "" || row.URL == "" {
					_ = reader.Close()
					return nil, errors.Newf("page at WARC index %d offset %d is missing domain or URL", row.WarcIndex, row.WarcRecordOffset)
				}
				if row.DomainPageRank == 0 {
					_ = reader.Close()
					return nil, errors.Newf("page at WARC index %d offset %d has rank 0", row.WarcIndex, row.WarcRecordOffset)
				}
				if row.WarcRecordLength == 0 || row.WarcRecordOffset > math.MaxInt64 ||
					row.WarcRecordLength > math.MaxInt64 || row.WarcRecordOffset+row.WarcRecordLength-1 > math.MaxInt64 {
					_ = reader.Close()
					return nil, errors.Newf(
						"page has invalid WARC coordinates: index=%d offset=%d length=%d",
						row.WarcIndex, row.WarcRecordOffset, row.WarcRecordLength,
					)
				}
				items = append(items, model.WorklistItem{
					RootDomain:   row.RootDomain,
					URL:          row.URL,
					WarcIndex:    row.WarcIndex,
					WarcFilename: warc.WarcFilename,
					Offset:       int64(row.WarcRecordOffset),
					Length:       int64(row.WarcRecordLength),
					Primary:      row.DomainPageRank == 1,
				})
			}
			if readErr != nil {
				if closeErr := reader.Close(); closeErr != nil {
					return nil, errors.Wrapf(closeErr, "close page catalog row-group %d", groupIndex)
				}
				if readErr == io.EOF {
					break
				}
				return nil, errors.Wrapf(readErr, "read page catalog row-group %d", groupIndex)
			}
		}
	}
	return items, nil
}

func rowGroupOverlaps(rowGroup parquet.RowGroup, warcColumn int, firstWarc, lastWarc uint32) (bool, error) {
	columnIndex, err := rowGroup.ColumnChunks()[warcColumn].ColumnIndex()
	if errors.Is(err, parquet.ErrMissingColumnIndex) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if columnIndex.NumPages() == 0 {
		return rowGroup.NumRows() > 0, nil
	}
	for page := 0; page < columnIndex.NumPages(); page++ {
		minimum := columnIndex.MinValue(page)
		maximum := columnIndex.MaxValue(page)
		if minimum.IsNull() || maximum.IsNull() {
			return true, nil
		}
		if maximum.Uint32() >= firstWarc && minimum.Uint32() <= lastWarc {
			return true, nil
		}
	}
	return false, nil
}

func validateSchema(schema *parquet.Schema, required []requiredColumn) error {
	for _, column := range required {
		leaf, exists := schema.Lookup(column.name)
		if !exists {
			return errors.Newf("missing required column %q", column.name)
		}
		if err := validateColumn(leaf, column.name, column.typeName); err != nil {
			return err
		}
	}
	return nil
}

func validateColumn(column parquet.LeafColumn, name, want string) error {
	if column.Node.Repeated() {
		return errors.Newf("column %q is repeated", name)
	}
	got := parquetTypeName(column.Node)
	if got != want {
		return errors.Newf("column %q has type %s, want %s", name, got, want)
	}
	return nil
}

func parquetTypeName(node parquet.Node) string {
	if logical := node.Type().LogicalType(); logical != nil {
		if logical.Integer != nil {
			prefix := "uint"
			if logical.Integer.IsSigned {
				prefix = "int"
			}
			return fmt.Sprintf("%s%d", prefix, logical.Integer.BitWidth)
		}
		if logical.UTF8 != nil {
			return "string"
		}
	}
	goType := node.GoType()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}
	return goType.Kind().String()
}
