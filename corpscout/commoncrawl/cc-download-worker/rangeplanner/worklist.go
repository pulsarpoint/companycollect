package rangeplanner

import (
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

type worklistRow struct {
	RootDomain       string `parquet:"root_domain"`
	URL              string `parquet:"url"`
	WARCFilename     string `parquet:"warc_filename"`
	WARCRecordOffset int64  `parquet:"warc_record_offset"`
	WARCRecordLength int64  `parquet:"warc_record_length"`
}

type Worklist struct {
	Records      []Record
	OutputChunks [][]Record
	WARCFiles    []string
}

func ReadWorklist(path string, maxPackBytes int64, maxRecords int) (Worklist, error) {
	if maxPackBytes <= 0 || maxRecords <= 0 {
		return Worklist{}, errors.New("pack byte and record limits must be positive")
	}
	rows, err := parquet.ReadFile[worklistRow](path)
	if err != nil {
		return Worklist{}, errors.Wrapf(err, "read worklist %s", path)
	}
	if len(rows) == 0 {
		return Worklist{}, errors.New("worklist is empty")
	}

	records := make([]Record, len(rows))
	warcFiles := make(map[string]struct{})
	for index, row := range rows {
		if strings.TrimSpace(row.RootDomain) == "" || strings.TrimSpace(row.URL) == "" {
			return Worklist{}, errors.Newf("worklist row %d is missing domain or URL", index)
		}
		record := Record{
			ID:       int64(index),
			WARCFile: row.WARCFilename,
			Offset:   row.WARCRecordOffset,
			Length:   row.WARCRecordLength,
		}
		if err := validateRecord(record); err != nil {
			return Worklist{}, errors.Wrapf(err, "worklist row %d", index)
		}
		records[index] = record
		warcFiles[record.WARCFile] = struct{}{}
	}
	filenames := make([]string, 0, len(warcFiles))
	for filename := range warcFiles {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)
	return Worklist{
		Records:      records,
		OutputChunks: chunkRecords(records, maxPackBytes, maxRecords),
		WARCFiles:    filenames,
	}, nil
}

func chunkRecords(records []Record, maxPackBytes int64, maxRecords int) [][]Record {
	chunks := make([][]Record, 0)
	start := 0
	var bytes int64
	for index, record := range records {
		if index > start && (bytes+record.Length > maxPackBytes || index-start >= maxRecords) {
			chunks = append(chunks, records[start:index])
			start = index
			bytes = 0
		}
		bytes += record.Length
	}
	if start < len(records) {
		chunks = append(chunks, records[start:])
	}
	return chunks
}
