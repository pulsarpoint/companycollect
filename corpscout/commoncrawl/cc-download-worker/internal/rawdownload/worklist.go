package rawdownload

import (
	"strings"

	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

type worklistRow struct {
	RootDomain       string  `parquet:"root_domain"`
	URL              string  `parquet:"url"`
	WARCFilename     string  `parquet:"warc_filename"`
	WARCRecordOffset int64   `parquet:"warc_record_offset"`
	WARCRecordLength int64   `parquet:"warc_record_length"`
	ContentLanguages *string `parquet:"content_languages,optional"`
}

type selectedRecord struct {
	worklistRow
	Ordinal    int64
	DomainRank int64
	Primary    bool
}

type chunkPlan struct {
	Number  int
	Records []selectedRecord
}

type worklistData struct {
	Key      string
	Size     int64
	Checksum rawstore.SHA256
	Records  []selectedRecord
}

func readWorklist(path, key string) (worklistData, error) {
	checksum, size, err := rawstore.ChecksumFile(path)
	if err != nil {
		return worklistData{}, err
	}
	rows, err := parquet.ReadFile[worklistRow](path)
	if err != nil {
		return worklistData{}, errors.Wrapf(err, "read worklist %s", path)
	}
	if len(rows) == 0 {
		return worklistData{}, errors.New("worklist is empty")
	}

	records := make([]selectedRecord, 0, len(rows))
	seenDomains := make(map[string]struct{})
	var previousDomain string
	var domainRank int64 = -1
	for index, row := range rows {
		if strings.TrimSpace(row.RootDomain) == "" || strings.TrimSpace(row.URL) == "" || strings.TrimSpace(row.WARCFilename) == "" {
			return worklistData{}, errors.Newf("worklist row %d is missing domain, URL, or WARC filename", index)
		}
		if row.WARCRecordOffset < 0 || row.WARCRecordLength <= 0 {
			return worklistData{}, errors.Newf("worklist row %d has invalid WARC range offset=%d length=%d", index, row.WARCRecordOffset, row.WARCRecordLength)
		}
		primary := row.RootDomain != previousDomain
		if primary {
			if _, exists := seenDomains[row.RootDomain]; exists {
				return worklistData{}, errors.Newf("worklist domain %q is not contiguous", row.RootDomain)
			}
			seenDomains[row.RootDomain] = struct{}{}
			domainRank++
			previousDomain = row.RootDomain
		}
		records = append(records, selectedRecord{
			worklistRow: row,
			Ordinal:     int64(index),
			DomainRank:  domainRank,
			Primary:     primary,
		})
	}
	return worklistData{Key: key, Size: size, Checksum: checksum, Records: records}, nil
}

func planChunks(records []selectedRecord, maxPackBytes int64, maxRecords int) []chunkPlan {
	chunks := make([]chunkPlan, 0)
	start := 0
	var bytes int64
	for index, record := range records {
		wouldExceedBytes := index > start && bytes+record.WARCRecordLength > maxPackBytes
		wouldExceedRecords := index > start && index-start >= maxRecords
		if wouldExceedBytes || wouldExceedRecords {
			chunks = append(chunks, chunkPlan{Number: len(chunks), Records: records[start:index]})
			start = index
			bytes = 0
		}
		bytes += record.WARCRecordLength
	}
	if start < len(records) {
		chunks = append(chunks, chunkPlan{Number: len(chunks), Records: records[start:]})
	}
	return chunks
}
