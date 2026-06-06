package secedgar

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/cockroachdb/errors"
)

const (
	SourceSlug = "united_states_sec_edgar"
	SourceName = "SEC EDGAR company_tickers.json"
	// DefaultDownloadURL is the bulk ticker map: a single JSON object keyed by
	// sequential integer index, each value one company {cik_str,ticker,title}.
	DefaultDownloadURL = "https://www.sec.gov/files/company_tickers.json"
)

// SecTickerRecord is one company entry from company_tickers.json. cik_str is the
// SEC Central Index Key as a bare integer; zero-pad it to 10 digits for the
// canonical CIK identifier (see CompanyProfile).
type SecTickerRecord struct {
	CIKStr int64  `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`

	// RawPayload is the exact source NDJSON line the record was decoded from and
	// PayloadHash is its SHA-256, both populated by Process for raw storage. They
	// are not part of the source JSON.
	RawPayload  []byte `json:"-"`
	PayloadHash string `json:"-"`
}

// tickerFile is the company_tickers.json top-level object. The keys are
// sequential integer indices ("0","1",...) with no semantic meaning; each value
// is one company. We retain the raw value bytes so the snapshot preserves any
// extra source fields rather than only the three known ones.
type tickerFile struct {
	Records []json.RawMessage
}

func (f *tickerFile) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("company_tickers.json must be a JSON object keyed by index")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return err
	}

	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return tickerKeyLess(keys[i], keys[j]) })

	f.Records = make([]json.RawMessage, 0, len(raw))
	for _, key := range keys {
		f.Records = append(f.Records, raw[key])
	}
	return nil
}

// tickerKeyLess orders the index keys numerically when both parse as integers so
// the flattened snapshot is stable, falling back to lexical order otherwise.
func tickerKeyLess(a, b string) bool {
	ai, aerr := strconv.Atoi(a)
	bi, berr := strconv.Atoi(b)
	if aerr == nil && berr == nil {
		return ai < bi
	}
	return a < b
}

// ParseTickerFile decodes company_tickers.json into typed records ordered by
// index. Useful for mapping and validation; Download preserves raw value bytes.
func ParseTickerFile(data []byte) ([]SecTickerRecord, error) {
	var file tickerFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, err
	}

	records := make([]SecTickerRecord, 0, len(file.Records))
	for _, raw := range file.Records {
		var record SecTickerRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}
