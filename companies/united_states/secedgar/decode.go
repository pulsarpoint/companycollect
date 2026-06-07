package secedgar

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func DecodeCompanyTickers(data []byte) ([]CompanyTickerRecord, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, errors.New("decode SEC EDGAR company tickers: empty payload")
	}
	if !strings.HasPrefix(trimmed, "{") {
		return nil, errors.New("decode SEC EDGAR company tickers: expected top-level object")
	}

	var keyed map[string]json.RawMessage
	if err := json.Unmarshal(data, &keyed); err != nil {
		return nil, errors.Wrap(err, "decode SEC EDGAR company tickers")
	}

	entries := make([]companyTickerEntry, 0, len(keyed))
	seenIndexes := make(map[int]string, len(keyed))
	for key, raw := range keyed {
		index, err := strconv.Atoi(key)
		if err != nil {
			return nil, errors.Wrapf(err, "decode SEC EDGAR company tickers key %q", key)
		}
		if key != strconv.Itoa(index) {
			return nil, errors.Errorf("decode SEC EDGAR company tickers key %q: non-canonical numeric key", key)
		}
		if previousKey, exists := seenIndexes[index]; exists {
			return nil, errors.Errorf(
				"decode SEC EDGAR company tickers key %q: duplicate numeric index %d previously seen as key %q",
				key,
				index,
				previousKey,
			)
		}
		seenIndexes[index] = key
		entries = append(entries, companyTickerEntry{key: key, index: index, raw: raw})
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].index < entries[right].index
	})

	records := make([]CompanyTickerRecord, 0, len(entries))
	for _, entry := range entries {
		raw := entry.raw
		if !isJSONObject(raw) {
			return nil, errors.Errorf("decode SEC EDGAR company ticker record %d: expected object", entry.index)
		}

		var payload companyTickerPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, errors.Wrapf(err, "decode SEC EDGAR company ticker record %d", entry.index)
		}
		if err := validateCompanyTickerPayload(payload); err != nil {
			return nil, errors.Wrapf(err, "decode SEC EDGAR company ticker record %d", entry.index)
		}

		rawCopy := append(json.RawMessage(nil), raw...)
		sum := sha256.Sum256(rawCopy)
		records = append(records, CompanyTickerRecord{
			SourceKey:   SourceKey,
			SourceIndex: entry.index,
			CIK:         payload.CIK,
			CIKString:   strconv.Itoa(payload.CIK),
			CIK10:       fmt.Sprintf("%010d", payload.CIK),
			Ticker:      payload.Ticker,
			Title:       payload.Title,
			RawPayload:  rawCopy,
			PayloadHash: hex.EncodeToString(sum[:]),
		})
	}

	return records, nil
}

type companyTickerEntry struct {
	key   string
	index int
	raw   json.RawMessage
}

func isJSONObject(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return strings.HasPrefix(trimmed, "{")
}

func validateCompanyTickerPayload(payload companyTickerPayload) error {
	if payload.CIK <= 0 {
		return errors.New("missing or zero cik_str")
	}
	if strings.TrimSpace(payload.Ticker) == "" {
		return errors.New("empty ticker")
	}
	if strings.TrimSpace(payload.Title) == "" {
		return errors.New("empty title")
	}
	return nil
}
