package secedgar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func ParseSnapshot(ctx context.Context, path string, handle func(CompanyTickerRecord) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "read SEC EDGAR company tickers snapshot")
	}
	records, err := decodeCompanyTickers(data)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handle(record); err != nil {
			return err
		}
	}
	return nil
}

func decodeCompanyTickers(data []byte) ([]CompanyTickerRecord, error) {
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
	for key, raw := range keyed {
		index, err := strconv.Atoi(key)
		if err != nil {
			return nil, errors.Wrapf(err, "decode SEC EDGAR company tickers key %q", key)
		}
		entries = append(entries, companyTickerEntry{key: key, index: index, raw: raw})
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].index < entries[right].index
	})

	records := make([]CompanyTickerRecord, 0, len(entries))
	for _, entry := range entries {
		var payload companyTickerPayload
		if err := json.Unmarshal(entry.raw, &payload); err != nil {
			return nil, errors.Wrapf(err, "decode SEC EDGAR company ticker record %d", entry.index)
		}
		if payload.CIK <= 0 {
			return nil, errors.Errorf("decode SEC EDGAR company ticker record %d: missing or zero cik_str", entry.index)
		}
		if strings.TrimSpace(payload.Ticker) == "" {
			return nil, errors.Errorf("decode SEC EDGAR company ticker record %d: empty ticker", entry.index)
		}
		if strings.TrimSpace(payload.Title) == "" {
			return nil, errors.Errorf("decode SEC EDGAR company ticker record %d: empty title", entry.index)
		}

		raw := append(json.RawMessage(nil), entry.raw...)
		sum := sha256.Sum256(raw)
		records = append(records, CompanyTickerRecord{
			SourceKey:   SourceKey,
			SourceIndex: entry.index,
			CIK:         payload.CIK,
			CIKString:   strconv.Itoa(payload.CIK),
			CIK10:       fmt.Sprintf("%010d", payload.CIK),
			Ticker:      strings.TrimSpace(payload.Ticker),
			Title:       strings.TrimSpace(payload.Title),
			RawPayload:  raw,
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
