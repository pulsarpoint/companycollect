package secedgar

import (
	"context"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func (s *Source) Store(ctx context.Context, records []SecTickerRecord) (countryimport.StoreResult, error) {
	if s != nil && s.StoreFunc != nil {
		return s.StoreFunc(ctx, records)
	}

	count := int64(len(records))
	return countryimport.StoreResult{
		RecordsReceived: count,
		RecordsStored:   count,
	}, nil
}
