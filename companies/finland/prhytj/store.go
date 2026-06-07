package prhytj

import (
	"context"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Store(ctx context.Context, records []CompanyRecord) (countryimport.StoreResult, error) {
	if s != nil && s.StoreFunc != nil {
		return s.StoreFunc(ctx, records)
	}

	count := int64(len(records))
	return countryimport.StoreResult{
		RecordsReceived: count,
		RecordsStored:   count,
	}, nil
}
