package secedgar

import (
	"context"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Store(ctx context.Context, records []CompanyTickerRecord) (countryimport.StoreResult, error) {
	if s != nil && s.storeFunc != nil {
		return s.storeFunc(ctx, records)
	}
	if err := ctx.Err(); err != nil {
		return countryimport.StoreResult{}, countryimport.WrapSourceError(
			countryimport.Classify(err),
			SourceSlug,
			"",
			"",
			0,
			errors.Wrap(err, "store SEC EDGAR records"),
		)
	}

	count := int64(len(records))
	return countryimport.StoreResult{
		RecordsReceived: count,
		RecordsStored:   count,
	}, nil
}
