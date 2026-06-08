package coloradoentities

import (
	"context"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// Store validates and counts typed records. When no store callback is
// configured it simply counts, so parser and fixture tests run without a
// database. It never imports Corpscout, sqlc, or scheduler types.
func (s *Source) Store(ctx context.Context, records []ColoradoEntityRecord) (countryimport.StoreResult, error) {
	if s != nil && s.storeFunc != nil {
		return s.storeFunc(ctx, records)
	}
	if err := ctx.Err(); err != nil {
		return countryimport.StoreResult{}, countryimport.WrapSourceError(
			countryimport.Classify(err), SourceSlug, "", "", 0,
			errors.Wrap(err, "store Colorado records"),
		)
	}

	count := int64(len(records))
	return countryimport.StoreResult{
		RecordsReceived: count,
		RecordsStored:   count,
	}, nil
}
