package secedgar

import (
	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
)

func secContextError(err error, sourcePath string) error {
	return countryimport.WrapSourceError(
		countryimport.Classify(err),
		SourceSlug,
		"",
		sourcePath,
		0,
		errors.Wrap(err, "process SEC EDGAR source file"),
	)
}
