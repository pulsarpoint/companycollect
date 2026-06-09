package prhytj

import (
	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
)

const maxSnapshotLineBytes = 32 * 1024 * 1024

func processContextError(err error, sourcePath string) error {
	return countryimport.WrapSourceError(
		countryimport.Classify(err),
		SourceSlug,
		"",
		sourcePath,
		0,
		errors.Wrap(err, "process PRH source file"),
	)
}
