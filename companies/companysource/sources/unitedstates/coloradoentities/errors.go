package coloradoentities

import (
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

const maxSnapshotLineBytes = 8 * 1024 * 1024

func processContextError(err error, sourcePath string) error {
	return countryimport.WrapSourceError(
		countryimport.Classify(err),
		SourceSlug,
		"",
		sourcePath,
		0,
		errors.Wrap(err, "process Colorado source file"),
	)
}

func resolveDuration(override time.Duration, configured time.Duration, fallback time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	if configured > 0 {
		return configured
	}
	return fallback
}

func resolveString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
