package irseobmf

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// utf8BOM is the UTF-8 byte order mark that can prefix the first CSV header
// cell. It is stripped before header comparison.
const utf8BOM = "\ufeff"

// validateHeader confirms the remote CSV header matches the documented EO BMF
// column set exactly (order and names). Header validation catches silent format
// changes before any row is converted.
func validateHeader(header []string) error {
	if len(header) != len(csvColumns) {
		return errors.Newf("unexpected EO BMF header width %d, want %d", len(header), len(csvColumns))
	}
	for i, column := range csvColumns {
		got := strings.TrimSpace(strings.TrimPrefix(header[i], utf8BOM))
		if !strings.EqualFold(got, column) {
			return errors.Newf("unexpected EO BMF header column %d %q, want %q", i, got, column)
		}
	}
	return nil
}

// rowToRecord converts a single CSV row (already matching the 28-column shape)
// into a source-native record. EIN is normalized to a 9-character zero-padded
// string; all other fields are trimmed of surrounding whitespace.
func rowToRecord(row []string) (IrsEoBmfRecord, error) {
	if len(row) != len(csvColumns) {
		return IrsEoBmfRecord{}, errors.Newf("unexpected EO BMF row width %d, want %d", len(row), len(csvColumns))
	}
	get := func(i int) string { return strings.TrimSpace(strings.TrimPrefix(row[i], utf8BOM)) }

	record := IrsEoBmfRecord{
		EIN:            NormalizeEIN(get(0)),
		Name:           get(1),
		InCareOf:       get(2),
		Street:         get(3),
		City:           get(4),
		State:          get(5),
		Zip:            get(6),
		Group:          get(7),
		Subsection:     get(8),
		Affiliation:    get(9),
		Classification: get(10),
		Ruling:         get(11),
		Deductibility:  get(12),
		Foundation:     get(13),
		Activity:       get(14),
		Organization:   get(15),
		Status:         get(16),
		TaxPeriod:      get(17),
		AssetCD:        get(18),
		IncomeCD:       get(19),
		FilingReqCD:    get(20),
		PFFilingReqCD:  get(21),
		AcctPD:         get(22),
		AssetAmt:       get(23),
		IncomeAmt:      get(24),
		RevenueAmt:     get(25),
		NTEECD:         get(26),
		SortName:       get(27),
	}
	if record.EIN == "" {
		return IrsEoBmfRecord{}, errors.New("missing EIN")
	}
	return record, nil
}

// NormalizeEIN keeps the EIN as an opaque string but left-pads all-digit values
// to nine characters so leading zeros are preserved consistently.
func NormalizeEIN(ein string) string {
	ein = strings.TrimSpace(ein)
	if ein == "" {
		return ""
	}
	if len(ein) < 9 && isAllDigits(ein) {
		return strings.Repeat("0", 9-len(ein)) + ein
	}
	return ein
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// parseAmount parses a whole-dollar financial amount. Blank or non-numeric
// values return present=false rather than a misleading zero.
func parseAmount(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	amount, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return amount, true
}

// isActiveExemptStatus reports whether the EO BMF STATUS code marks an
// unconditionally recognized exempt organization.
func isActiveExemptStatus(status string) bool {
	return strings.TrimSpace(status) == "01"
}
