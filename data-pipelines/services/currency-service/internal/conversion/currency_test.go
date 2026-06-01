package conversion_test

import (
	"testing"

	"github.com/pulsarpoint/currency-service/internal/conversion"
	"github.com/stretchr/testify/require"
)

func TestMinorUnitsKnownCurrencies(t *testing.T) {
	cases := []struct {
		currency string
		want     int
	}{
		{"USD", 2},
		{"EUR", 2},
		{"NOK", 2},
		{"GBP", 2},
		{"JPY", 0},
		{"KRW", 0},
		{"ISK", 0}, // Icelandic króna — no subunit in use; CLDR=0, not 2
		{"KWD", 3},
		{"BHD", 3},
		{"OMR", 3},
	}
	for _, tc := range cases {
		t.Run(tc.currency, func(t *testing.T) {
			got, ok := conversion.MinorUnits(tc.currency)
			require.True(t, ok, "currency %s should be known", tc.currency)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMinorUnitsUnknownCurrency(t *testing.T) {
	_, ok := conversion.MinorUnits("XYZ")
	require.False(t, ok)
}
