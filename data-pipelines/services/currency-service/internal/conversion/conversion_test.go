package conversion_test

import (
	"fmt"
	"testing"

	"github.com/pulsarpoint/currency-service/internal/conversion"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func d(s string) decimal.Decimal {
	v, err := decimal.NewFromString(s)
	if err != nil {
		panic(fmt.Sprintf("invalid decimal literal %q: %v", s, err))
	}
	return v
}

func TestConvertIdentity(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	rate, result, err := conversion.Convert(d("100.00"), "USD", "USD", eurPer)
	require.NoError(t, err)
	require.True(t, rate.Equal(decimal.NewFromInt(1)), "rate should be 1")
	require.True(t, result.Equal(d("100.00")), "converted should equal input")
}

func TestConvertNOKtoUSD(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	// NOK→USD rate = 1.09 / 11.50 = 0.094782608...
	// 100 NOK * rate = 9.478260869...
	rate, result, err := conversion.Convert(d("100.00"), "NOK", "USD", eurPer)
	require.NoError(t, err)
	require.False(t, rate.IsZero())
	// Round to 2 minor units for USD → 9.48
	rounded := conversion.RoundToMinorUnits(result, 2)
	require.Equal(t, "9.48", rounded.String())
}

func TestConvertUSDtoNOK(t *testing.T) {
	eurPer := map[string]decimal.Decimal{
		"EUR": decimal.NewFromInt(1),
		"USD": d("1.09"),
		"NOK": d("11.50"),
	}
	// USD→NOK rate = 11.50 / 1.09 = 10.5504587...
	// 100 USD → 1055.04587... NOK → rounded to 2 dp = 1055.05
	_, result, err := conversion.Convert(d("100.00"), "USD", "NOK", eurPer)
	require.NoError(t, err)
	rounded := conversion.RoundToMinorUnits(result, 2)
	require.Equal(t, "1055.05", rounded.String())
}

func TestConvertUnknownSourceCurrency(t *testing.T) {
	eurPer := map[string]decimal.Decimal{"EUR": decimal.NewFromInt(1), "USD": d("1.09")}
	_, _, err := conversion.Convert(d("100.00"), "XYZ", "USD", eurPer)
	require.Error(t, err)
}

func TestConvertUnknownTargetCurrency(t *testing.T) {
	eurPer := map[string]decimal.Decimal{"EUR": decimal.NewFromInt(1), "NOK": d("11.50")}
	_, _, err := conversion.Convert(d("100.00"), "NOK", "ABC", eurPer)
	require.Error(t, err)
}

func TestToMinorUnits(t *testing.T) {
	require.Equal(t, int64(948), conversion.ToMinorUnits(d("9.48"), 2))
	require.Equal(t, int64(9), conversion.ToMinorUnits(d("9.48"), 0))
	require.Equal(t, int64(10), conversion.ToMinorUnits(d("9.5"), 0)) // half-up: rounds up
	require.Equal(t, int64(9478), conversion.ToMinorUnits(d("9.478"), 3))
}
