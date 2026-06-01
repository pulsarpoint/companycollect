package conversion

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Convert returns the exchange rate and converted amount from sourceCurrency to
// targetCurrency using an EUR-anchored rate sheet.
// eurPer[currency] = units of that currency per 1 EUR (ECB convention).
// Both source and target currencies must be present in eurPer.
// For identity conversions (src == tgt) the rate is 1 and no eurPer lookup occurs.
// Returns (rate, convertedAmount, error).
func Convert(
	amount decimal.Decimal,
	sourceCurrency, targetCurrency string,
	eurPer map[string]decimal.Decimal,
) (rate decimal.Decimal, converted decimal.Decimal, err error) {
	if sourceCurrency == targetCurrency {
		return decimal.NewFromInt(1), amount, nil
	}
	srcRate, ok := eurPer[sourceCurrency]
	if !ok {
		return decimal.Zero, decimal.Zero, fmt.Errorf("currency %s not in rate sheet", sourceCurrency)
	}
	tgtRate, ok := eurPer[targetCurrency]
	if !ok {
		return decimal.Zero, decimal.Zero, fmt.Errorf("currency %s not in rate sheet", targetCurrency)
	}
	// rate = units of target per 1 unit of source = tgtRate / srcRate
	rate = tgtRate.Div(srcRate)
	converted = amount.Mul(rate)
	return rate, converted, nil
}

// RoundToMinorUnits rounds amount to the given number of minor units using
// half-up rounding.
func RoundToMinorUnits(amount decimal.Decimal, minorUnit int) decimal.Decimal {
	return amount.Round(int32(minorUnit))
}

// ToMinorUnits converts a decimal amount to an integer minor-unit value using
// half-up rounding. For 0-decimal currencies (e.g. JPY), the amount is rounded
// to the nearest integer (9.5 JPY → 10, 9.48 JPY → 9).
// e.g. 9.48 USD with minorUnit=2 → 948 cents.
func ToMinorUnits(amount decimal.Decimal, minorUnit int) int64 {
	multiplier := decimal.New(1, int32(minorUnit))
	return amount.Mul(multiplier).Round(0).IntPart()
}
