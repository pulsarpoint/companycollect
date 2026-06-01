package conversion

import "github.com/bojanz/currency"

// MinorUnits returns the number of minor units for an ISO 4217 currency code.
// Returns false if the currency is not recognised.
// Delegates to bojanz/currency which uses CLDR data (handles ISK=0, KWD=3, JPY=0, etc.).
func MinorUnits(code string) (int, bool) {
	d, ok := currency.GetDigits(code)
	return int(d), ok
}
