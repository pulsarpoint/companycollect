package extract

import "testing"

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		raw, domain, want string
	}{
		{"+1 650-253-0000", "acme.com", "+16502530000"},  // international: normalized regardless of TLD
		{"020 7946 0958", "acme.uk", "+442079460958"},    // national format + ccTLD hint (.uk -> GB)
		{" +49 (0)30 901820 ", "acme.de", "+4930901820"}, // messy formatting collapses to E.164
		{"call us", "acme.com", "call us"},               // unparseable: kept raw (trimmed)
		{"12345", "acme.com", "12345"},                   // generic TLD, no region hint: kept raw
		{"", "acme.de", ""},
	}
	for _, tt := range tests {
		if got := NormalizePhone(tt.raw, tt.domain); got != tt.want {
			t.Errorf("NormalizePhone(%q, %q) = %q, want %q", tt.raw, tt.domain, got, tt.want)
		}
	}
}
