package extract

import (
	"strings"

	"github.com/nyaruka/phonenumbers"
)

// ccTLDRegionOverrides maps ccTLDs whose ISO 3166-1 region code differs from the TLD itself.
var ccTLDRegionOverrides = map[string]string{"uk": "GB"}

// NormalizePhone returns the E.164 form of a phone found on a page when it parses as a valid
// number, using the domain's ccTLD as the region hint (so national formats on .de sites parse
// as DE; generic TLDs get no hint, so only +-prefixed numbers parse). E.164 dedupes the many
// formatting variants of one number across pages. Anything unparseable or invalid is returned
// trimmed, never dropped.
func NormalizePhone(raw, rootDomain string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return trimmed
	}
	region := ""
	if i := strings.LastIndexByte(rootDomain, '.'); i >= 0 {
		if tld := strings.ToLower(rootDomain[i+1:]); len(tld) == 2 {
			if r, ok := ccTLDRegionOverrides[tld]; ok {
				region = r
			} else {
				region = strings.ToUpper(tld)
			}
		}
	}
	num, err := phonenumbers.Parse(trimmed, region)
	if err != nil || !phonenumbers.IsValidNumber(num) {
		return trimmed
	}
	return phonenumbers.Format(num, phonenumbers.E164)
}
