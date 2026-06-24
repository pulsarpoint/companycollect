package main

import (
	"regexp"
	"strings"
)

// Identifier is a company identifier scraped from a page that links the domain to an
// authoritative registry (e.g. an LEI → GLEIF). Extensible to VAT / registration numbers.
type Identifier struct {
	Type   string // "lei"
	Value  string
	Valid  bool   // checksum/format validated
	Source string // "jsonld" | "text"
}

// A 20-char LEI: 18 alphanumeric + 2 check digits (ISO 17442).
var leiTokenRe = regexp.MustCompile(`[A-Z0-9]{18}[0-9]{2}`)

// schema.org JSON-LD: "leiCode":"...". Keyed case-insensitively, value 20 alphanumerics.
var leiCodeRe = regexp.MustCompile(`(?i)"lei(?:Code)?"\s*:\s*"([A-Za-z0-9]{20})"`)

// validLEI checks the ISO 7064 MOD 97-10 checksum: map each char to a number
// (0-9 → 0-9, A-Z → 10-35), interpret as one big integer, valid iff mod 97 == 1.
func validLEI(s string) bool {
	if len(s) != 20 {
		return false
	}
	rem := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			rem = (rem*10 + int(c-'0')) % 97
		case c >= 'A' && c <= 'Z':
			rem = (rem*100 + int(c-'A') + 10) % 97 // letters are two-digit values
		default:
			return false
		}
	}
	return rem == 1
}

// ExtractLEIs finds LEIs on a page. JSON-LD leiCode values are kept even if the checksum
// fails (an explicitly-claimed LEI — flagged invalid); bare text tokens are kept only when
// the checksum validates (no label, so the checksum is what makes us confident it's an LEI).
func ExtractLEIs(body []byte) []Identifier {
	found := map[string]Identifier{}

	for _, m := range leiCodeRe.FindAllSubmatch(body, -1) {
		v := strings.ToUpper(string(m[1]))
		if _, ok := found[v]; !ok {
			found[v] = Identifier{Type: "lei", Value: v, Valid: validLEI(v), Source: "jsonld"}
		}
	}
	for _, m := range leiTokenRe.FindAll(body, -1) {
		v := string(m)
		if !validLEI(v) {
			continue
		}
		if _, ok := found[v]; !ok {
			found[v] = Identifier{Type: "lei", Value: v, Valid: true, Source: "text"}
		}
	}

	out := make([]Identifier, 0, len(found))
	for _, id := range found {
		out = append(out, id)
	}
	return out
}
