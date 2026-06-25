package main

import (
	"regexp"
	"strings"

	"cc-enrich-worker/internal/model"
)

// vatFormats: the VIES format of the VAT number *after* the 2-letter country code.
var vatFormats = map[string]*regexp.Regexp{
	"AT": regexp.MustCompile(`^U\d{8}$`),
	"BE": regexp.MustCompile(`^[01]\d{9}$`),
	"BG": regexp.MustCompile(`^\d{9,10}$`),
	"CY": regexp.MustCompile(`^\d{8}[A-Z]$`),
	"CZ": regexp.MustCompile(`^\d{8,10}$`),
	"DE": regexp.MustCompile(`^\d{9}$`),
	"DK": regexp.MustCompile(`^\d{8}$`),
	"EE": regexp.MustCompile(`^\d{9}$`),
	"EL": regexp.MustCompile(`^\d{9}$`),
	"ES": regexp.MustCompile(`^[A-Z0-9]\d{7}[A-Z0-9]$`),
	"FI": regexp.MustCompile(`^\d{8}$`),
	"FR": regexp.MustCompile(`^[A-Z0-9]{2}\d{9}$`),
	"GB": regexp.MustCompile(`^(\d{9}|\d{12}|(GD|HA)\d{3})$`),
	"HR": regexp.MustCompile(`^\d{11}$`),
	"HU": regexp.MustCompile(`^\d{8}$`),
	"IE": regexp.MustCompile(`^(\d{7}[A-W]|[7-9][A-Z*+]\d{5}[A-W]|\d{7}[A-W][AH])$`),
	"IT": regexp.MustCompile(`^\d{11}$`),
	"LT": regexp.MustCompile(`^(\d{9}|\d{12})$`),
	"LU": regexp.MustCompile(`^\d{8}$`),
	"LV": regexp.MustCompile(`^\d{11}$`),
	"MT": regexp.MustCompile(`^\d{8}$`),
	"NL": regexp.MustCompile(`^\d{9}B\d{2}$`),
	"PL": regexp.MustCompile(`^\d{10}$`),
	"PT": regexp.MustCompile(`^\d{9}$`),
	"RO": regexp.MustCompile(`^\d{2,10}$`),
	"SE": regexp.MustCompile(`^\d{12}$`),
	"SI": regexp.MustCompile(`^\d{8}$`),
	"SK": regexp.MustCompile(`^\d{10}$`),
}

// only tokens beginning with a real EU country code are even candidates (precise + fast).
var vatCandidateRe = func() *regexp.Regexp {
	codes := make([]string, 0, len(vatFormats))
	for c := range vatFormats {
		codes = append(codes, c)
	}
	return regexp.MustCompile(`\b(` + strings.Join(codes, "|") + `)([A-Z0-9]{2,13})\b`)
}()

// validVAT validates a full EU VAT id (country code + number): the per-country format
// always, plus a checksum for the countries where one is implemented (DE, IT).
func validVAT(s string) bool {
	s = strings.ToUpper(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	if len(s) < 4 {
		return false
	}
	cc, num := s[:2], s[2:]
	re, ok := vatFormats[cc]
	if !ok || !re.MatchString(num) {
		return false
	}
	switch cc {
	case "DE":
		return vatCheckDE(num)
	case "IT":
		return luhn(num)
	}
	return true // format-validated (no checksum implemented for this country)
}

// German USt-IdNr checksum (ISO 7064 MOD 11,10).
func vatCheckDE(num string) bool {
	if len(num) != 9 {
		return false
	}
	product := 10
	for i := 0; i < 8; i++ {
		sum := (int(num[i]-'0') + product) % 10
		if sum == 0 {
			sum = 10
		}
		product = (sum * 2) % 11
	}
	check := 11 - product
	if check == 10 {
		check = 0
	}
	return check == int(num[8]-'0')
}

// luhn validates a digit string by the Luhn (mod-10) algorithm (Italian VAT).
func luhn(num string) bool {
	sum, alt := 0, false
	for i := len(num) - 1; i >= 0; i-- {
		d := int(num[i] - '0')
		if d < 0 || d > 9 {
			return false
		}
		if alt {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
	}
	return sum%10 == 0
}

// ExtractVATs finds checksum/format-valid EU VAT ids in page text.
func ExtractVATs(body []byte) []model.Identifier {
	seen := map[string]bool{}
	var out []model.Identifier
	for _, m := range vatCandidateRe.FindAllSubmatch(body, -1) {
		full := string(m[1]) + string(m[2])
		if seen[full] || !validVAT(full) {
			continue
		}
		seen[full] = true
		out = append(out, model.Identifier{Type: "vat", Value: full, Valid: true, Source: "text"})
	}
	return out
}
