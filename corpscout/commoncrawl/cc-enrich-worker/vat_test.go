package main

import "testing"

func TestValidVAT(t *testing.T) {
	valid := []string{
		"DE000000003",   // hand-computed valid German checksum (8 zeros -> check digit 3)
		"IT00000000000", // Luhn(11 zeros) = 0
		"PT123456789",   // format-only country (9 digits) — no checksum implemented
		"NL123456789B01",
		"DE 000 000 003", // spaces tolerated
	}
	for _, v := range valid {
		if !validVAT(v) {
			t.Errorf("expected %q to be valid", v)
		}
	}
	invalid := []string{
		"DE000000004",   // checksum off by one
		"IT00000000001", // Luhn fails
		"XX123456789",   // unknown country
		"DE12345678",    // 8 digits — format fail
		"FRABC",         // too short for FR format
		"",
	}
	for _, v := range invalid {
		if validVAT(v) {
			t.Errorf("expected %q to be invalid", v)
		}
	}
}

func TestExtractVATs(t *testing.T) {
	body := []byte(`<footer>USt-IdNr: DE000000003 — registered. FRAME and DEFAULT are not VATs.</footer>`)
	ids := ExtractVATs(body)
	if len(ids) != 1 || ids[0].Type != "vat" || ids[0].Value != "DE000000003" || !ids[0].Valid {
		t.Fatalf("vat extraction wrong: %+v", ids)
	}
}
