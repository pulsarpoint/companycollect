package hosts

import "testing"

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{
		"NS1.Binero.SE.":   "ns1.binero.se",
		" mx.example.com ": "mx.example.com",
		".":                "",
	} {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRegistrable(t *testing.T) {
	for in, want := range map[string]string{
		"ns1.binero.se.":         "binero.se",
		"mx1.example.co.uk":      "example.co.uk",
		"ns-12.awsdns-01.co.uk.": "awsdns-01.co.uk",
		"foo.bar.github.io":      "bar.github.io",
		"aspmx.l.google.com":     "google.com",
		"com":                    "",
		"192.0.2.1":              "",
		"":                       "",
	} {
		if got := Registrable(in); got != want {
			t.Errorf("Registrable(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUnder(t *testing.T) {
	if !Under("mail.example.se.", "example.se") || !Under("example.se", "EXAMPLE.se") {
		t.Fatal("host under its domain not recognised")
	}
	if Under("badexample.se", "example.se") {
		t.Fatal("suffix without a dot boundary counted as under")
	}
}
