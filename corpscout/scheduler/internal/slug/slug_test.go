package slug_test

import (
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/slug"
)

func TestGenerate(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "Apache Software Foundation", "apache-software-foundation"},
		{"ampersand", "Johnson & Johnson", "johnson-and-johnson"},
		{"special chars", "F5, Inc.", "f5-inc"},
		{"accented", "Société Générale", "societe-generale"},
		{"multiple spaces", "  CNCF  ", "cncf"},
		{"numbers", "3Com", "3com"},
		{"leading trailing hyphens", "---foo---", "foo"},
		{"unicode symbol", "café©", "cafe"},
		{"empty after strip", "---", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := slug.Generate(tc.input)
			if got != tc.want {
				t.Errorf("Generate(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
