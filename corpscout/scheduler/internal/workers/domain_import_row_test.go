package workers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDomainImportCSVRow(t *testing.T) {
	tests := []struct {
		name string
		row  []string
		want domainImportCSVRow
	}{
		{
			name: "short row is invalid",
			row:  []string{"1"},
			want: domainImportCSVRow{Status: domainImportCSVRowStatusInvalid},
		},
		{
			name: "blank domain is skipped",
			row:  []string{"1", "   "},
			want: domainImportCSVRow{Status: domainImportCSVRowStatusSkipped},
		},
		{
			name: "normalizes domain and company name",
			row:  []string{"1", " Example.COM ", "  Acme Corp  "},
			want: domainImportCSVRow{Domain: "example.com", CompanyName: "Acme Corp", Status: domainImportCSVRowStatusValid},
		},
		{
			name: "missing company name is valid",
			row:  []string{"1", "example.org"},
			want: domainImportCSVRow{Domain: "example.org", Status: domainImportCSVRowStatusValid},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseDomainImportCSVRow(tt.row))
		})
	}
}
