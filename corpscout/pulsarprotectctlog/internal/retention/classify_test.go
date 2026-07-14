package retention

import (
	"testing"
	"time"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

func TestKeepMetadata(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
		want      bool
	}{
		{
			name:      "valid cert kept",
			notBefore: now.Add(-30 * 24 * time.Hour),
			notAfter:  now.Add(60 * 24 * time.Hour),
			want:      true,
		},
		{
			name:      "issuance age is irrelevant while the TTL would retain it",
			notBefore: now.Add(-5 * 365 * 24 * time.Hour),
			notAfter:  now.Add(30 * 24 * time.Hour),
			want:      true,
		},
		{
			name:      "expired but inside the 90-day TTL grace kept",
			notBefore: now.Add(-200 * 24 * time.Hour),
			notAfter:  now.Add(-89 * 24 * time.Hour),
			want:      true,
		},
		{
			name:      "expired exactly 90 days ago dropped (TTL boundary)",
			notBefore: now.Add(-200 * 24 * time.Hour),
			notAfter:  now.Add(-metadataTTLGrace),
			want:      false,
		},
		{
			name:      "long-expired dropped (would be TTL-deleted on arrival)",
			notBefore: now.Add(-400 * 24 * time.Hour),
			notAfter:  now.Add(-365 * 24 * time.Hour),
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := model.CertMeta{NotBefore: tt.notBefore, NotAfter: tt.notAfter}
			if got := KeepMetadata(c, now); got != tt.want {
				t.Errorf("KeepMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegisteredDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		host string
		want string
	}{
		{"www.example.com", "example.com"},
		{"a.b.example.com", "example.com"},
		{"*.example.com", "example.com"},
		{"example.co.uk", "example.co.uk"},
		{"sub.example.co.uk", "example.co.uk"},
		{"EXAMPLE.com", "example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()
			host, _ := normalizeHostname(tt.host)
			if got := RegisteredDomain(host); got != tt.want {
				t.Errorf("RegisteredDomain(%q) = %q, want %q", tt.host, got, tt.want)
			}
		})
	}
}

func TestHostnameRowsIgnoresNonHostnameCN(t *testing.T) {
	t.Parallel()
	c := model.CertMeta{
		CommonName: "Some CA Authority X3", // not a hostname
		SANs:       []string{"www.example.com"},
	}
	rows := HostnameRows(c)
	for _, r := range rows {
		if r.FQDN == "some ca authority x3" {
			t.Fatalf("non-hostname CN leaked into hostname rows: %+v", r)
		}
	}
	if len(rows) != 1 || rows[0].FQDN != "www.example.com" {
		t.Fatalf("rows = %+v, want only www.example.com", rows)
	}
}

func TestHostnameRows(t *testing.T) {
	t.Parallel()
	sct := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	notAfter := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := model.CertMeta{
		CommonName:   "www.example.com",
		SANs:         []string{"www.example.com", "*.example.com", "api.example.com", "WWW.EXAMPLE.COM"},
		IssuerCAID:   "ca1",
		SerialNumber: "deadbeef",
		SCTTimestamp: sct,
		NotAfter:     notAfter,
		LogName:      "test-log",
	}

	rows := HostnameRows(c)

	// www.example.com appears 3x (incl. CN and uppercase) -> deduped to one.
	if len(rows) != 3 {
		t.Fatalf("expected 3 unique hostname rows, got %d: %+v", len(rows), rows)
	}

	var wildcardFound bool
	for _, r := range rows {
		if r.RegisteredDomain != "example.com" {
			t.Errorf("row %q: registered domain = %q, want example.com", r.FQDN, r.RegisteredDomain)
		}
		if !r.FirstSeen.Equal(sct) || !r.LastSeen.Equal(sct) {
			t.Errorf("row %q: first/last seen = %v/%v, want both %v", r.FQDN, r.FirstSeen, r.LastSeen, sct)
		}
		if !r.LastNotAfter.Equal(notAfter) {
			t.Errorf("row %q: last_not_after = %v, want %v", r.FQDN, r.LastNotAfter, notAfter)
		}
		if r.SourceLog != "test-log" {
			t.Errorf("row %q: source log = %q, want test-log", r.FQDN, r.SourceLog)
		}
		if r.FQDN == "*.example.com" {
			wildcardFound = true
			if !r.IsWildcard {
				t.Errorf("wildcard row not flagged as wildcard")
			}
		}
	}
	if !wildcardFound {
		t.Errorf("wildcard hostname row missing")
	}
}
