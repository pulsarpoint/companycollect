package hostsource

import (
	"testing"

	"cc-dns-worker/internal/model"
)

func TestNormalizeLabel(t *testing.T) {
	cases := []struct {
		rd, fqdn, want string
		ok             bool
	}{
		{"example.com", "mail.example.com", "mail", true},
		{"example.com", "a.b.example.com", "a.b", true},
		{"example.com", "MAIL.example.com", "mail", true},
		{"example.com", "example.com", "", false},   // apex
		{"example.com", "*.example.com", "", false}, // wildcard
		{"example.com", "other.org", "", false},     // not a subdomain
	}
	for _, c := range cases {
		got, ok := NormalizeLabel(c.rd, c.fqdn)
		if ok != c.ok || got != c.want {
			t.Errorf("NormalizeLabel(%q,%q) = (%q,%v), want (%q,%v)", c.rd, c.fqdn, got, ok, c.want, c.ok)
		}
	}
}

func TestMerge(t *testing.T) {
	ct := map[string][]model.HostLabel{"e.com": {
		{Label: "www", DiscoverySource: "ct", LiveCert: true},
		{Label: "api", DiscoverySource: "ct", LiveCert: true},
	}}
	reg := map[string][]model.HostLabel{"e.com": {
		{Label: "api", DiscoverySource: "axfr"}, // dup of ct api — axfr precedence wins
		{Label: "vpn", DiscoverySource: "axfr"},
	}}
	got := Merge(ct, reg, 100)["e.com"]
	by := map[string]string{}
	for _, h := range got {
		by[h.Label] = h.DiscoverySource
	}
	if len(got) != 3 || by["www"] != "ct" || by["api"] != "axfr" || by["vpn"] != "axfr" {
		t.Fatalf("merge wrong: %+v", by)
	}
	// cap
	capped := Merge(ct, reg, 2)["e.com"]
	if len(capped) != 2 {
		t.Fatalf("cap 2 not applied: got %d", len(capped))
	}
}
