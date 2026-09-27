package matcher

import (
	"strings"
	"testing"
)

func TestPatternMatch(t *testing.T) {
	cases := []struct {
		kind, pattern string
		cs            bool
		value         string
		want          bool
	}{
		{"suffix", "ns.cloudflare.com", false, "Adam.NS.cloudflare.com.", true},
		{"suffix", "ns.cloudflare.com", false, "ns.cloudflare.com", true},
		{"suffix", "ns.cloudflare.com", false, "evilns.cloudflare.com", false}, // label-aware
		{"suffix", ".one.com", false, "mx1.one.com", true},
		{"prefix", "google-site-verification=", false, "google-site-verification=abc", true},
		{"prefix", "ms=", false, "MS=ms123456", true},
		{"contains", "include:amazonses.com", false, "v=spf1 include:amazonses.com ~all", true},
		{"exact", "smtp.google.com", false, "SMTP.google.com.", true},
		{"glob", "awsdns-*", true, "awsdns-45.co.uk", true},
		{"glob", "AzureCloud*", true, "AzureCloud.westeurope", true},
		{"glob", "AzureCloud*", true, "AppService", false},
		{"glob", "*", true, "ANYTHING", true},
		{"regex", `^ns-[0-9]+\.awsdns-[0-9]+\.(com|net|org|co\.uk)$`, false, "NS-12.awsdns-45.co.uk", true},
		{"regex", `^ns-[0-9]+\.awsdns`, true, "NS-12.awsdns-45.com", false},
		{"exists", "", false, "", true},
		{"suffix", "one.com", false, "", false},
	}
	for _, c := range cases {
		p, err := Compile(c.kind, c.pattern, c.cs)
		if err != nil {
			t.Fatalf("Compile(%q,%q): %v", c.kind, c.pattern, err)
		}
		if got := p.Match(c.value); got != c.want {
			t.Errorf("%s %q vs %q = %v, want %v", c.kind, c.pattern, c.value, got, c.want)
		}
	}
}

func TestCompileRejects(t *testing.T) {
	cases := []struct{ kind, pattern, wantErr string }{
		{"regex", "(", "invalid regex"},
		{"suffix", "  ", "pattern is required"},
		{"exists", "x", "exists takes an empty pattern"},
		{"fuzzy", "x", "unsupported matcher_type"},
	}
	for _, c := range cases {
		_, err := Compile(c.kind, c.pattern, false)
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("Compile(%q,%q) error = %v, want %q", c.kind, c.pattern, err, c.wantErr)
		}
	}
}
