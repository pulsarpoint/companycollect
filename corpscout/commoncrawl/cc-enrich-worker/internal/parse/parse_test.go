package parse

import "testing"

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func TestParseHTML(t *testing.T) {
	html := `<html><head><meta name="generator" content="WordPress 6.4.2"><link href="/wp-content/themes/x/style.css" rel="stylesheet"></head><body>ACME software info@acme.com <a href="https://facebook.com/acme">fb</a></body></html>`
	text, emails, socials := ParseHTML(html)
	if !contains(emails, "info@acme.com") {
		t.Fatalf("emails=%v", emails)
	}
	if !contains(socials, "facebook") {
		t.Fatalf("socials=%v", socials)
	}
	if len(text) == 0 {
		t.Fatal("no text")
	}
}
