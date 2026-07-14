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

func TestEmailsRejectsAssetPathsAndMarkupNoise(t *testing.T) {
	html := `<html><body>
	<img src="logo@2x.png"><img srcset="hero@3x.jpg 3x">
	<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
	<p>Contact us at info@acme.com. Or write to sales@acme.co.uk!</p>
	<a href="mailto:Support@Acme-Group.de">support</a>
	</body></html>`
	got := Emails(html)
	for _, want := range []string{"info@acme.com", "sales@acme.co.uk", "Support@Acme-Group.de"} {
		if !contains(got, want) {
			t.Errorf("Emails() missing %q, got %v", want, got)
		}
	}
	for _, bad := range []string{"logo@2x.png", "hero@3x.jpg", "bootstrap@5.3.3", "info@acme.com."} {
		if contains(got, bad) {
			t.Errorf("Emails() must not contain %q, got %v", bad, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("Emails() = %v, want exactly the 3 real addresses", got)
	}
}

func TestExtractSocialsMatchesHostNotSubstring(t *testing.T) {
	html := `<html><body>
	<a href="https://www.wix.com/website-builder">wix</a>
	<a href="https://netflix.com/browse">netflix</a>
	<a href="https://xerox.com">xerox</a>
	<a href="https://example.com/share?u=facebook.com">share</a>
	<a href="https://notfacebook.com/p">fake</a>
	<a href="https://x.com/acme">real twitter</a>
	<a href="https://www.linkedin.com/company/acme">li</a>
	<a href="//youtube.com/@acme">yt</a>
	</body></html>`
	_, _, socials := ParseHTML(html)
	want := []string{"linkedin", "twitter", "youtube"}
	if len(socials) != len(want) {
		t.Fatalf("socials = %v, want %v", socials, want)
	}
	for _, w := range want {
		if !contains(socials, w) {
			t.Errorf("socials = %v, missing %q", socials, w)
		}
	}
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
