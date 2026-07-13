package tech

import "testing"

func TestLongestRequiredLiteral(t *testing.T) {
	cases := map[string]string{
		`wp-content/themes`:                "wp-content/themes",
		`(?i)Shopify\.theme`:               "shopify.theme",
		`<div class="x">.*foo`:             `<div class="x">`, // .* after is not required
		`(?:alpha|beta)gamma-delta`:        "gamma-delta",     // alternation gives nothing; tail is required
		`/wp-(?:content|includes)/`:        "/wp-",            // longest required run around the alternation
		`<meta[^>]+name=["']generator["']`: "generator",       // [^>]+ breaks the run; "generator" is required
		`.*`:                               "",                // nothing required
		`[a-z]+`:                           "",                // char class, nothing literal
		`buy this domain`:                  "buy this domain",
	}
	for pat, want := range cases {
		if got := longestRequiredLiteral(pat); got != want {
			t.Errorf("longestRequiredLiteral(%q) = %q, want %q", pat, got, want)
		}
	}
}
