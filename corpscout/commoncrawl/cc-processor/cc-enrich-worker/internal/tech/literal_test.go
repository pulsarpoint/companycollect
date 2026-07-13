package tech

import "testing"

func TestGateLiterals(t *testing.T) {
	cases := map[string][]string{
		// single required literal — same result as longestRequiredLiteral
		`wp-content/themes`: {"wp-content/themes"},
		// every alternation branch carries a long literal -> gate on any of them (Go's parser
		// factors common branch prefixes, so the "<" / "c" prefix is missing from the literals —
		// still sound: each literal remains a required substring of its branch)
		`(?:<a[^>]+>powered by flyspray|<map id="projectsearchform)`: {">powered by flyspray", `map id="projectsearchform`},
		// concat containing an alternation: pick the alternative set with the longest weakest literal
		`<[^>]+(?:assets|downloads|images|videos)\.(?:ct?fassets\.net|contentful\.com)`: {"fassets.net", "ontentful.com"},
		// a branch with nothing extractable poisons the whole set
		`(?:powered by flyspray|.*)`: nil,
		// a branch whose literal is too short (<4 runes) cannot gate
		`(?:powered by flyspray|<b>)`: nil,
		`.*`:                          nil,
	}
	for pat, want := range cases {
		got := gateLiterals(pat)
		if len(got) != len(want) {
			t.Errorf("gateLiterals(%q) = %v, want %v", pat, got, want)
			continue
		}
		wantSet := map[string]bool{}
		for _, w := range want {
			wantSet[w] = true
		}
		for _, g := range got {
			if !wantSet[g] {
				t.Errorf("gateLiterals(%q) = %v, want %v", pat, got, want)
				break
			}
		}
	}
}

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
