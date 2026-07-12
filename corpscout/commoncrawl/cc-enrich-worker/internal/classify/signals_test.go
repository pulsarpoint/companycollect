package classify

import "testing"

func TestMatchSignal(t *testing.T) {
	cases := map[string]string{
		"This domain is for sale":                        "for_sale",
		"Welcome to nginx! If you see this page":         "default_server",
		"This site is under construction":                "under_construction",
		"Index of /pub\nParent Directory\nLast modified": "directory_listing",
		"We are a law firm, courtesy of our partners":    "", // must NOT fire
		"Original artwork for sale by the artist":        "", // generic 'for sale'
	}
	for text, want := range cases {
		if got := MatchSignal(text); got != want {
			t.Errorf("MatchSignal(%q)=%q want %q", text, got, want)
		}
	}
}
