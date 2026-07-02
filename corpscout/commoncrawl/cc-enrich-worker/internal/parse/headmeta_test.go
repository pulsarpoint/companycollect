package parse

import "testing"

func TestParseHeadMeta(t *testing.T) {
	page := `<!doctype html><html><head>
	<meta charset="utf-8">
	<title>  Acme GmbH — Home  </title>
	<meta name="description" content="We make widgets">
	<meta name="generator" content="WordPress 6.4">
	<meta property="og:title" content="Acme">
	<meta name="description" content="dup ignored">
	<link rel="canonical" href="https://acme.com/">
	<link rel="alternate" hreflang="de" href="https://acme.com/de">
	<link rel="alternate" hreflang="EN" href="https://acme.com/en">
	<link rel="alternate" hreflang="de" href="https://acme.com/de2">
	<script type="application/ld+json">{"@type":"Organization"}</script>
	</head><body>hi</body></html>`

	hm := ParseHeadMeta(page)
	if hm.Title != "Acme GmbH — Home" {
		t.Fatalf("title trimmed: %q", hm.Title)
	}
	if hm.Charset != "utf-8" {
		t.Fatalf("charset: %q", hm.Charset)
	}
	if hm.Meta["description"] != "We make widgets" {
		t.Fatalf("first meta value wins: %q", hm.Meta["description"])
	}
	if hm.Meta["generator"] != "WordPress 6.4" || hm.Meta["og:title"] != "Acme" {
		t.Fatalf("meta map: %+v", hm.Meta)
	}
	if hm.Canonical != "https://acme.com/" {
		t.Fatalf("canonical: %q", hm.Canonical)
	}
	if len(hm.Hreflang) != 2 || hm.Hreflang[0] != "de" || hm.Hreflang[1] != "en" {
		t.Fatalf("hreflang lowercased + deduped in order: %+v", hm.Hreflang)
	}
}

func TestParseHeadMetaMalformed(t *testing.T) {
	// unquoted attrs, missing </head>/</html> — must not panic, best-effort
	hm := ParseHeadMeta(`<html><head><title>Broken</title><meta name=x content=y><body>text`)
	if hm.Title != "Broken" || hm.Meta["x"] != "y" {
		t.Fatalf("best-effort on malformed: %+v", hm)
	}
}
