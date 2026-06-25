package main

import "regexp"

type sig struct {
	label string
	re    *regexp.Regexp
}

// signals: high-precision page-type keyword patterns, ported from
// commoncrawl_enrich/page_types.py PAGE_TYPE_SIGNALS. Case-insensitive + DOTALL,
// scanned over only the first embedMaxChars chars.
var signals = func() []sig {
	pats := []struct{ label, p string }{
		{"for_sale", `(?is)\bbuy this domain\b`},
		{"for_sale", `(?is)\bpurchase this domain\b`},
		{"for_sale", `(?is)this domain (?:is|may be|could be) for sale`},
		{"for_sale", `(?is)\bdomain(?: name)? is for sale\b`},
		{"for_sale", `(?is)the domain .{0,60}? is for sale`},
		{"for_sale", `(?is)\bthis domain has expired\b`},
		{"for_sale", `(?is)\binterested in this domain\b`},
		{"parking_provider", `(?is)\bsedoparking\b`},
		{"parking_provider", `(?is)\bparkingcrew\b`},
		{"parking_provider", `(?is)\bafternic\b`},
		{"parking_provider", `(?is)\bhugedomains\b`},
		{"parking_provider", `(?is)\bcashparking\b`},
		{"parking_provider", `(?is)this (?:web ?)?page is parked`},
		{"parking_provider", `(?is)\bthis domain (?:is )?parked\b`},
		{"parking_provider", `(?is)\bparked free\b`},
		{"parking_provider", `(?is)\bdomain parking\b`},
		{"parking_provider", `(?is)\bparked by (?:the )?(?:domain|registrant|owner)\b`},
		{"under_construction", `(?is)\bunder construction\b`},
		{"under_construction", `(?is)\bwebsite coming soon\b`},
		{"default_server", `(?is)\bwelcome to nginx\b`},
		{"default_server", `(?is)apache2?.{0,30}default page`},
		{"default_server", `(?is)\btest page for the (?:apache|nginx)\b`},
		{"default_server", `(?is)\bapache http server test page\b`},
		{"default_server", `(?is)\b(?:iis windows server|iisstart|internet information services)\b`},
		{"default_server", `(?is)\bsite not configured\b`},
		{"default_server", `(?is)\bno (?:web)?site (?:is )?configured\b`},
		{"default_server", `(?is)\byour new website is ready\b`},
		{"default_server", `(?is)\bweb server'?s default (?:web ?)?page\b`},
		{"directory_listing", `(?is)\bindex of /\S*.{0,120}(?:parent directory|last modified)`},
	}
	out := make([]sig, len(pats))
	for i, p := range pats {
		out[i] = sig{p.label, regexp.MustCompile(p.p)}
	}
	return out
}()

// MatchSignal returns the page-type label for the first matching high-precision
// pattern, or "" if none fire. Only the first embedMaxChars are scanned.
func MatchSignal(text string) string {
	head := text
	if len(head) > embedMaxChars {
		head = head[:embedMaxChars]
	}
	for _, s := range signals {
		if s.re.MatchString(head) {
			return s.label
		}
	}
	return ""
}
