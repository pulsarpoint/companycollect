package extract

import (
	"regexp"

	"cc-enrich-worker/internal/model"
)

// trackerPats maps an id_type to a regex whose capture group 1 is the account/container id. Patterns are
// deliberately specific (provider prefixes like G-/UA-/GTM-/ca-pub-, or the provider's init-call shape) to
// keep false positives low across millions of pages. The ownership graph additionally rarity-filters ids
// (a value on 2..~20 distinct domains), so any stray unique FP is dropped there. Wappalyzer still supplies
// the tech NAME independently; this captures the ID VALUE Wappalyzer often doesn't.
var trackerPats = []struct {
	typ string
	re  *regexp.Regexp
}{
	{"ga", regexp.MustCompile(`\b(G-[A-Z0-9]{6,12})\b`)},
	{"ua", regexp.MustCompile(`\b(UA-\d{4,10}-\d{1,3})\b`)},
	{"gtm", regexp.MustCompile(`\b(GTM-[A-Z0-9]{4,8})\b`)},
	{"adsense", regexp.MustCompile(`\b(ca-pub-\d{10,20})\b`)},
	{"fb_pixel", regexp.MustCompile(`fbq\(\s*['"]init['"]\s*,\s*['"](\d{10,20})['"]`)},
	{"hotjar", regexp.MustCompile(`hjid\s*[:=]\s*(\d{5,9})`)},
	{"linkedin_insight", regexp.MustCompile(`_linkedin_partner_id\s*=\s*["'](\d{4,10})["']`)},
	{"yandex_metrika", regexp.MustCompile(`ym\(\s*(\d{5,9})\s*,`)},
	{"mixpanel", regexp.MustCompile(`mixpanel\.init\(\s*["']([a-f0-9]{32})["']`)},
	{"tiktok_pixel", regexp.MustCompile(`ttq\.load\(\s*['"]([A-Z0-9]{15,25})['"]`)},
	{"pinterest_tag", regexp.MustCompile(`pintrk\(\s*['"]load['"]\s*,\s*['"](\d{10,20})['"]`)},
	{"snap_pixel", regexp.MustCompile(`snaptr\(\s*['"]init['"]\s*,\s*['"]([0-9a-f-]{36})['"]`)},
	{"segment", regexp.MustCompile(`analytics\.load\(\s*["']([A-Za-z0-9]{10,40})["']`)},
	{"clarity", regexp.MustCompile(`clarity\.ms/tag/([a-z0-9]{8,15})`)},
}

// Trackers extracts analytics / advertising / tag-manager account IDs from page HTML — the "same operator"
// edge for the ownership/sibling graph. Each match -> Identifier{Type, Value, Valid: true (format),
// Source: "html"}, deduped by (type, value) within the page.
func Trackers(body []byte) []model.Identifier {
	s := string(body)
	var out []model.Identifier
	seen := map[string]bool{}
	for _, tp := range trackerPats {
		for _, m := range tp.re.FindAllStringSubmatch(s, -1) {
			val := m[1]
			key := tp.typ + ":" + val
			if val == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, model.Identifier{Type: tp.typ, Value: val, Valid: true, Source: "html"})
		}
	}
	return out
}
