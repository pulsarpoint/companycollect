package extract

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/iand/microdata"

	"cc-enrich-worker/internal/model"
)

// ExtractProfileMicrodata parses schema.org microdata (itemscope/itemprop) org items — the
// pre-JSON-LD markup older sites still carry — into the same profile/identifier shapes as the
// JSON-LD path. Identifiers get Source "microdata". Cheap on the ~95% of pages without
// microdata: it bails before any DOM parse unless "itemtype" occurs in the body.
func ExtractProfileMicrodata(body []byte, pageURL string) (model.CompanyProfile, []model.Identifier) {
	var prof model.CompanyProfile
	var ids []model.Identifier
	if !bytes.Contains(body, []byte("itemtype")) {
		return prof, ids
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		base = &url.URL{}
	}
	data, err := microdata.NewParser(bytes.NewReader(body), base).Parse()
	if err != nil || data == nil {
		return prof, ids
	}
	seen := map[string]bool{}
	for _, it := range data.Items {
		mdVisit(it, 0, &prof, &ids, seen)
	}
	return prof, ids
}

// mdVisit fills from org-typed items and recurses into nested items (depth-capped).
func mdVisit(it *microdata.Item, depth int, prof *model.CompanyProfile, ids *[]model.Identifier, seen map[string]bool) {
	if it == nil || depth > 4 {
		return
	}
	if mdIsOrg(it) {
		mdFill(it, prof, ids, seen)
	}
	for _, vals := range it.Properties {
		for _, v := range vals {
			if child, ok := v.(*microdata.Item); ok {
				mdVisit(child, depth+1, prof, ids, seen)
			}
		}
	}
}

func mdIsOrg(it *microdata.Item) bool {
	for _, t := range it.Types {
		if isOrgType(t) { // itemtype is an IRI; isOrgType strips the last path segment
			return true
		}
	}
	return false
}

func mdString(it *microdata.Item, key string) string {
	for _, v := range it.Properties[key] {
		if s, ok := v.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}

func mdFill(it *microdata.Item, prof *model.CompanyProfile, ids *[]model.Identifier, seen map[string]bool) {
	for _, pair := range []struct{ key, typ string }{
		{"leiCode", "lei"}, {"vatID", "vat"}, {"taxID", "tax"}, {"duns", "duns"}, {"naics", "naics"},
	} {
		val := strings.ToUpper(mdString(it, pair.key))
		if val == "" || seen[pair.typ+":"+val] {
			continue
		}
		seen[pair.typ+":"+val] = true
		*ids = append(*ids, model.Identifier{Type: pair.typ, Value: val, Valid: identValid(pair.typ, val), Source: "microdata"})
	}
	if prof.Name == "" {
		if prof.Name = mdString(it, "name"); prof.Name == "" {
			prof.Name = mdString(it, "legalName")
		}
	}
	if prof.Description == "" {
		prof.Description = mdString(it, "description")
	}
	if prof.Logo == "" {
		prof.Logo = mdString(it, "logo")
	}
	if prof.Email == "" {
		prof.Email = strings.TrimPrefix(mdString(it, "email"), "mailto:") // <a itemprop=email href=mailto:…>
	}
	if prof.Phone == "" {
		prof.Phone = mdString(it, "telephone")
	}
	if prof.FoundingYear == 0 {
		prof.FoundingYear = parseYear(mdString(it, "foundingDate"))
	}
	if prof.EmployeeCount == 0 {
		if n := mdString(it, "numberOfEmployees"); n != "" {
			prof.EmployeeCount = ldNumber(n)
		} else {
			for _, v := range it.Properties["numberOfEmployees"] {
				if q, ok := v.(*microdata.Item); ok {
					if n := mdString(q, "value"); n != "" {
						prof.EmployeeCount = ldNumber(n)
						break
					}
				}
			}
		}
	}
	if prof.Country == "" {
		for _, v := range it.Properties["address"] {
			if addr, ok := v.(*microdata.Item); ok {
				if c := mdString(addr, "addressCountry"); c != "" {
					if len(c) == 2 {
						c = strings.ToUpper(c)
					}
					prof.Country = c
					break
				}
			}
		}
	}
	if len(prof.SameAs) == 0 {
		for _, v := range it.Properties["sameAs"] {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				prof.SameAs = append(prof.SameAs, strings.TrimSpace(s))
			}
		}
	}
}
