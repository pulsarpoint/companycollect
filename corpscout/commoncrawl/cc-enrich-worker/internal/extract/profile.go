package extract

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"cc-enrich-worker/internal/model"
)

var ldBlockRe = regexp.MustCompile(`(?is)<script[^>]+type\s*=\s*["']application/ld\+json["'][^>]*>(.*?)</script>`)

// orgTypeRe catches org types whose names contain a generic substring (…Organization, …Store,
// FastFoodRestaurant, …); localBusinessTypes below pins the schema.org LocalBusiness/Organization
// subtypes that share none of them (Dentist, Hotel, Plumber, …) — the SMB segment.
var orgTypeRe = regexp.MustCompile(`(?i)organization|localbusiness|corporation|\bngo\b|store|restaurant|\bcompany\b`)

// localBusinessTypes is the lowercased schema.org LocalBusiness + Organization subtype names not
// already matched by orgTypeRe (from the schema.org type hierarchy).
var localBusinessTypes = map[string]bool{
	// LocalBusiness direct subtypes
	"animalshelter": true, "automotivebusiness": true, "childcare": true, "dentist": true,
	"drycleaningorlaundry": true, "emergencyservice": true, "employmentagency": true,
	"entertainmentbusiness": true, "financialservice": true, "foodestablishment": true,
	"governmentoffice": true, "healthandbeautybusiness": true, "homeandconstructionbusiness": true,
	"internetcafe": true, "legalservice": true, "library": true, "lodgingbusiness": true,
	"medicalbusiness": true, "professionalservice": true, "radiostation": true,
	"realestateagent": true, "recyclingcenter": true, "selfstorage": true, "shoppingcenter": true,
	"sportsactivitylocation": true, "televisionstation": true, "touristinformationcenter": true,
	"travelagency": true,
	// automotive
	"autobodyshop": true, "autodealer": true, "autorental": true, "autorepair": true,
	"autowash": true, "gasstation": true, "motorcycledealer": true, "motorcyclerepair": true,
	// entertainment
	"adultentertainment": true, "amusementpark": true, "artgallery": true, "casino": true,
	"comedyclub": true, "movietheater": true, "nightclub": true,
	// financial
	"accountingservice": true, "bankorcreditunion": true, "insuranceagency": true,
	// food
	"bakery": true, "barorpub": true, "brewery": true, "cafeorcoffeeshop": true,
	"distillery": true, "icecreamshop": true, "winery": true,
	// health & beauty
	"beautysalon": true, "dayspa": true, "hairsalon": true, "healthclub": true,
	"nailsalon": true, "tattooparlor": true,
	// home & construction
	"electrician": true, "generalcontractor": true, "hvacbusiness": true, "housepainter": true,
	"locksmith": true, "movingcompany": true, "plumber": true, "roofingcontractor": true,
	// legal
	"attorney": true, "notary": true,
	// lodging
	"bedandbreakfast": true, "campground": true, "hostel": true, "hotel": true,
	"motel": true, "resort": true,
	// medical
	"dermatology": true, "hospital": true, "medicalclinic": true, "optician": true,
	"pharmacy": true, "physician": true, "physiotherapy": true, "veterinarycare": true,
	// sports & leisure
	"bowlingalley": true, "exercisegym": true, "golfcourse": true, "publicswimmingpool": true,
	"skiresort": true, "sportsclub": true, "stadiumorarena": true, "tenniscomplex": true,
	// stores without "store"/"shop" collisions handled by orgTypeRe
	"florist": true, "hobbyshop": true, "pawnshop": true, "tireshop": true,
	// Organization subtypes
	"airline": true, "consortium": true, "collegeoruniversity": true, "school": true,
	"elementaryschool": true, "highschool": true, "middleschool": true, "preschool": true,
	"newsmediaorganization": true, "onlinebusiness": true, "sportsteam": true, "workersunion": true,
}
var yearRe = regexp.MustCompile(`(?:18|19|20)\d{2}`)

// ExtractProfile parses schema.org Organization/LocalBusiness JSON-LD into a company
// profile plus any structured identifiers (lei/vat/tax/duns/naics). First non-empty
// org object wins for each field; identifiers are unioned.
func ExtractProfile(body []byte) (model.CompanyProfile, []model.Identifier) {
	var prof model.CompanyProfile
	var ids []model.Identifier
	seen := map[string]bool{}

	for _, m := range ldBlockRe.FindAllSubmatch(body, -1) {
		var v any
		if json.Unmarshal(m[1], &v) != nil {
			continue
		}
		walkLD(v, func(obj map[string]any) {
			if !isOrg(obj) {
				return
			}
			for _, pair := range []struct{ key, typ string }{
				{"leiCode", "lei"}, {"vatID", "vat"}, {"taxID", "tax"}, {"duns", "duns"}, {"naics", "naics"},
			} {
				val := strings.ToUpper(strings.TrimSpace(ldString(obj[pair.key])))
				if val == "" || seen[pair.typ+":"+val] {
					continue
				}
				seen[pair.typ+":"+val] = true
				ids = append(ids, model.Identifier{Type: pair.typ, Value: val, Valid: identValid(pair.typ, val), Source: "jsonld"})
			}
			if prof.Name == "" {
				if prof.Name = ldString(obj["name"]); prof.Name == "" {
					prof.Name = ldString(obj["legalName"])
				}
			}
			if prof.Description == "" {
				prof.Description = ldString(obj["description"])
			}
			if prof.Logo == "" {
				prof.Logo = ldString(obj["logo"])
			}
			if prof.Email == "" {
				prof.Email = ldString(obj["email"])
			}
			if prof.Phone == "" {
				prof.Phone = ldString(obj["telephone"])
			}
			if prof.FoundingYear == 0 {
				prof.FoundingYear = parseYear(ldString(obj["foundingDate"]))
			}
			if prof.EmployeeCount == 0 {
				prof.EmployeeCount = ldNumber(obj["numberOfEmployees"])
			}
			if prof.Country == "" {
				prof.Country = countryOf(obj["address"])
			}
			if len(prof.SameAs) == 0 {
				prof.SameAs = ldStrings(obj["sameAs"])
			}
		})
	}
	return prof, ids
}

func identValid(typ, val string) bool {
	switch typ {
	case "lei":
		return validLEI(val)
	case "vat":
		return validVAT(val)
	}
	return false // no validation yet for tax/duns/naics
}

// walkLD visits every JSON object in a parsed JSON-LD value (handles @graph, arrays, nesting).
func walkLD(v any, fn func(map[string]any)) {
	switch x := v.(type) {
	case map[string]any:
		fn(x)
		for _, val := range x {
			walkLD(val, fn)
		}
	case []any:
		for _, it := range x {
			walkLD(it, fn)
		}
	}
}

func isOrg(obj map[string]any) bool {
	switch t := obj["@type"].(type) {
	case string:
		return isOrgType(t)
	case []any:
		for _, it := range t {
			if s, ok := it.(string); ok && isOrgType(s) {
				return true
			}
		}
	}
	return false
}

func isOrgType(t string) bool {
	if orgTypeRe.MatchString(t) {
		return true
	}
	// @type may be a bare name, a full IRI (https://schema.org/Dentist), or prefixed (schema:Dentist).
	if i := strings.LastIndexAny(t, "/:#"); i >= 0 {
		t = t[i+1:]
	}
	return localBusinessTypes[strings.ToLower(strings.TrimSpace(t))]
}

func ldString(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case map[string]any:
		for _, k := range []string{"@value", "name", "url", "value"} {
			if s, ok := x[k].(string); ok {
				return strings.TrimSpace(s)
			}
		}
	case []any:
		if len(x) > 0 {
			return ldString(x[0])
		}
	}
	return ""
}

func ldStrings(v any) []string {
	switch x := v.(type) {
	case string:
		if s := strings.TrimSpace(x); s != "" {
			return []string{s}
		}
	case []any:
		var out []string
		for _, it := range x {
			if s := ldString(it); s != "" {
				out = append(out, s)
			}
		}
		return out
	case map[string]any:
		if s := ldString(x); s != "" {
			return []string{s}
		}
	}
	return nil
}

func ldNumber(v any) uint32 {
	switch x := v.(type) {
	case float64:
		if x > 0 {
			return uint32(x)
		}
	case string:
		i := 0
		for i < len(x) && x[i] >= '0' && x[i] <= '9' {
			i++
		}
		if i > 0 {
			n, _ := strconv.Atoi(x[:i])
			return uint32(n)
		}
	case map[string]any:
		for _, k := range []string{"value", "@value", "minValue"} {
			if n := ldNumber(x[k]); n > 0 {
				return n
			}
		}
	}
	return 0
}

func parseYear(s string) uint16 {
	if m := yearRe.FindString(s); m != "" {
		y, _ := strconv.Atoi(m)
		return uint16(y)
	}
	return 0
}

func countryOf(v any) string {
	switch x := v.(type) {
	case map[string]any:
		c := strings.TrimSpace(ldString(x["addressCountry"]))
		if len(c) == 2 {
			return strings.ToUpper(c)
		}
		return c
	case []any:
		for _, it := range x {
			if c := countryOf(it); c != "" {
				return c
			}
		}
	}
	return ""
}
