package extract

import "encoding/json"

// JSONLDTypes returns every distinct schema.org @type declared across the page's JSON-LD blocks
// (Organization, Product, JobPosting, Event, BreadcrumbList, …) — a cheap content-classification signal
// captured without storing the raw blocks. Deduped, in first-seen order. Reuses ldBlockRe + walkLD.
func JSONLDTypes(body []byte) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range ldBlockRe.FindAllSubmatch(body, -1) {
		var v any
		if json.Unmarshal(m[1], &v) != nil {
			continue
		}
		walkLD(v, func(obj map[string]any) {
			for _, t := range ldTypeList(obj["@type"]) {
				if t != "" && !seen[t] {
					seen[t] = true
					out = append(out, t)
				}
			}
		})
	}
	return out
}

// ldTypeList normalizes a JSON-LD @type value (string or array of strings) to a slice.
func ldTypeList(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		var out []string
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
