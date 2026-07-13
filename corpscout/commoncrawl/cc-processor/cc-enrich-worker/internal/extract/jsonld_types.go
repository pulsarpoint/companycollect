package extract

import (
	"sort"
	"strings"

	"cc-enrich-worker/internal/model"
)

// JSONLDTypeNames returns the sorted union of types from already-parsed entities. The worker uses
// this as a compact primary-page summary while commoncrawl_page_jsonld remains authoritative.
func JSONLDTypeNames(entities []model.JSONLDEntity) []string {
	seen := map[string]bool{}
	for _, entity := range entities {
		for _, entityType := range entity.Types {
			seen[entityType] = true
		}
	}
	types := make([]string, 0, len(seen))
	for entityType := range seen {
		types = append(types, entityType)
	}
	sort.Strings(types)
	return types
}

func normalizedLDTypes(value any) []string {
	types := ldTypeList(value)
	for i := range types {
		types[i] = strings.TrimSpace(types[i])
	}
	sort.Strings(types)
	return compactStrings(types)
}

func sortedLDStrings(value any) []string {
	values := ldStrings(value)
	sort.Strings(values)
	return compactStrings(values)
}

func compactStrings(values []string) []string {
	result := values[:0]
	for _, value := range values {
		if value == "" || len(result) > 0 && result[len(result)-1] == value {
			continue
		}
		result = append(result, value)
	}
	return result
}

// ldTypeList normalizes a JSON-LD @type value (string or array of strings) to a slice.
func ldTypeList(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case []any:
		var types []string
		for _, element := range typed {
			if entityType, ok := element.(string); ok {
				types = append(types, entityType)
			}
		}
		return types
	}
	return nil
}
