package nacetaxonomy

import (
	"strings"
	"unicode"
)

const DefaultRevision = "2.1"

func NormalizeCode(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		if r == '.' || r == '-' || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

func LevelForCode(value string) int16 {
	normalized := NormalizeCode(value)
	switch {
	case len(normalized) == 1 && normalized[0] >= 'A' && normalized[0] <= 'Z':
		return 1
	case len(normalized) == 2:
		return 2
	case len(normalized) == 3:
		return 3
	case len(normalized) == 4:
		return 4
	default:
		return 0
	}
}

func LevelNameForCode(value string) string {
	switch LevelForCode(value) {
	case 1:
		return "section"
	case 2:
		return "division"
	case 3:
		return "group"
	case 4:
		return "class"
	default:
		return ""
	}
}

func ClassFromNorwegianSNCode(value string) string {
	normalized := NormalizeCode(value)
	if len(normalized) != 5 {
		return ""
	}
	return normalized[:2] + "." + normalized[2:4]
}
