package tech

import (
	"strconv"
	"strings"
)

// betterVersion reports whether candidate is a more useful version than current. More numeric
// segments win, then the numerically higher version, matching wappalyzergo. A lexical tie-break
// makes equivalent textual forms deterministic instead of depending on map or pattern order.
func betterVersion(candidate, current string) bool {
	if candidate == "" {
		return false
	}
	if current == "" {
		return true
	}
	candidateParts := numericVersionParts(candidate)
	currentParts := numericVersionParts(current)
	if len(candidateParts) != len(currentParts) {
		return len(candidateParts) > len(currentParts)
	}
	for index := range candidateParts {
		if candidateParts[index] != currentParts[index] {
			return candidateParts[index] > currentParts[index]
		}
	}
	return candidate < current
}

func numericVersionParts(version string) []int {
	fields := strings.FieldsFunc(version, func(character rune) bool {
		return character == '.'
	})
	parts := make([]int, len(fields))
	for index, field := range fields {
		parts[index], _ = strconv.Atoi(field)
	}
	return parts
}
