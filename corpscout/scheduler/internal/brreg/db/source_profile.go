package brregdb

import (
	"strings"
)

const defaultNACERevision = "2.1"

func textFilter(filters map[string]string, keys ...string) *string {
	if filters == nil {
		return nil
	}
	for _, key := range keys {
		value := strings.TrimSpace(filters[key])
		if value != "" {
			return &value
		}
	}
	return nil
}
