package httpapi

import (
	"strings"
	"testing"
)

func TestBuildReferenceNACEListQuery(t *testing.T) {
	query := buildReferenceNACEListQuery("corpscout")
	for _, needle := range []string{
		"FROM `corpscout`.`nace_codes`",
		"WHERE revision = ?",
		"active = true",
		"ORDER BY level ASC, normalized_code ASC",
		"LIMIT 5000",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildReferenceNACEListQuery() = %q, missing %q", query, needle)
		}
	}
}

func TestParseReferenceNACERevision(t *testing.T) {
	if got := parseReferenceNACERevision(""); got != "2.1" {
		t.Fatalf("parseReferenceNACERevision() = %q, want 2.1", got)
	}
	if got := parseReferenceNACERevision(" 2 "); got != "2" {
		t.Fatalf("parseReferenceNACERevision() = %q, want 2", got)
	}
}
