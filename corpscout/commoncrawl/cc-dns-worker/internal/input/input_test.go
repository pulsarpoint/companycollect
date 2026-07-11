package input

import (
	"strings"
	"testing"
)

func TestPageQueryUsesBoundKeysetPagination(t *testing.T) {
	for _, fragment := range []string{
		"FROM corpscout.commoncrawl_domains", "root_domain != ''", "root_domain > ?",
		"GROUP BY root_domain", "ORDER BY root_domain", "LIMIT ?",
	} {
		if !strings.Contains(pageQuery, fragment) {
			t.Errorf("page query missing %q: %s", fragment, pageQuery)
		}
	}
	if strings.Contains(pageQuery, "OFFSET") {
		t.Errorf("pagination must be keyset based: %s", pageQuery)
	}
}
