package input

import (
	"strings"
	"testing"
)

func TestPageQueryUsesBoundKeysetPagination(t *testing.T) {
	for _, fragment := range []string{
		"SELECT DISTINCT root_domain", "FROM corpscout.commoncrawl_domains", "PREWHERE root_domain > ?",
		"ORDER BY root_domain", "LIMIT ?", "optimize_distinct_in_order = 1",
	} {
		if !strings.Contains(pageQuery, fragment) {
			t.Errorf("page query missing %q: %s", fragment, pageQuery)
		}
	}
	for _, forbidden := range []string{"OFFSET", "GROUP BY", "FROM\n("} {
		if strings.Contains(pageQuery, forbidden) {
			t.Errorf("page query contains expensive construct %q: %s", forbidden, pageQuery)
		}
	}
}
