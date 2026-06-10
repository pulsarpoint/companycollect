package httpapi

import (
	"strings"
	"testing"
)

func TestSourceExplorerCompanyOrderByKeepsBlankNamesLast(t *testing.T) {
	orderBy, err := sourceExplorerCompanyOrderBy("name")
	if err != nil {
		t.Fatalf("sourceExplorerCompanyOrderBy() error = %v", err)
	}
	if !strings.Contains(orderBy, "(ifNull(name, '') = '') ASC") {
		t.Fatalf("sourceExplorerCompanyOrderBy() = %q, want blank-name expression", orderBy)
	}
}

func TestBuildSourceExplorerCompanyListQueryRejectsUnsupportedSort(t *testing.T) {
	_, _, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer`", sourceExplorerCompanyQuery{
		Limit:     50,
		Direction: "asc",
		Sort:      "name; DROP TABLE companies",
	})
	if err == nil {
		t.Fatal("buildSourceExplorerCompanyListQuery() error = nil, want unsupported sort error")
	}
}

func TestBuildSourceExplorerCompanyListQueryUsesSearchAndActiveFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer`", sourceExplorerCompanyQuery{
		Limit:     50,
		Search:    "dynava",
		Active:    "true",
		Sort:      "registration_date",
		Direction: "desc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	if !strings.Contains(query, "positionCaseInsensitiveUTF8") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want search predicate", query)
	}
	if !strings.Contains(query, "ifNull(is_active, false) = true") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want active predicate", query)
	}
	if !strings.Contains(query, "ORDER BY ifNull(registration_date, '') desc") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want whitelisted order by", query)
	}
	if len(args) != 7 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 7", len(args))
	}
}
