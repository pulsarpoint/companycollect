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

func TestBuildSourceExplorerCompanyListQueryUsesCompanyFormFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`", sourceExplorerCompanyQuery{
		Limit:            50,
		CompanyFormCodes: []string{"16", "26"},
		Sort:             "name",
		Direction:        "asc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	if !strings.Contains(query, "ifNull(company_form_code, '') IN (?, ?)") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want form predicate", query)
	}
	if len(args) != 4 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 4", len(args))
	}
	if args[0] != "16" || args[1] != "26" {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args = %#v, want form codes first", args)
	}
}

func TestParseSourceExplorerStringListSplitsAndDeduplicates(t *testing.T) {
	got := parseSourceExplorerStringList([]string{"16, 26", "16", "", "  35  "}, 10)
	want := []string{"16", "26", "35"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("parseSourceExplorerStringList() = %#v, want %#v", got, want)
	}
}

func TestBuildSourceExplorerFilterOptionsQueryUsesCacheTable(t *testing.T) {
	query := buildSourceExplorerFilterOptionsQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	for _, needle := range []string{
		"company_form_code",
		"company_form_description_en",
		"GROUP BY form_code",
		"FROM `corpscout_sources`.`fi_prhytj_company_explorer_cache`",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerFilterOptionsQuery() = %q, missing %q", query, needle)
		}
	}
}
