package httpapi

import (
	"reflect"
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

func TestBuildSourceExplorerCompanyListQueryUsesIndustryNACEFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`", sourceExplorerCompanyQuery{
		Limit:        50,
		IndustryNACE: []string{"68", "68.20", "L"},
		Sort:         "name",
		Direction:    "asc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	for _, needle := range []string{
		"ifNull(nace_division_code, '') = ?",
		"ifNull(nace_class_code, '') = ?",
		"ifNull(nace_section_code, '') = ?",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, missing %q", query, needle)
		}
	}
	if !strings.Contains(query, " OR ") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want ORed NACE filters", query)
	}
	if len(args) != 5 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 5", len(args))
	}
	for index, want := range []any{"68", "68.20", "L"} {
		if args[index] != want {
			t.Fatalf("buildSourceExplorerCompanyListQuery() args[%d] = %#v, want %#v", index, args[index], want)
		}
	}
}

func TestBuildSourceExplorerCompanyListQueryUsesSourceIndustryFilters(t *testing.T) {
	query, args, err := buildSourceExplorerCompanyListQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`", sourceExplorerCompanyQuery{
		Limit: 50,
		SourceIndustries: []sourceExplorerSourceIndustryFilter{
			{CodeSet: "TOIMI4", Code: "68203"},
			{CodeSet: "TOIMI3", Code: "64190"},
		},
		Sort:      "name",
		Direction: "asc",
	})
	if err != nil {
		t.Fatalf("buildSourceExplorerCompanyListQuery() error = %v", err)
	}
	needle := "(ifNull(main_business_line_code_set, '') = ? AND ifNull(main_business_line_code, '') = ?)"
	if !strings.Contains(query, needle) {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, missing %q", query, needle)
	}
	if !strings.Contains(query, " OR ") {
		t.Fatalf("buildSourceExplorerCompanyListQuery() query = %q, want ORed source industry filters", query)
	}
	if len(args) != 6 {
		t.Fatalf("buildSourceExplorerCompanyListQuery() args length = %d, want 6", len(args))
	}
	wantArgs := []any{"TOIMI4", "68203", "TOIMI3", "64190"}
	for index, want := range wantArgs {
		if args[index] != want {
			t.Fatalf("buildSourceExplorerCompanyListQuery() args[%d] = %#v, want %#v", index, args[index], want)
		}
	}
}

func TestParseSourceExplorerStringListSplitsAndDeduplicates(t *testing.T) {
	got := parseSourceExplorerStringList([]string{"16, 26", "16", "", "  35  "}, 10)
	want := []string{"16", "26", "35"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("parseSourceExplorerStringList() = %#v, want %#v", got, want)
	}
}

func TestParseSourceExplorerSourceIndustriesSkipsInvalidValues(t *testing.T) {
	got := parseSourceExplorerSourceIndustries([]string{" toimi4 : 68203 ", "bad", "TOIMI4:68203:bad", "TOIMI3:64190"}, 10)
	want := []sourceExplorerSourceIndustryFilter{
		{CodeSet: "TOIMI4", Code: "68203"},
		{CodeSet: "TOIMI3", Code: "64190"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseSourceExplorerSourceIndustries() = %#v, want %#v", got, want)
	}
}

func TestBuildSourceExplorerFormFilterOptionsQueryUsesCacheTable(t *testing.T) {
	query := buildSourceExplorerFormFilterOptionsQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	for _, needle := range []string{
		"company_form_code",
		"company_form_description_en",
		"GROUP BY form_code",
		"FROM `corpscout_sources`.`fi_prhytj_company_explorer_cache`",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerFormFilterOptionsQuery() = %q, missing %q", query, needle)
		}
	}
}

func TestBuildSourceExplorerIndustryFilterOptionsQueryUsesNACEAndSourceIndustryOptions(t *testing.T) {
	query := buildSourceExplorerIndustryFilterOptionsQuery("`corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	for _, needle := range []string{
		"'nace' AS kind",
		"'source_industry' AS kind",
		"`corpscout_reference`.`nace_codes`",
		"nace_division_code",
		"main_business_line_code_set",
		"company_count",
		"UNION ALL",
		"concat('nace:division:', cache.code)",
	} {
		if !strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerIndustryFilterOptionsQuery() = %q, missing %q", query, needle)
		}
	}
	for _, needle := range []string{
		"concat('nace:', cache.revision",
		"concat('nace:', revision",
	} {
		if strings.Contains(query, needle) {
			t.Fatalf("buildSourceExplorerIndustryFilterOptionsQuery() = %q, should not build NACE ids with %q", query, needle)
		}
	}
}
