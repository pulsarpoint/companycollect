package httpapi

type rawInputSource struct {
	source             string
	tableName          string
	nameColumn         string
	nativeColumn       string
	translated         bool
	companyTypeExpr    string
	registrationColumn string
	websiteExpr        string
	countryColumn      string
}

var rawInputSources = []rawInputSource{
	{
		source:             "gleif",
		tableName:          "gleif_company_raw_inputs",
		nameColumn:         "legal_name",
		nativeColumn:       "lei",
		companyTypeExpr:    "''",
		registrationColumn: "registration_status",
		websiteExpr:        "''",
		countryColumn:      "headquarters_country_code",
	},
	{
		source:             "companies_house",
		tableName:          "companies_house_company_raw_inputs",
		nameColumn:         "company_name",
		nativeColumn:       "company_number",
		companyTypeExpr:    "company_type",
		registrationColumn: "''",
		websiteExpr:        "''",
		countryColumn:      "country_iso2",
	},
	{
		source:             "cvr",
		tableName:          "cvr_company_raw_inputs",
		nameColumn:         "company_name",
		nativeColumn:       "cvr_number",
		translated:         true,
		companyTypeExpr:    "company_type",
		registrationColumn: "registration_status",
		websiteExpr:        "website",
		countryColumn:      "country_iso2",
	},
	{
		source:             "ariregister",
		tableName:          "ariregister_company_raw_inputs",
		nameColumn:         "legal_name",
		nativeColumn:       "registry_code",
		translated:         true,
		companyTypeExpr:    "legal_form",
		registrationColumn: "registration_status",
		websiteExpr:        "website",
		countryColumn:      "country_iso2",
	},
}

func rawInputSourceByName(source string) (rawInputSource, bool) {
	for _, cfg := range rawInputSources {
		if cfg.source == source {
			return cfg, true
		}
	}
	return rawInputSource{}, false
}
