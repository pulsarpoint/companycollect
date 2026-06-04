package httpapi

type rawInputSource struct {
	source             string
	tableName          string
	nameColumn         string
	nativeColumn       string
	translated         bool
	statusExpr         string
	stateExpr          string
	companyTypeExpr    string
	registrationColumn string
	websiteExpr        string
	countryColumn      string
	runIDExpr          string
	attemptsExpr       string
	errorExpr          string
	firstSeenExpr      string
	lastSeenExpr       string
	processedAtExpr    string
	createdAtExpr      string
	updatedAtExpr      string
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
		tableName:          "cvr_workflow.raw_records",
		nameColumn:         "company_name",
		nativeColumn:       "cvr_number",
		statusExpr:         "'pending'",
		stateExpr:          "'pending'",
		companyTypeExpr:    "company_type",
		registrationColumn: "registration_status",
		websiteExpr:        "website",
		countryColumn:      "country_iso2",
		runIDExpr:          "''",
		attemptsExpr:       "0",
		errorExpr:          "''",
		firstSeenExpr:      "first_seen_at",
		lastSeenExpr:       "last_seen_at",
		processedAtExpr:    "NULL::timestamptz",
		createdAtExpr:      "first_seen_at",
		updatedAtExpr:      "last_seen_at",
	},
	{
		source:             "ariregister",
		tableName:          "ariregister_workflow.raw_records",
		nameColumn:         "legal_name",
		nativeColumn:       "registry_code",
		statusExpr:         "'pending'",
		stateExpr:          "'pending'",
		companyTypeExpr:    "legal_form",
		registrationColumn: "registration_status",
		websiteExpr:        "website",
		countryColumn:      "country_iso2",
		runIDExpr:          "''",
		attemptsExpr:       "0",
		errorExpr:          "''",
		firstSeenExpr:      "first_seen_at",
		lastSeenExpr:       "last_seen_at",
		processedAtExpr:    "NULL::timestamptz",
		createdAtExpr:      "first_seen_at",
		updatedAtExpr:      "last_seen_at",
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
