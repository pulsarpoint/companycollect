package httpapi

type rawInputSource struct {
	source              string
	tableName           string
	suggestionTableName string
	nameColumn          string
	nativeColumn        string
	translated          bool
	statusExpr          string
	stateExpr           string
	companyTypeExpr     string
	registrationColumn  string
	websiteExpr         string
	countryColumn       string
	runIDExpr           string
	attemptsExpr        string
	errorExpr           string
	firstSeenExpr       string
	lastSeenExpr        string
	processedAtExpr     string
	createdAtExpr       string
	updatedAtExpr       string
}

const franceRawInputTableName = `(
	SELECT
		id,
		COALESCE(NULLIF(legal_name, ''), NULLIF(usage_name, ''), NULLIF(birth_name, ''), siren) AS display_name,
		siren AS native_id,
		'legal_unit'::text AS company_type,
		COALESCE(administrative_status, '') AS registration_status,
		''::text AS website,
		'FR'::text AS country_iso2,
		''::text AS run_id,
		0::integer AS processing_attempts,
		''::text AS processing_error,
		payload_hash,
		raw_payload,
		first_seen_at,
		last_seen_at,
		NULL::timestamptz AS processed_at,
		first_seen_at AS created_at,
		last_seen_at AS updated_at
	FROM france_workflow.raw_legal_units
	WHERE is_current
	UNION ALL
	SELECT
		id,
		COALESCE(NULLIF(trade_name_1, ''), NULLIF(usual_name, ''), NULLIF(street_label, ''), siret) AS display_name,
		siret AS native_id,
		'establishment'::text AS company_type,
		COALESCE(administrative_status, '') AS registration_status,
		''::text AS website,
		'FR'::text AS country_iso2,
		''::text AS run_id,
		0::integer AS processing_attempts,
		''::text AS processing_error,
		payload_hash,
		raw_payload,
		first_seen_at,
		last_seen_at,
		NULL::timestamptz AS processed_at,
		first_seen_at AS created_at,
		last_seen_at AS updated_at
	FROM france_workflow.raw_establishments
	WHERE is_current
)`

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
	{
		source:              "france",
		tableName:           franceRawInputTableName,
		suggestionTableName: "france_workflow.raw_records",
		nameColumn:          "display_name",
		nativeColumn:        "native_id",
		statusExpr:          "'pending'",
		stateExpr:           "'pending'",
		companyTypeExpr:     "company_type",
		registrationColumn:  "registration_status",
		websiteExpr:         "website",
		countryColumn:       "country_iso2",
		runIDExpr:           "run_id",
		attemptsExpr:        "processing_attempts",
		errorExpr:           "processing_error",
		firstSeenExpr:       "first_seen_at",
		lastSeenExpr:        "last_seen_at",
		processedAtExpr:     "processed_at",
		createdAtExpr:       "created_at",
		updatedAtExpr:       "updated_at",
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

func rawInputSuggestionTableName(src rawInputSource) string {
	if src.suggestionTableName != "" {
		return src.suggestionTableName
	}
	return src.tableName
}
