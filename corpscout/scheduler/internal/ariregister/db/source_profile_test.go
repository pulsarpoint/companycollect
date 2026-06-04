package ariregisterdb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestNormalizeSourceProfilesWithCopyUpsertsAndSupersedesAriregisterProfile(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	registryCode := "t5-" + uuid.NewString()
	rawRecordID := uuid.New()
	payload := ariregisterGeneralDataPayload(t, registryCode, "Task Five OÜ")
	payloadHash := uuid.NewString()
	insertAriregisterRawRecord(t, tx, rawRecordID, registryCode, "Task Five OÜ", payload, payloadHash)

	result, err := New(tx).NormalizeSourceProfilesWithCopy(ctx, NormalizeSourceProfilesCommand{
		Limit:   10,
		Trigger: "test",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RecordsSeen)
	require.EqualValues(t, 1, result.CompaniesUpserted)
	require.EqualValues(t, 1, result.AddressesUpserted)
	require.EqualValues(t, 2, result.ContactsUpserted)
	require.EqualValues(t, 1, result.WebsitesUpserted)
	require.EqualValues(t, 1, result.DomainsUpserted)
	require.EqualValues(t, 1, result.IndustriesUpserted)
	require.EqualValues(t, 1, result.CapitalUpserted)
	require.EqualValues(t, 1, result.AnnualReportsUpserted)

	companyID := requireActiveAriregisterCompany(t, tx, registryCode, rawRecordID, payloadHash)
	requireAriregisterChildCount(t, tx, "addresses", companyID, 1)
	requireAriregisterChildCount(t, tx, "contacts", companyID, 2)
	requireAriregisterChildCount(t, tx, "websites", companyID, 1)
	requireAriregisterChildCount(t, tx, "domains", companyID, 1)
	requireAriregisterChildCount(t, tx, "industries", companyID, 1)
	requireAriregisterChildCount(t, tx, "capital", companyID, 1)
	requireAriregisterChildCount(t, tx, "annual_reports", companyID, 1)

	retryResult, err := New(tx).NormalizeSourceProfilesWithCopy(ctx, NormalizeSourceProfilesCommand{
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, retryResult.RecordsSeen)

	newRawRecordID := uuid.New()
	newPayload := ariregisterGeneralDataPayload(t, registryCode, "Task Five Updated OÜ")
	newPayloadHash := uuid.NewString()
	_, err = tx.Exec(ctx, `
UPDATE ariregister_workflow.raw_records
SET is_current = false
WHERE id = $1
`, rawRecordID)
	require.NoError(t, err)
	insertAriregisterRawRecord(t, tx, newRawRecordID, registryCode, "Task Five Updated OÜ", newPayload, newPayloadHash)

	updateResult, err := New(tx).NormalizeSourceProfilesWithCopy(ctx, NormalizeSourceProfilesCommand{
		Limit:   10,
		Trigger: "test",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, updateResult.RecordsSeen)
	require.EqualValues(t, 1, updateResult.CompaniesUpserted)

	newCompanyID := requireActiveAriregisterCompany(t, tx, registryCode, newRawRecordID, newPayloadHash)
	require.NotEqual(t, companyID, newCompanyID)

	var statusCounts struct {
		Active     int64
		Superseded int64
	}
	err = tx.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE row_status = 'active'),
  count(*) FILTER (WHERE row_status = 'superseded')
FROM ariregister_source.companies
WHERE registry_code = $1
`, registryCode).Scan(&statusCounts.Active, &statusCounts.Superseded)
	require.NoError(t, err)
	require.EqualValues(t, 1, statusCounts.Active)
	require.EqualValues(t, 1, statusCounts.Superseded)
}

func TestNormalizeSourceProfilesWithCopyExplicitIDRerunDoesNotDuplicateRowsWithoutSourceEntryID(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	registryCode := "t5-missing-id-" + uuid.NewString()
	rawRecordID := uuid.New()
	payload := ariregisterGeneralDataPayloadWithoutSourceEntryIDs(t, registryCode, "Missing Entry ID OÜ")
	payloadHash := uuid.NewString()
	insertAriregisterRawRecord(t, tx, rawRecordID, registryCode, "Missing Entry ID OÜ", payload, payloadHash)

	command := NormalizeSourceProfilesCommand{
		IDs:     []string{rawRecordID.String()},
		Limit:   10,
		Trigger: "test",
	}
	firstResult, err := New(tx).NormalizeSourceProfilesWithCopy(ctx, command)
	require.NoError(t, err)
	require.EqualValues(t, 1, firstResult.RecordsSeen)
	require.EqualValues(t, 1, firstResult.AddressesUpserted)
	require.EqualValues(t, 1, firstResult.CapitalUpserted)
	require.EqualValues(t, 1, firstResult.AnnualReportsUpserted)

	companyID := requireActiveAriregisterCompany(t, tx, registryCode, rawRecordID, payloadHash)
	childTables := []string{
		"company_names",
		"company_statuses",
		"legal_forms",
		"addresses",
		"capital",
		"financial_year_periods",
		"annual_reports",
		"articles",
		"registry_notes",
	}
	for _, table := range childTables {
		requireAriregisterChildCount(t, tx, table, companyID, 1)
	}

	secondResult, err := New(tx).NormalizeSourceProfilesWithCopy(ctx, command)
	require.NoError(t, err)
	require.EqualValues(t, 1, secondResult.RecordsSeen)

	for _, table := range childTables {
		requireAriregisterChildCount(t, tx, table, companyID, 1)
	}
}

func TestNormalizeSourceProfilesCopySQLDoesNotUseNullableChildConflictTargets(t *testing.T) {
	mergeSQL := strings.Join([]string{
		mergeSourceProfileCompanyNamesSQL,
		mergeSourceProfileCompanyStatusesSQL,
		mergeSourceProfileLegalFormsSQL,
		mergeSourceProfileAddressesSQL,
		mergeSourceProfileCapitalSQL,
		mergeSourceProfileFinancialYearPeriodsSQL,
		mergeSourceProfileAnnualReportsSQL,
		mergeSourceProfileArticlesSQL,
		mergeSourceProfileRegistryNotesSQL,
	}, "\n")

	require.NotContains(t, mergeSQL, "ON CONFLICT (company_id, source_entry_id)")
	require.NotContains(t, mergeSQL, "ON CONFLICT (company_id, card_region, card_number, card_type, entry_number, status_code)")
	require.Contains(t, createSourceProfileCopyStageTablesSQL, "stage_key text NOT NULL")
}

func ariregisterGeneralDataPayload(t *testing.T, registryCode, legalName string) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"ariregistri_kood": registryCode,
		"nimi":             legalName,
		"yldandmed": map[string]any{
			"esmaregistreerimise_kpv": "05.06.2023",
			"staatus":                 "R",
			"staatus_tekstina":        "Registrisse kantud",
			"oiguslik_vorm":           "OÜ",
			"oiguslik_vorm_nr":        5,
			"oiguslik_vorm_tekstina":  "Osaühing",
			"tegutseb_tekstina":       "Jah",
			"aadressid": []map[string]any{{
				"kirje_id":          1,
				"riik":              "EST",
				"riik_tekstina":     "Eesti",
				"ehak":              "0596",
				"ehak_nimetus":      "Pirita linnaosa, Tallinn, Harju maakond",
				"tanav_maja_korter": "Regati pst 12",
				"postiindeks":       "11911",
				"aadress_ads__ads_normaliseeritud_taisaadress": "Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12",
			}},
			"sidevahendid": []map[string]any{
				{"kirje_id": 2, "liik": "EMAIL", "liik_tekstina": "Elektronposti aadress", "sisu": "info@example.ee"},
				{"kirje_id": 3, "liik": "WWW", "liik_tekstina": "Interneti WWW aadress", "sisu": "https://example.ee/"},
			},
			"teatatud_tegevusalad": []map[string]any{{
				"kirje_id":                4,
				"emtak_kood":              "73111",
				"emtak_tekstina":          "Reklaamiagentuuride tegevus",
				"emtak_versioon":          3,
				"emtak_versioon_tekstina": "EMTAK 2025",
				"nace_kood":               "73.11",
				"on_pohitegevusala":       true,
			}},
			"kapitalid": []map[string]any{{
				"kirje_id":                  5,
				"kapitali_suurus":           "0.01",
				"kapitali_valuuta":          "EUR",
				"kapitali_valuuta_tekstina": "euro",
			}},
			"info_majandusaasta_aruannetest": []map[string]any{{
				"kirje_id":                         6,
				"majandusaasta_perioodi_algus_kpv": "01.01.2024",
				"majandusaasta_perioodi_lopp_kpv":  "31.12.2024",
				"tootajate_arv":                    "0",
				"tegevusala_emtak_kood":            "73111",
				"tegevusala_emtak_tekstina":        "Reklaamiagentuuride tegevus",
			}},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

func ariregisterGeneralDataPayloadWithoutSourceEntryIDs(t *testing.T, registryCode, legalName string) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"ariregistri_kood": registryCode,
		"nimi":             legalName,
		"yldandmed": map[string]any{
			"esmaregistreerimise_kpv": "05.06.2023",
			"staatus":                 "R",
			"staatus_tekstina":        "Registrisse kantud",
			"oiguslik_vorm":           "OÜ",
			"oiguslik_vorm_nr":        5,
			"oiguslik_vorm_tekstina":  "Osaühing",
			"arinimed": []map[string]any{{
				"sisu":      "Missing Entry ID Historic OÜ",
				"algus_kpv": "01.01.2024",
			}},
			"staatused": []map[string]any{{
				"staatus":          "R",
				"staatus_tekstina": "Registrisse kantud",
				"algus_kpv":        "01.01.2024",
			}},
			"oiguslikud_vormid": []map[string]any{{
				"sisu":          "OÜ",
				"sisu_nr":       5,
				"sisu_tekstina": "Osaühing",
				"algus_kpv":     "01.01.2024",
			}},
			"aadressid": []map[string]any{{
				"riik":              "EST",
				"riik_tekstina":     "Eesti",
				"ehak":              "0596",
				"ehak_nimetus":      "Pirita linnaosa, Tallinn, Harju maakond",
				"tanav_maja_korter": "Regati pst 12",
				"postiindeks":       "11911",
				"aadress_ads__ads_normaliseeritud_taisaadress": "Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12",
				"algus_kpv": "01.01.2024",
			}},
			"sidevahendid": []map[string]any{
				{"liik": "EMAIL", "liik_tekstina": "Elektronposti aadress", "sisu": "info@example.ee"},
				{"liik": "WWW", "liik_tekstina": "Interneti WWW aadress", "sisu": "https://example.ee/"},
			},
			"teatatud_tegevusalad": []map[string]any{{
				"emtak_kood":              "73111",
				"emtak_tekstina":          "Reklaamiagentuuride tegevus",
				"emtak_versioon":          3,
				"emtak_versioon_tekstina": "EMTAK 2025",
				"nace_kood":               "73.11",
				"on_pohitegevusala":       true,
			}},
			"kapitalid": []map[string]any{{
				"kapitali_suurus":           "0.01",
				"kapitali_valuuta":          "EUR",
				"kapitali_valuuta_tekstina": "euro",
				"algus_kpv":                 "01.01.2024",
			}},
			"majandusaastad": []map[string]any{{
				"maj_aasta_algus": "01.01",
				"maj_aasta_lopp":  "31.12",
				"algus_kpv":       "01.01.2024",
			}},
			"info_majandusaasta_aruannetest": []map[string]any{{
				"majandusaasta_perioodi_algus_kpv": "01.01.2024",
				"majandusaasta_perioodi_lopp_kpv":  "31.12.2024",
				"tootajate_arv":                    "0",
				"tegevusala_emtak_kood":            "73111",
				"tegevusala_emtak_tekstina":        "Reklaamiagentuuride tegevus",
			}},
			"pohikirjad": []map[string]any{{
				"kinnitamise_kpv": "01.01.2024",
				"muutmise_kpv":    "02.01.2024",
				"selgitus":        "Põhikiri kinnitatud",
			}},
			"markused_kaardil": []map[string]any{{
				"tyyp":          "NOTICE",
				"tyyp_tekstina": "Märkus",
				"sisu":          "Kaardil märkus",
				"algus_kpv":     "01.01.2024",
			}},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

func insertAriregisterRawRecord(t *testing.T, tx pgx.Tx, id uuid.UUID, registryCode string, legalName string, payload json.RawMessage, payloadHash string) {
	t.Helper()
	_, err := tx.Exec(t.Context(), `
INSERT INTO ariregister_workflow.raw_records (
  id,
  source_native_id,
  registry_code,
  legal_name,
  registration_status,
  legal_form,
  website,
  email,
  phone,
  country_iso2,
  raw_payload,
  payload_hash
) VALUES ($1, $2, $2, $3, 'Registrisse kantud', 'Osaühing', 'https://example.ee/', 'info@example.ee', '+372 5555555', 'EE', $4::jsonb, $5)
`, id, registryCode, legalName, payload, payloadHash)
	require.NoError(t, err)
}

func requireActiveAriregisterCompany(t *testing.T, tx pgx.Tx, registryCode string, rawRecordID uuid.UUID, payloadHash string) uuid.UUID {
	t.Helper()
	var companyID uuid.UUID
	err := tx.QueryRow(t.Context(), `
SELECT id
FROM ariregister_source.companies
WHERE registry_code = $1
  AND raw_record_id = $2
  AND payload_hash = $3
  AND row_status = 'active'
`, registryCode, rawRecordID, payloadHash).Scan(&companyID)
	require.NoError(t, err)
	return companyID
}

func requireAriregisterChildCount(t *testing.T, tx pgx.Tx, table string, companyID uuid.UUID, expected int64) {
	t.Helper()
	query := "SELECT count(*) FROM ariregister_source." + table + " WHERE company_id = $1"
	var count int64
	err := tx.QueryRow(t.Context(), query, companyID).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, expected, count)
}
