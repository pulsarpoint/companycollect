package brregdb

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestNormalizeSourceProfilesUpsertsRawBrregProfile(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	rawRecordID := uuid.New()
	organizationNumber := "test-" + uuid.NewString()
	payload := map[string]any{
		"navn":                          "BORTIGARD TEST AS",
		"konkurs":                       false,
		"erIKonsern":                    false,
		"underAvvikling":                false,
		"harRegistrertAntallAnsatte":    false,
		"registrertIMvaregisteret":      true,
		"registrertIForetaksregisteret": true,
		"organisasjonsform": map[string]any{
			"kode":        "AS",
			"beskrivelse": "Aksjeselskap",
		},
		"stiftelsesdato": "1975-06-04",
		"aktivitet": []string{
			"Drive utleie av fast eiendom.",
		},
		"vedtektsfestetFormaal": []string{
			"Drive utleie av fast eiendom.",
		},
		"forretningsadresse": map[string]any{
			"adresse":       []string{"Løkkeveien 18"},
			"postnummer":    "3085",
			"poststed":      "HOLMESTRAND",
			"kommune":       "HOLMESTRAND",
			"kommunenummer": "3903",
			"land":          "Norge",
			"landkode":      "NO",
		},
		"naeringskode1": map[string]any{
			"kode":        "41.000",
			"beskrivelse": "Oppføring av bygninger",
		},
		"telefon": "33051963",
		"kapital": map[string]any{
			"type":         "Aksjekapital",
			"belop":        81870,
			"valuta":       "NOK",
			"innfortDato":  "2012-07-09",
			"antallAksjer": 8187,
		},
	}
	rawPayload, err := json.Marshal(payload)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id,
  source_native_id,
  organization_number,
  organization_name,
  registration_status,
  website,
  country_iso2,
  raw_payload,
  payload_hash
) VALUES ($1, $2, $2, 'BORTIGARD TEST AS', 'active', 'https://bortigard.example', 'NO', $3::jsonb, $4)
`, rawRecordID, organizationNumber, rawPayload, uuid.NewString())
	require.NoError(t, err)

	mismatchedStateResult, err := New(tx).NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		IDs:     []string{rawRecordID.String()},
		Filters: map[string]string{"state": "translated"},
		Limit:   10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, mismatchedStateResult.RecordsSeen)

	result, err := New(tx).NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		IDs:     []string{rawRecordID.String()},
		Filters: map[string]string{"state": "input"},
		Limit:   10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RecordsSeen)
	require.EqualValues(t, 1, result.CompaniesUpserted)
	require.EqualValues(t, 1, result.AddressesUpserted)
	require.EqualValues(t, 1, result.IndustriesUpserted)
	require.EqualValues(t, 1, result.WebsitesUpserted)
	require.EqualValues(t, 1, result.DomainsUpserted)
	require.EqualValues(t, 1, result.ContactsUpserted)
	require.EqualValues(t, 1, result.CapitalUpserted)

	bulkRetryResult, err := New(tx).NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, bulkRetryResult.RecordsSeen)

	explicitRetryResult, err := New(tx).NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		IDs:   []string{rawRecordID.String()},
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, explicitRetryResult.RecordsSeen)

	var companyID uuid.UUID
	var organizationName string
	var activityDescription *string
	err = tx.QueryRow(ctx, `
SELECT id, organization_name, activity_description
FROM brreg_source.companies
WHERE raw_record_id = $1
`, rawRecordID).Scan(&companyID, &organizationName, &activityDescription)
	require.NoError(t, err)
	require.Equal(t, "BORTIGARD TEST AS", organizationName)
	require.NotNil(t, activityDescription)
	require.Contains(t, *activityDescription, "Drive utleie")

	var city string
	err = tx.QueryRow(ctx, `
SELECT city
FROM brreg_source.addresses
WHERE company_id = $1 AND address_type = 'business'
`, companyID).Scan(&city)
	require.NoError(t, err)
	require.Equal(t, "HOLMESTRAND", city)

	var domain string
	err = tx.QueryRow(ctx, `
SELECT normalized_domain
FROM brreg_source.domains
WHERE company_id = $1
`, companyID).Scan(&domain)
	require.NoError(t, err)
	require.Equal(t, "bortigard.example", domain)
}

func TestNormalizeSourceProfilesRealRawRecordSample(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	rows, err := tx.Query(ctx, `
SELECT id::text
FROM brreg_workflow.raw_records
WHERE is_current
ORDER BY organization_number
`)
	require.NoError(t, err)
	defer rows.Close()

	var rawRecordIDs []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		rawRecordIDs = append(rawRecordIDs, id)
	}
	require.NoError(t, rows.Err())
	if len(rawRecordIDs) == 0 {
		t.Skip("brreg_workflow.raw_records contains no current rows")
	}

	result, err := New(tx).NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		IDs:   rawRecordIDs,
		Limit: 0,
	})
	require.NoError(t, err)
	require.EqualValues(t, len(rawRecordIDs), result.RecordsSeen)
	require.EqualValues(t, len(rawRecordIDs), result.CompaniesUpserted)

	var profileCoverage struct {
		CompaniesWithNames        int64
		CompaniesWithFormCode     int64
		CompaniesWithActivity     int64
		BusinessAddresses         int64
		Industries                int64
		Websites                  int64
		Domains                   int64
		Contacts                  int64
		CapitalRows               int64
		CapitalWithOriginalAmount int64
	}
	err = tx.QueryRow(ctx, `
WITH selected_companies AS (
  SELECT id, raw_record_id
  FROM brreg_source.companies
  WHERE raw_record_id::text = ANY($1::text[])
)
SELECT
  count(*) FILTER (WHERE nullif(btrim(c.organization_name), '') IS NOT NULL),
  count(*) FILTER (WHERE nullif(btrim(c.organization_form_code), '') IS NOT NULL),
  count(*) FILTER (WHERE nullif(btrim(c.activity_description), '') IS NOT NULL),
  (SELECT count(*) FROM brreg_source.addresses a JOIN selected_companies sc ON sc.id = a.company_id WHERE a.address_type = 'business'),
  (SELECT count(*) FROM brreg_source.industries i JOIN selected_companies sc ON sc.id = i.company_id),
  (SELECT count(*) FROM brreg_source.websites w JOIN selected_companies sc ON sc.id = w.company_id),
  (SELECT count(*) FROM brreg_source.domains d JOIN selected_companies sc ON sc.id = d.company_id),
  (SELECT count(*) FROM brreg_source.contacts ct JOIN selected_companies sc ON sc.id = ct.company_id),
  (SELECT count(*) FROM brreg_source.capital cap JOIN selected_companies sc ON sc.id = cap.company_id),
  (SELECT count(*) FROM brreg_source.capital cap JOIN selected_companies sc ON sc.id = cap.company_id WHERE cap.original_amount IS NOT NULL)
FROM brreg_source.companies c
JOIN selected_companies sc ON sc.id = c.id
`, rawRecordIDs).Scan(
		&profileCoverage.CompaniesWithNames,
		&profileCoverage.CompaniesWithFormCode,
		&profileCoverage.CompaniesWithActivity,
		&profileCoverage.BusinessAddresses,
		&profileCoverage.Industries,
		&profileCoverage.Websites,
		&profileCoverage.Domains,
		&profileCoverage.Contacts,
		&profileCoverage.CapitalRows,
		&profileCoverage.CapitalWithOriginalAmount,
	)
	require.NoError(t, err)

	t.Logf(
		"BRREG source profile real sample coverage: records=%d companies=%d addresses=%d industries=%d websites=%d domains=%d contacts=%d capital=%d capital_with_amount=%d names=%d forms=%d activities=%d",
		result.RecordsSeen,
		result.CompaniesUpserted,
		profileCoverage.BusinessAddresses,
		profileCoverage.Industries,
		profileCoverage.Websites,
		profileCoverage.Domains,
		profileCoverage.Contacts,
		profileCoverage.CapitalRows,
		profileCoverage.CapitalWithOriginalAmount,
		profileCoverage.CompaniesWithNames,
		profileCoverage.CompaniesWithFormCode,
		profileCoverage.CompaniesWithActivity,
	)
	require.EqualValues(t, len(rawRecordIDs), profileCoverage.CompaniesWithNames)
	require.Greater(t, profileCoverage.BusinessAddresses, int64(0))
	require.Greater(t, profileCoverage.Industries, int64(0))
}
