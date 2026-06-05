package francedb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestNormalizeSourceProfilesUpsertsAndSupersedesFranceProfile(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := context.Background()
	gateway := New(tx)
	siren := fmt.Sprintf("8%08d", time.Now().UnixNano()%100000000)
	siret := siren + "00042"
	rawLegalUnitID := uuid.New()
	rawEstablishmentID := uuid.New()
	legalPayloadHash := uuid.NewString()
	establishmentPayloadHash := uuid.NewString()

	insertFranceRawLegalUnit(t, tx, rawLegalUnitID, siren, "PULSAR POINT FRANCE", legalPayloadHash)
	insertFranceRawEstablishment(t, tx, rawEstablishmentID, siren, siret, establishmentPayloadHash)

	result, err := gateway.NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		IDs:     []string{rawLegalUnitID.String()},
		Limit:   10,
		Trigger: "test",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RecordsSeen)
	require.EqualValues(t, 1, result.CompaniesUpserted)
	require.EqualValues(t, 1, result.EstablishmentsUpserted)
	require.EqualValues(t, 1, result.AddressesUpserted)
	require.EqualValues(t, 2, result.IndustriesInserted)

	companyID := requireActiveFranceCompany(t, tx, siren, rawLegalUnitID, legalPayloadHash)
	requireFranceChildCount(t, tx, "establishments", companyID, 1)
	requireFranceChildCount(t, tx, "addresses", companyID, 1)
	requireFranceChildCount(t, tx, "industries", companyID, 2)

	retryResult, err := gateway.NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		Limit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, retryResult.RecordsSeen)

	newRawLegalUnitID := uuid.New()
	newPayloadHash := uuid.NewString()
	_, err = tx.Exec(ctx, `
UPDATE france_workflow.raw_legal_units
SET is_current = false
WHERE id = $1
`, rawLegalUnitID)
	require.NoError(t, err)
	insertFranceRawLegalUnit(t, tx, newRawLegalUnitID, siren, "PULSAR POINT FRANCE UPDATED", newPayloadHash)

	updateResult, err := gateway.NormalizeSourceProfiles(ctx, NormalizeSourceProfilesCommand{
		Limit:   10,
		Trigger: "test",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, updateResult.RecordsSeen)
	require.EqualValues(t, 1, updateResult.CompaniesUpserted)
	require.EqualValues(t, 1, updateResult.EstablishmentsUpserted)

	newCompanyID := requireActiveFranceCompany(t, tx, siren, newRawLegalUnitID, newPayloadHash)
	require.NotEqual(t, companyID, newCompanyID)
	requireFranceChildCount(t, tx, "establishments", newCompanyID, 1)
	requireFranceChildCount(t, tx, "addresses", newCompanyID, 1)
	requireFranceChildCount(t, tx, "industries", newCompanyID, 2)

	var statusCounts struct {
		Active     int64
		Superseded int64
	}
	err = tx.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE row_status = 'active'),
  count(*) FILTER (WHERE row_status = 'superseded')
FROM france_source.companies
WHERE siren = $1
`, siren).Scan(&statusCounts.Active, &statusCounts.Superseded)
	require.NoError(t, err)
	require.EqualValues(t, 1, statusCounts.Active)
	require.EqualValues(t, 1, statusCounts.Superseded)
}

func insertFranceRawLegalUnit(t *testing.T, tx pgx.Tx, id uuid.UUID, siren string, name string, payloadHash string) {
	t.Helper()
	_, err := tx.Exec(context.Background(), `
INSERT INTO france_workflow.raw_legal_units (
  id,
  source_native_id,
  siren,
  diffusion_status,
  legal_name,
  administrative_status,
  legal_form_code,
  primary_activity_code,
  primary_activity_nomenclature,
  headquarters_nic,
  raw_payload,
  payload_hash,
  is_current
) VALUES (
  $1, $2, $2, 'O', $3, 'A', '5710', '62.01Z', 'NAFRev2', '00042', jsonb_build_object('siren', $2, 'name', $3), $4, true
)
`, id, siren, name, payloadHash)
	require.NoError(t, err)
}

func insertFranceRawEstablishment(t *testing.T, tx pgx.Tx, id uuid.UUID, siren string, siret string, payloadHash string) {
	t.Helper()
	_, err := tx.Exec(context.Background(), `
INSERT INTO france_workflow.raw_establishments (
  id,
  source_native_id,
  siren,
  nic,
  siret,
  diffusion_status,
  is_headquarters,
  street_number,
  street_type,
  street_label,
  postal_code,
  city_label,
  administrative_status,
  primary_activity_code,
  primary_activity_nomenclature,
  raw_payload,
  payload_hash,
  is_current
) VALUES (
  $1, $3, $2, '00042', $3, 'O', true, '10', 'RUE', 'DE PARIS', '75001', 'PARIS', 'A', '62.01Z', 'NAFRev2', jsonb_build_object('siret', $3), $4, true
)
`, id, siren, siret, payloadHash)
	require.NoError(t, err)
}

func requireActiveFranceCompany(t *testing.T, tx pgx.Tx, siren string, rawLegalUnitID uuid.UUID, payloadHash string) uuid.UUID {
	t.Helper()
	var companyID uuid.UUID
	var organizationName string
	err := tx.QueryRow(context.Background(), `
SELECT id, organization_name
FROM france_source.companies
WHERE siren = $1
  AND raw_legal_unit_id = $2
  AND payload_hash = $3
  AND row_status = 'active'
`, siren, rawLegalUnitID, payloadHash).Scan(&companyID, &organizationName)
	require.NoError(t, err)
	require.NotEmpty(t, organizationName)
	return companyID
}

func requireFranceChildCount(t *testing.T, tx pgx.Tx, table string, companyID uuid.UUID, want int64) {
	t.Helper()
	var count int64
	err := tx.QueryRow(context.Background(), fmt.Sprintf(`
SELECT count(*)::bigint
FROM france_source.%s
WHERE company_id = $1
`, table), companyID).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, want, count)
}
