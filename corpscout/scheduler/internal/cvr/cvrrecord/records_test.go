package cvrrecord_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/cvr/cvrrecord"
)

func TestNewRecordExtractsOfficialCVRSourceShape(t *testing.T) {
	raw := json.RawMessage(`{
		"Vrvirksomhed": {
			"cvrNummer": 12345678,
			"reklamebeskyttet": true,
			"sidstOpdateret": "2026-06-01T12:00:00.000+02:00",
			"virksomhedMetadata": {
				"nyesteNavn": {"navn": "Dansk Test ApS"},
				"nyesteStatus": "NORMAL",
				"nyesteVirksomhedsform": {"kortBeskrivelse": "APS"}
			},
			"hjemmeside": [{"kontaktoplysning": "https://example.dk"}],
			"elektroniskPost": [{"kontaktoplysning": "info@example.dk"}],
			"telefonNummer": [{"kontaktoplysning": "+45 12345678"}]
		}
	}`)

	record, err := cvrrecord.NewRecord(raw)

	require.NoError(t, err)
	require.Equal(t, "12345678", record.CVRNumber)
	require.Equal(t, "Dansk Test ApS", record.CompanyName)
	require.Equal(t, "NORMAL", record.RegistrationStatus)
	require.Equal(t, "APS", record.CompanyType)
	require.Equal(t, "https://example.dk", record.Website)
	require.Equal(t, "info@example.dk", record.Email)
	require.Equal(t, "+45 12345678", record.Phone)
	require.NotNil(t, record.MarketingProtected)
	require.True(t, *record.MarketingProtected)
	require.NotNil(t, record.SourceUpdatedAt)
	require.NotEmpty(t, record.PayloadHash)
	require.JSONEq(t, string(raw), string(record.RawPayload))
}

func TestNewRecordUnwrapsElasticsearchHitSource(t *testing.T) {
	raw := json.RawMessage(`{
		"_id": "company-87654321",
		"_source": {
			"Vrvirksomhed": {
				"cvrNummer": "87654321",
				"virksomhedMetadata": {"nyesteNavn": {"navn": "Wrapped ApS"}}
			}
		}
	}`)

	record, err := cvrrecord.NewRecord(raw)

	require.NoError(t, err)
	require.Equal(t, "87654321", record.CVRNumber)
	require.Equal(t, "Wrapped ApS", record.CompanyName)
	require.JSONEq(t, `{"Vrvirksomhed":{"cvrNummer":"87654321","virksomhedMetadata":{"nyesteNavn":{"navn":"Wrapped ApS"}}}}`, string(record.RawPayload))
}

func TestNewRecordRequiresCVRNumber(t *testing.T) {
	_, err := cvrrecord.NewRecord(json.RawMessage(`{"Vrvirksomhed":{"virksomhedMetadata":{"nyesteNavn":{"navn":"Missing CVR"}}}}`))

	require.Error(t, err)
	require.Contains(t, err.Error(), "cvr number is required")
}
