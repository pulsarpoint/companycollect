package activities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/contracts"
)

func TestImportSEHVDBulk_InsertsOrganisationPayloadFromJSON(t *testing.T) {
	path := writeTempJSON(t, map[string]any{
		"data": []map[string]any{{
			"identitetsbeteckning": "5566778899",
			"organisationsnamn":    "Exempel Sverige AB",
			"organisationsform": map[string]any{
				"kod":       "AB",
				"klartext": "Aktiebolag",
			},
			"avregistreradOrganisation": map[string]any{
				"avregistrerad": false,
			},
			"verksamhetsbeskrivning": "Konsultverksamhet inom IT",
			"naringsgrenOrganisation": []map[string]any{{
				"sni": map[string]any{"kod": "62010", "klartext": "Dataprogrammering"},
			}},
			"postadressOrganisation": map[string]any{
				"postadress": map[string]any{"postnummer": "11122", "postort": "Stockholm"},
			},
		}},
	})
	db := &recordingDB{}
	acts := activities.NewGoActivitiesWithDB(db)

	written, err := acts.ImportSEHVDBulk(context.Background(), contracts.ImportSEHVDBulkParams{
		RunID: "run-se-001",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "se",
			Dataset:  "organisationer",
			FilePath: path,
			Format:   "json",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Len(t, db.entries, 1)

	entry := db.entries[0]
	require.Contains(t, entry.query, "INSERT INTO se_workflow.raw_records")
	require.Contains(t, entry.query, "ON CONFLICT (organization_number, payload_hash)")
	require.Equal(t, "5566778899", entry.args[0])
	require.Equal(t, "5566778899", entry.args[1])
	require.Equal(t, "Exempel Sverige AB", entry.args[2])
	require.Equal(t, "active", entry.args[3])
	require.Equal(t, "Aktiebolag", entry.args[4])
	require.Equal(t, "Konsultverksamhet inom IT", entry.args[5])
	require.Equal(t, "SE", entry.args[6])

	sniCodes := entry.args[7].([]byte)
	require.JSONEq(t, `[{"code":"62010","label":"Dataprogrammering"}]`, string(sniCodes))

	postalAddress := entry.args[8].([]byte)
	require.JSONEq(t, `{"post_code":"11122","city":"Stockholm"}`, string(postalAddress))

	rawPayload := entry.args[9].([]byte)
	require.Equal(t, sha256Hex(rawPayload), entry.args[10])
	require.Equal(t, "run-se-001", entry.args[11])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rawPayload, &payload))
	require.Equal(t, "5566778899", payload["organization_number"])
	require.Equal(t, "Exempel Sverige AB", payload["organization_name"])
	require.Equal(t, "Aktiebolag", payload["legal_form"])
	require.Equal(t, "Konsultverksamhet inom IT", payload["business_description"])
	require.NotEmpty(t, payload["source_record"])
}

func TestImportSEHVDBulk_IncludesSourceDatasetsOnInsertError(t *testing.T) {
	path := writeTempJSON(t, []map[string]any{{
		"organization_number": "5566778899",
		"organization_name":   "Exempel Sverige AB",
	}})
	db := newFailingRecordingDB()
	acts := activities.NewGoActivitiesWithDB(db)

	_, err := acts.ImportSEHVDBulk(context.Background(), contracts.ImportSEHVDBulkParams{
		RunID: "run-se-error",
		Files: []contracts.DownloadedSourceFile{{
			Source:   "se",
			Dataset:  "organisationer",
			FilePath: path,
			Format:   "json",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "se:organisationer")
	require.ErrorContains(t, err, "batch offset 0")
}
