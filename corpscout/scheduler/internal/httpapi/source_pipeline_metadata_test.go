package httpapi_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourcePipelineMetadataMatchesDownloaderContracts(t *testing.T) {
	migration, err := os.ReadFile("../../../database/migrations/000040_source_pipeline_modules.up.sql")
	require.NoError(t, err)
	sql := string(migration)

	assert.Contains(t, sql, "https://goldencopy.gleif.org/api/v2/golden-copies/publishes")
	assert.Contains(t, sql, "latest.json?delta=LastDay")

	cvrMigration, err := os.ReadFile("../../../database/migrations/000090_cvr_workflow_store.up.sql")
	require.NoError(t, err)
	cvrSQL := string(cvrMigration)

	assert.Contains(t, cvrSQL, "CREATE SCHEMA IF NOT EXISTS cvr_workflow")
	assert.Contains(t, cvrSQL, "cvr_workflow.raw_records")
	assert.Contains(t, cvrSQL, "https://distribution.virk.dk/cvr-permanent/virksomhed/_search")
	assert.Contains(t, cvrSQL, "'scroll_keepalive', '1m'")
	assert.Contains(t, cvrSQL, "'CORPSCOUT_CVR_DISTRIBUTION_USERNAME'")
	assert.Contains(t, cvrSQL, "'CORPSCOUT_CVR_DISTRIBUTION_PASSWORD'")
	assert.Contains(t, cvrSQL, "DROP TABLE IF EXISTS cvr_company_raw_inputs")
	assert.NotContains(t, cvrSQL, "CVR_FILEDOWNLOAD")
	assert.NotContains(t, cvrSQL, "datafordeler.dk")
}
