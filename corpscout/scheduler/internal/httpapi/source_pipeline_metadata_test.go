package httpapi_test

import (
	"os"
	"strings"
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
	assert.Contains(t, sql, `"base_url_env": "CVR_FILEDOWNLOAD_BASE_URL"`)
	assert.Contains(t, sql, `"auth_env": "CVR_FILEDOWNLOAD_BEARER_TOKEN"`)
	assert.Contains(t, sql, `"api_key_env": "CVR_FILEDOWNLOAD_API_KEY"`)
	assert.False(t, strings.Contains(sql, `"auth_env": "DATAFORDELER_CVR_TOKEN"`))
}
