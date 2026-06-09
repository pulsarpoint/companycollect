package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildNativeSelectSQL(t *testing.T) {
	sql, err := BuildNativeSelectSQL(TableConfig{
		Table: "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"ingested_at":      "DateTime64(3, 'UTC')",
		},
	}, "/exports/companies.parquet", "11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	require.Contains(t, sql, "SELECT *, now64(3) AS `ingested_at`, toUUID('11111111-1111-1111-1111-111111111111') AS `source_export_id`")
	require.Contains(t, sql, "FROM file('/exports/companies.parquet', Parquet)")
	require.Contains(t, sql, "FORMAT Native")
}

func TestParseClickHouseNativeURL(t *testing.T) {
	target, err := ParseClickHouseNativeURL("clickhouse://host.docker.internal:9002?username=default&password=change-me&database=corpscout_sources")
	require.NoError(t, err)
	require.Equal(t, ClickHouseTarget{
		Host:     "host.docker.internal",
		Port:     "9002",
		Username: "default",
		Password: "change-me",
		Database: "corpscout_sources",
	}, target)
}

func TestDockerMountRoot(t *testing.T) {
	require.Equal(t, "/Users", DockerMountRoot("/Users/graovic/export/companies.parquet"))
	require.Equal(t, "/", DockerMountRoot("/var/tmp/export/companies.parquet"))
}
