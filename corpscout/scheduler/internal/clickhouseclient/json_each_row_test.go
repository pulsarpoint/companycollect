package clickhouseclient

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseNativeURL(t *testing.T) {
	target, err := ParseNativeURL("clickhouse://companycollect:9002?username=default&password=change-me&database=corpscout_sources")
	require.NoError(t, err)
	require.Equal(t, Target{
		Host:     "companycollect",
		Port:     "9002",
		Username: "default",
		Password: "change-me",
		Database: "corpscout_sources",
	}, target)
}

func TestBuildInsertQuery(t *testing.T) {
	query := BuildInsertQuery("corpscout_sources", "fi_prhytj_companies", []string{"business_id", "legal_name"})
	require.Equal(t, "INSERT INTO `corpscout_sources`.`fi_prhytj_companies` (`business_id`, `legal_name`) FORMAT JSONEachRow", query)
}

func TestEncodeJSONEachRow(t *testing.T) {
	body, err := EncodeJSONEachRow([]map[string]any{
		{"business_id": "0100130-4", "legal_name": "Dynava Oy"},
		{"business_id": "0112038-9", "legal_name": "Example"},
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"business_id":"0100130-4","legal_name":"Dynava Oy"}`, lines[0])
	require.JSONEq(t, `{"business_id":"0112038-9","legal_name":"Example"}`, lines[1])
}

func TestClickHouseClientDockerArgsAddsCompanycollectHost(t *testing.T) {
	t.Setenv("COMPANYCOLLECT_HOST_IP", "203.0.113.10")
	args := clickHouseClientDockerArgs("clickhouse/clickhouse-server:26.5", Target{
		Host:     "companycollect",
		Port:     "9002",
		Username: "default",
		Password: "secret",
		Database: "corpscout_sources",
	}, "INSERT INTO `corpscout_sources`.`fi_prhytj_companies` FORMAT JSONEachRow")

	require.Contains(t, args, "companycollect:203.0.113.10")
	require.Contains(t, args, "clickhouse-client")
	require.Contains(t, args, "--password")
	require.Contains(t, args, "secret")
	require.Contains(t, args, "--query")
}
