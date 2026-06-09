package clickhouseclient

import (
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
