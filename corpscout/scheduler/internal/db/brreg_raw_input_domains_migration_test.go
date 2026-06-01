package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputDomainsMigrationDefinesBridgeTable(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000048_brreg_raw_input_domains.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_domains")
	require.Contains(t, sql, "raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE")
	require.Contains(t, sql, "domain_id UUID NOT NULL REFERENCES domains(id)")
	require.Contains(t, sql, "action_id UUID REFERENCES brreg_raw_input_actions(id) ON DELETE SET NULL")
	require.Contains(t, sql, "signal IN ('manual', 'wikidata', 'certsh', 'whois', 'search', 'heuristic')")
	require.Contains(t, sql, "status IN ('active', 'removed')")
	require.Contains(t, sql, "UNIQUE (raw_input_id, domain_id, signal)")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_raw_status")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_domain_status")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_domains_action")
	require.Contains(t, sql, "GRANT SELECT ON brreg_raw_input_domains TO corpscout_anon")
}

func TestBrregRawInputDomainsMigrationAddsSourceRawInputCount(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000048_brreg_raw_input_domains.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "connected_domain_count")
	require.Contains(t, sql, "FROM brreg_raw_input_domains brid")
	require.Contains(t, sql, "brid.status = 'active'")
	require.Contains(t, sql, "0::bigint AS connected_domain_count")
	require.Contains(t, sql, "GRANT SELECT ON v_source_raw_inputs TO corpscout_anon")
}
