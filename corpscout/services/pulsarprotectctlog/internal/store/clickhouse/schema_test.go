package clickhouse

import (
	"os"
	"strings"
	"testing"
)

const lastIngestedAtDefinition = "last_ingested_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC')) DEFAULT toDateTime64(0, 3, 'UTC')"

func TestFreshHostnameSchemaIncludesIngestionWatermark(t *testing.T) {
	if !strings.Contains(hostnamesDDL, lastIngestedAtDefinition) {
		t.Fatalf("hostnamesDDL does not contain %q", lastIngestedAtDefinition)
	}
}

func TestLastIngestedAtMigrationMatchesFreshSchema(t *testing.T) {
	up := readMigration(t, "../../../clickhouse/migrations/000002_hostname_last_ingested_at.up.sql")
	down := readMigration(t, "../../../clickhouse/migrations/000002_hostname_last_ingested_at.down.sql")

	if !strings.Contains(up, lastIngestedAtDefinition) {
		t.Fatalf("up migration does not contain %q", lastIngestedAtDefinition)
	}
	if !strings.Contains(down, "DROP COLUMN IF EXISTS last_ingested_at") {
		t.Fatal("down migration does not remove last_ingested_at")
	}
}

func readMigration(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	return string(content)
}
