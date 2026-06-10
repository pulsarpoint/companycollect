package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestCompanySourceExistingArtifactFixtureShape(t *testing.T) {
	body, err := os.ReadFile("../../../database/fixtures/company_source_existing_artifact_runs.sql")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	sql := string(body)

	required := []string{
		"INSERT INTO data_source_action_runs",
		"ON CONFLICT (id) DO UPDATE",
		"fixture_key",
		"existing_source_artifact",
		"run_dir",
		"source_file_path",
		"/var/lib/corpscout/company-source-fixtures/finland/sources/prhytj/runs/20260608T201348Z-prhytj",
		"/var/lib/corpscout/company-source-fixtures/united_states/sources/coloradoentities/runs/20260608T233837Z-coloradoentities",
		"/var/lib/corpscout/company-source-fixtures/united_states/sources/irseobmf/runs/20260608T203550Z-irseobmf",
		"/var/lib/corpscout/company-source-fixtures/united_states/sources/secedgar/runs/20260608T203500Z-secedgar",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("fixture missing %q", needle)
		}
	}

	forbidden := []string{
		"/Users/graovic",
		"companycollect/companies/data",
	}
	for _, needle := range forbidden {
		if strings.Contains(sql, needle) {
			t.Fatalf("fixture should not store host-only path %q", needle)
		}
	}
}

func TestCompanySourceFixtureVolumeMountExists(t *testing.T) {
	body, err := os.ReadFile("../../../docker-compose.yml")
	if err != nil {
		t.Fatalf("read docker compose: %v", err)
	}
	compose := string(body)

	mount := "../companies/data:/var/lib/corpscout/company-source-fixtures:ro"
	if !strings.Contains(compose, mount) {
		t.Fatalf("docker-compose.yml missing source fixture mount %q", mount)
	}
}
