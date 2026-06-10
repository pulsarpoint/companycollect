package nace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	domainnace "github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

func TestSyncNACETaxonomyActivityRejectsMissingSourceURL(t *testing.T) {
	actions := NewActions(nil, nil)

	_, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: "workflow-1",
		Revision:           domainnace.DefaultRevision,
	})

	require.ErrorContains(t, err, "nace source url is required")
}

func TestSyncNACETaxonomyActivityImportsNewHash(t *testing.T) {
	pool := newNACETestPool(t)
	server := newNACEFixtureServer(t)
	actions := NewActions(pool, server.Client())

	result, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: "nace-test-" + uuid.NewString(),
		Revision:           "test-" + uuid.NewString(),
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, naceworkflow.SyncStatusSucceeded, result.Status)
	require.NotEmpty(t, result.ImportRunID)
	require.NotEmpty(t, result.SourceFileID)
	require.NotEmpty(t, result.ContentSHA256)
	require.Equal(t, int32(3), result.RecordsSeen)
	require.Equal(t, int32(3), result.RecordsImported)
}

func TestSyncNACETaxonomyActivityLinksParentsByNormalizedParentCode(t *testing.T) {
	pool := newNACETestPool(t)
	server := newNACEFixtureServerWithBody(t, naceNormalizedParentFixtureRDFXML)
	actions := NewActions(pool, server.Client())
	revision := "test-" + uuid.NewString()

	_, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: "nace-test-" + uuid.NewString(),
		Revision:           revision,
		SourceURL:          server.URL,
		Trigger:            "test",
	})
	require.NoError(t, err)

	var parentCode string
	err = pool.QueryRow(t.Context(), `
SELECT parent.code
FROM nace_codes child
JOIN nace_classifications classification ON classification.id = child.classification_id
JOIN nace_codes parent ON parent.id = child.parent_id
WHERE classification.revision = $1
  AND child.code = '01.11'
`, revision).Scan(&parentCode)
	require.NoError(t, err)
	require.Equal(t, "01.1", parentCode)
}

func TestSyncNACETaxonomyActivitySkipsAlreadyProcessedHash(t *testing.T) {
	pool := newNACETestPool(t)
	server := newNACEFixtureServer(t)
	actions := NewActions(pool, server.Client())
	revision := "test-" + uuid.NewString()

	first, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: "nace-test-" + uuid.NewString(),
		Revision:           revision,
		SourceURL:          server.URL,
		Trigger:            "test",
	})
	require.NoError(t, err)
	require.Equal(t, naceworkflow.SyncStatusSucceeded, first.Status)

	second, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: "nace-test-" + uuid.NewString(),
		Revision:           revision,
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, naceworkflow.SyncStatusSkipped, second.Status)
	require.Equal(t, first.SourceFileID, second.SourceFileID)
	require.Equal(t, first.ContentSHA256, second.ContentSHA256)
	require.Contains(t, second.Message, "already processed")
}

func TestSyncNACETaxonomyActivityReturnsPreviousSucceededRunForSameWorkflowRetry(t *testing.T) {
	pool := newNACETestPool(t)
	server := newNACEFixtureServer(t)
	actions := NewActions(pool, server.Client())
	workflowID := "nace-test-" + uuid.NewString()

	first, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: workflowID,
		Revision:           "test-" + uuid.NewString(),
		SourceURL:          server.URL,
		Trigger:            "test",
	})
	require.NoError(t, err)
	require.Equal(t, naceworkflow.SyncStatusSucceeded, first.Status)
	require.Equal(t, int32(3), first.RecordsImported)

	second, err := actions.SyncNACETaxonomyActivity(t.Context(), SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: workflowID,
		Revision:           "ignored-on-terminal-retry",
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, naceworkflow.SyncStatusSucceeded, second.Status)
	require.Equal(t, first.ImportRunID, second.ImportRunID)
	require.Equal(t, first.SourceFileID, second.SourceFileID)
	require.Equal(t, first.ContentSHA256, second.ContentSHA256)
	require.Equal(t, int32(3), second.RecordsSeen)
	require.Equal(t, int32(3), second.RecordsImported)
}

func newNACETestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CORPSCOUT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CORPSCOUT_TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var sourceFilesRegclass *string
	err = pool.QueryRow(context.Background(), "SELECT to_regclass('nace_source_files')::text").Scan(&sourceFilesRegclass)
	require.NoError(t, err)
	if sourceFilesRegclass == nil {
		t.Skip("CORPSCOUT_TEST_DATABASE_URL must point to a migrated database with nace_source_files")
	}
	return pool
}

func newNACEFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newNACEFixtureServerWithBody(t, naceFixtureRDFXML)
}

func newNACEFixtureServerWithBody(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rdf+xml")
		w.Header().Set("ETag", "nace-test-etag")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

const naceFixtureRDFXML = `<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/M">
    <skos:notation>M</skos:notation>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68">
    <skos:notation>68</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/M"/>
    <skos:prefLabel xml:lang="en">Real estate activities</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/68.20">
    <skos:notation>68.20</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/68.2"/>
    <skos:prefLabel xml:lang="en">Renting and operating of own or leased real estate</skos:prefLabel>
    <skos:scopeNote xml:lang="en">This class includes landlords.</skos:scopeNote>
  </skos:Concept>
</rdf:RDF>`

const naceNormalizedParentFixtureRDFXML = `<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/A">
    <skos:notation>A</skos:notation>
    <skos:prefLabel xml:lang="en">Agriculture, forestry and fishing</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/01">
    <skos:notation>01</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/A"/>
    <skos:prefLabel xml:lang="en">Crop and animal production</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/01.1">
    <skos:notation>01.1</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/01"/>
    <skos:prefLabel xml:lang="en">Growing of non-perennial crops</skos:prefLabel>
  </skos:Concept>
  <skos:Concept rdf:about="http://data.europa.eu/ux2/nace2.1/01.11">
    <skos:notation>01.11</skos:notation>
    <skos:broader rdf:resource="http://data.europa.eu/ux2/nace2.1/011"/>
    <skos:prefLabel xml:lang="en">Growing of cereals</skos:prefLabel>
  </skos:Concept>
</rdf:RDF>`
