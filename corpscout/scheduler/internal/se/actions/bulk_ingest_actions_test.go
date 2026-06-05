package actions

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sedb "github.com/pulsarpoint/corpscout/scheduler/internal/se/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestLoadSEBulkRawRecordsSkipsAlreadyProcessedSourceFileHash(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{
			"identitetsbeteckning":"556677-8899",
			"organisationsnamn":"Exempel Sverige AB",
			"organisationsform":{"kod":"AB","klartext":"Aktiebolag"}
		}]}`))
	}))
	t.Cleanup(server.Close)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{})
	input := LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: "test-se-hash-" + time.Now().Format("20060102150405.000000000"),
		Datasets: []HVDDatasetConfig{{
			Dataset: "organisationer",
			URL:     server.URL + "/organisationer.json",
			Format:  "json",
		}},
		BatchSize: 100,
		Trigger:   "test",
	}

	first, err := actions.LoadSEBulkRawRecords(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.RowsWritten)
	require.Len(t, first.SourceFiles, 1)
	require.Equal(t, "parsed", first.SourceFiles[0].Status)

	input.TemporalWorkflowID += "-again"
	second, err := actions.LoadSEBulkRawRecords(ctx, input)
	require.NoError(t, err)
	require.Zero(t, second.RowsSeen)
	require.Zero(t, second.RowsWritten)
	require.Len(t, second.SourceFiles, 1)
	require.Equal(t, "skipped_duplicate", second.SourceFiles[0].Status)
	require.Equal(t, first.SourceFiles[0].SourceFileID, second.SourceFiles[0].SkippedSourceFileID)
}

func TestLoadSEBulkRawRecordsSkipsDownloadWhenStrongETagMatchesParsedSourceFile(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()

	var getCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"se-hvd-v1"`)
		if r.Method == http.MethodHead {
			return
		}
		atomic.AddInt32(&getCount, 1)
		_, _ = w.Write([]byte(`{"data":[{
			"identitetsbeteckning":"556677-9900",
			"organisationsnamn":"Etag Sverige AB",
			"organisationsform":{"kod":"AB","klartext":"Aktiebolag"}
		}]}`))
	}))
	t.Cleanup(server.Close)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{})
	input := LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: "test-se-etag-" + time.Now().Format("20060102150405.000000000"),
		Datasets: []HVDDatasetConfig{{
			Dataset: "organisationer",
			URL:     server.URL + "/organisationer.json",
			Format:  "json",
		}},
		BatchSize: 100,
		Trigger:   "test",
	}

	first, err := actions.LoadSEBulkRawRecords(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.RowsWritten)
	require.EqualValues(t, 1, atomic.LoadInt32(&getCount))

	input.TemporalWorkflowID += "-again"
	second, err := actions.LoadSEBulkRawRecords(ctx, input)
	require.NoError(t, err)
	require.Zero(t, second.RowsSeen)
	require.Zero(t, second.RowsWritten)
	require.Len(t, second.SourceFiles, 1)
	require.Equal(t, "skipped_duplicate", second.SourceFiles[0].Status)
	require.Equal(t, first.SourceFiles[0].SourceFileID, second.SourceFiles[0].SkippedSourceFileID)
	require.EqualValues(t, 1, atomic.LoadInt32(&getCount))
}

func TestLoadSEBulkRawRecordsUsesDataSourceConfigDatasets(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{
			"identitetsbeteckning":"556677-0001",
			"organisationsnamn":"Konfigurerad Sverige AB",
			"organisationsform":{"kod":"AB","klartext":"Aktiebolag"}
		}]}`))
	}))
	t.Cleanup(server.Close)

	_, err := tx.Exec(ctx, `
		UPDATE data_sources
		SET config = jsonb_build_object(
			'datasets', jsonb_build_array(
				jsonb_build_object(
					'dataset', 'organisationer',
					'url', $1::text,
					'format', 'json'
				)
			)
		)
		WHERE name = 'se'
	`, server.URL+"/organisationer.json")
	require.NoError(t, err)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{})
	result, err := actions.LoadSEBulkRawRecords(ctx, LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: "test-se-config-" + time.Now().Format("20060102150405.000000000"),
		BatchSize:          100,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsWritten)
	require.Len(t, result.SourceFiles, 1)
	require.Equal(t, server.URL+"/organisationer.json", result.SourceFiles[0].SourceURL)
}

func TestLoadSEBulkRawRecordsResolvesConfiguredMetadataDataset(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metadata":
			w.Header().Set("Content-Type", "application/rdf+xml")
			_, _ = fmt.Fprintf(w, `<rdf:RDF xmlns:dcat="http://www.w3.org/ns/dcat#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
				<dcat:Distribution rdf:about="%[1]s/metadata">
					<dcat:accessURL rdf:resource="%[1]s/downloads"/>
				</dcat:Distribution>
			</rdf:RDF>`, server.URL)
		case "/downloads":
			w.Header().Set("Content-Type", "text/html")
			_, _ = fmt.Fprintf(w, `<a href="%s/files/organisationer.json">Organisationer</a>`, server.URL)
		case "/files/organisationer.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{
				"identitetsbeteckning":"556677-0002",
				"organisationsnamn":"Metadata Sverige AB",
				"organisationsform":{"kod":"AB","klartext":"Aktiebolag"}
			}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	_, err := tx.Exec(ctx, `
		UPDATE data_sources
		SET config = jsonb_build_object(
			'datasets', jsonb_build_array(
				jsonb_build_object(
					'dataset', 'organisationer',
					'url', $1::text,
					'format', 'metadata'
				)
			)
		)
		WHERE name = 'se'
	`, server.URL+"/metadata")
	require.NoError(t, err)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{})
	result, err := actions.LoadSEBulkRawRecords(ctx, LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: "test-se-metadata-" + time.Now().Format("20060102150405.000000000"),
		BatchSize:          100,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsWritten)
	require.Len(t, result.SourceFiles, 1)
	require.Equal(t, server.URL+"/files/organisationer.json", result.SourceFiles[0].SourceURL)
	require.Equal(t, "json", result.SourceFiles[0].FileFormat)
}
