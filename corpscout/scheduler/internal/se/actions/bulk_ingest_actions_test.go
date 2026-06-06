package actions

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"

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

func TestParseDatasetsFromDataSourceConfigExpandsLegacyMetadataDatasetToDirectZIPs(t *testing.T) {
	datasets, err := parseDatasetsFromDataSourceConfig([]byte(`{
		"datasets": [
			{
				"dataset": "organisationer",
				"url": "https://metadata.bolagsverket.se/store/2/resource/42",
				"format": "metadata"
			}
		]
	}`))

	require.NoError(t, err)
	require.Equal(t, []HVDDatasetConfig{
		{
			Dataset: "bolagsverket",
			URL:     "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip",
			Format:  "zip",
		},
		{
			Dataset: "scb",
			URL:     "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip",
			Format:  "zip",
		},
	}, datasets)
}

func TestResolveInputCapsSEBulkDatabaseBatchSize(t *testing.T) {
	actions := NewBulkIngestActions(nil, nil, BulkIngestConfig{})

	resolved, err := actions.resolveInput(context.Background(), LoadSEBulkRawRecordsActivityInput{
		Datasets: []HVDDatasetConfig{{
			Dataset: "bolagsverket",
			URL:     "https://example.test/bolagsverket_bulkfil.zip",
			Format:  "zip",
		}},
		BatchSize: 1000,
	})

	require.NoError(t, err)
	require.EqualValues(t, maxBulkIngestDBBatchSize, resolved.BatchSize)
}

func TestBulkIngestConfigDefaultsToMountedWorksetRoot(t *testing.T) {
	actions := NewBulkIngestActions(nil, nil, BulkIngestConfig{})

	require.Equal(t, "/var/lib/corpscout/worksets/se-hvd", actions.cfg.StagingRoot)
}

func TestLoadSEBulkRawRecordsDownloadsBothRealHVDZipDatasets(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()

	bolagsverketZip := zipBytes(t, "bolagsverket_bulkfil.txt", []byte(strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"5566778899$ORGNR-IDORG";"1";"SE-LAND";"Exempel Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"Konsultverksamhet inom IT";"Box 1$$STOCKHOLM$11122$SE-LAND"`,
		"",
	}, "\n")))
	scbText := strings.Join([]string{
		"ForAndrTyp\tCOAdress\tForetagsnamn\tFtgStat\tGatuadress\tJEStat\tJurForm\tNamn\tNg1\tNg2\tNg3\tNg4\tNg5\tPeOrgNr\tPostNr\tPostOrt\tRegDatKtid\tReklamsparrtyp\tmCOAdress\tmForetagsnamn\tmFtgStat\tmGatuadress\tmJEStat\tmJurForm\tmNamn\tmNg1\tmNg2\tmNg3\tmNg4\tmNg5\tmPostNr\tmPostOrt\tmRegDatKtid\tmReklamsparrtyp\t",
		"1\tÅSA TEST\t\t1\tSTORGATAN 1\t1\t49\tEXEMPEL SVERIGE AB\t62010\t\t\t\t\t165566778899\t11122\tSTOCKHOLM\t20200102\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t",
		"",
	}, "\n")
	scbEncoded, err := charmap.ISO8859_1.NewEncoder().Bytes([]byte(scbText))
	require.NoError(t, err)
	scbZip := zipBytes(t, "scb_bulkfil_JE_20260601T065839_76.txt", scbEncoded)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload []byte
		switch r.URL.Path {
		case "/bolagsverket_bulkfil.zip":
			payload = bolagsverketZip
		case "/scb_bulkfil.zip":
			payload = scbZip
		default:
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{StagingRoot: t.TempDir()})
	result, err := actions.LoadSEBulkRawRecords(ctx, LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: "test-se-real-zips-" + time.Now().Format("20060102150405.000000000"),
		Datasets: []HVDDatasetConfig{
			{Dataset: "bolagsverket", URL: server.URL + "/bolagsverket_bulkfil.zip", Format: "zip"},
			{Dataset: "scb", URL: server.URL + "/scb_bulkfil.zip", Format: "zip"},
		},
		BatchSize: 100,
		Trigger:   "test",
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.RowsWritten)
	require.Len(t, result.SourceFiles, 2)

	var bolagsverketCount int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.bolagsverket_raw_records
		WHERE organization_number = '5566778899'
		  AND organization_name = 'Exempel Sverige AB'
		  AND is_current
	`).Scan(&bolagsverketCount))
	require.Equal(t, 1, bolagsverketCount)

	var scbCount int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.scb_raw_records
		WHERE pe_org_nr = '165566778899'
		  AND organization_number = '5566778899'
		  AND namn = 'EXEMPEL SVERIGE AB'
		  AND is_current
	`).Scan(&scbCount))
	require.Equal(t, 1, scbCount)
}

func TestProcessSEBulkRawRecordsResumesBolagsverketSourceFileProgress(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()
	metadata := []byte(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-resume-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)
	snapshotID, err := gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: workflowRunID,
		SnapshotKey:   "test-se-resume",
		SnapshotDate:  time.Now(),
		Metadata:      metadata,
	})
	require.NoError(t, err)
	sourceURL := "https://example.test/bolagsverket_bulkfil.zip"
	sourceFileID, err := gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "bolagsverket",
		SourceURL:      sourceURL,
		FileFormat:     "zip",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	_, err = gateway.IngestBolagsverketRawRecords(ctx, []sedb.BolagsverketRawRecord{{
		SourceFileID:           sourceFileID,
		SourceRecordKey:        "1111111111|1",
		RowNumber:              1,
		Organisationsidentitet: "1111111111$ORGNR-IDORG",
		OrganizationNumber:     "1111111111",
		Namnskyddslopnummer:    "1",
		Registreringsland:      "SE-LAND",
		Organisationsnamn:      "First Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02",
		OrganizationName:       "First Sverige AB",
		Organisationsform:      "AB-ORGFO",
		Registreringsdatum:     "2020-01-02",
		RawPayload:             []byte(`{"organisationsidentitet":"1111111111$ORGNR-IDORG"}`),
		PayloadHash:            "first-row-hash",
		RunID:                  "test-se-resume",
		Metadata:               metadata,
	}})
	require.NoError(t, err)
	require.NoError(t, gateway.UpdateSourceFileProgress(ctx, sedb.UpdateSourceFileProgressParams{
		ID:          sourceFileID,
		RowsSeen:    1,
		RowsWritten: 1,
	}))

	sourcePath := filepath.Join(t.TempDir(), "bolagsverket_bulkfil.zip")
	require.NoError(t, os.WriteFile(sourcePath, zipBytes(t, "bolagsverket_bulkfil.txt", []byte(strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"1111111111$ORGNR-IDORG";"1";"SE-LAND";"First Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		`"2222222222$ORGNR-IDORG";"1";"SE-LAND";"Second Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		"",
	}, "\n"))), 0o600))

	actions := NewBulkIngestActions(gateway, nil, BulkIngestConfig{})
	result, err := actions.ProcessSEBulkRawRecords(ctx, ProcessSEBulkRawRecordsActivityInput{
		WorkflowRunID:      workflowRunID.String(),
		SnapshotID:         snapshotID.String(),
		TemporalWorkflowID: "test-se-resume",
		BatchSize:          1,
		Metadata:           metadata,
		SourceFiles: []DownloadedSEBulkSourceFile{{
			ComparedSEBulkSourceFile: ComparedSEBulkSourceFile{
				VerifiedSEBulkSourceFile: VerifiedSEBulkSourceFile{
					Dataset:    "bolagsverket",
					SourceURL:  sourceURL,
					FileFormat: "zip",
				},
			},
			SourceFileID: sourceFileID.String(),
			SourcePath:   sourcePath,
			Status:       "downloaded",
		}},
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.RowsSeen)
	require.EqualValues(t, 2, result.RowsWritten)

	var count int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.bolagsverket_raw_records
		WHERE source_file_id = $1
	`, sourceFileID).Scan(&count))
	require.Equal(t, 2, count)
}

func TestProcessSEBulkRawRecordsReportsSanitizedBolagsverketNULBytes(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()
	metadata := []byte(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-sanitize-nul-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)
	snapshotID, err := gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: workflowRunID,
		SnapshotKey:   "test-se-sanitize-nul",
		SnapshotDate:  time.Now(),
		Metadata:      metadata,
	})
	require.NoError(t, err)
	sourceURL := "https://example.test/bolagsverket_bulkfil.zip"
	sourceFileID, err := gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "bolagsverket",
		SourceURL:      sourceURL,
		FileFormat:     "zip",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	sourcePath := filepath.Join(t.TempDir(), "bolagsverket_bulkfil.zip")
	require.NoError(t, os.WriteFile(sourcePath, zipBytes(t, "bolagsverket_bulkfil.txt", []byte(strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		"\"5566778899$ORGNR-IDORG\";\"1\";\"SE-LAND\";\"Exempel\x00 Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02\";\"AB-ORGFO\";\"\";\"\";\"\";\"2020-01-02\";\"Konsultverksamhet\";\"Box 1$$STOCKHOLM$11122$SE-LAND\"",
		"",
	}, "\n"))), 0o600))

	actions := NewBulkIngestActions(gateway, nil, BulkIngestConfig{})
	result, err := actions.ProcessSEBulkRawRecords(ctx, ProcessSEBulkRawRecordsActivityInput{
		WorkflowRunID:      workflowRunID.String(),
		SnapshotID:         snapshotID.String(),
		TemporalWorkflowID: "test-se-sanitize-nul",
		BatchSize:          1,
		Metadata:           metadata,
		SourceFiles: []DownloadedSEBulkSourceFile{{
			ComparedSEBulkSourceFile: ComparedSEBulkSourceFile{
				VerifiedSEBulkSourceFile: VerifiedSEBulkSourceFile{
					Dataset:    "bolagsverket",
					SourceURL:  sourceURL,
					FileFormat: "zip",
				},
			},
			SourceFileID: sourceFileID.String(),
			SourcePath:   sourcePath,
			Status:       "downloaded",
		}},
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.EqualValues(t, 1, result.RowsWritten)
	require.Equal(t, []SEBulkProcessingIssue{{
		Dataset:      "bolagsverket",
		Code:         "nul_bytes_removed",
		RowsAffected: 1,
		Fields:       []string{"organisationsnamn"},
		SampleRows:   []int32{1},
	}}, result.ProcessingIssues)

	var organizationName string
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT organization_name
		FROM se_workflow.bolagsverket_raw_records
		WHERE source_file_id = $1
	`, sourceFileID).Scan(&organizationName))
	require.Equal(t, "Exempel Sverige AB", organizationName)
}

func TestDownloadSEBulkSourceFilesReusesPartialSourceFileForSameHash(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := sedb.New(tx)
	ctx := context.Background()
	metadata := []byte(`{"trigger":"test"}`)
	payload := zipBytes(t, "bolagsverket_bulkfil.txt", []byte(strings.Join([]string{
		`organisationsidentitet;namnskyddslopnummer;registreringsland;organisationsnamn;organisationsform;avregistreringsdatum;avregistreringsorsak;pagandeAvvecklingsEllerOmstruktureringsforfarande;registreringsdatum;verksamhetsbeskrivning;postadress`,
		`"1111111111$ORGNR-IDORG";"1";"SE-LAND";"First Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		`"2222222222$ORGNR-IDORG";"1";"SE-LAND";"Second Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02";"AB-ORGFO";"";"";"";"2020-01-02";"";""`,
		"",
	}, "\n")))
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])

	oldWorkflowRunID, err := gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-old-partial-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)
	oldSnapshotID, err := gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: oldWorkflowRunID,
		SnapshotKey:   "old-partial",
		SnapshotDate:  time.Now(),
		Metadata:      metadata,
	})
	require.NoError(t, err)
	sourceURL := "https://example.test/bolagsverket_bulkfil.zip"
	oldSourceFileID, err := gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID: oldSnapshotID,
		DatasetKey:     "bolagsverket",
		SourceURL:      sourceURL,
		FileFormat:     "zip",
		PayloadHash:    &payloadHash,
		Status:         "failed",
		Metadata:       metadata,
	})
	require.NoError(t, err)
	_, err = gateway.IngestBolagsverketRawRecords(ctx, []sedb.BolagsverketRawRecord{{
		SourceFileID:           oldSourceFileID,
		SourceRecordKey:        "1111111111|1",
		RowNumber:              1,
		Organisationsidentitet: "1111111111$ORGNR-IDORG",
		OrganizationNumber:     "1111111111",
		Namnskyddslopnummer:    "1",
		Registreringsland:      "SE-LAND",
		Organisationsnamn:      "First Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02",
		OrganizationName:       "First Sverige AB",
		Organisationsform:      "AB-ORGFO",
		Registreringsdatum:     "2020-01-02",
		RawPayload:             []byte(`{"organisationsidentitet":"1111111111$ORGNR-IDORG"}`),
		PayloadHash:            "first-row-hash",
		RunID:                  "old-partial",
		Metadata:               metadata,
	}})
	require.NoError(t, err)

	newWorkflowRunID, err := gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-new-resume-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)
	newSnapshotID, err := gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: newWorkflowRunID,
		SnapshotKey:   "new-resume",
		SnapshotDate:  time.Now(),
		Metadata:      metadata,
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	actions := NewBulkIngestActions(gateway, server.Client(), BulkIngestConfig{StagingRoot: t.TempDir()})
	result, err := actions.DownloadSEBulkSourceFiles(ctx, DownloadSEBulkSourceFilesActivityInput{
		WorkflowRunID: newWorkflowRunID.String(),
		SnapshotID:    newSnapshotID.String(),
		BatchSize:     1,
		Metadata:      metadata,
		SourceFiles: []ComparedSEBulkSourceFile{{
			VerifiedSEBulkSourceFile: VerifiedSEBulkSourceFile{
				Dataset:    "bolagsverket",
				SourceURL:  server.URL + "/bolagsverket_bulkfil.zip",
				FileFormat: "zip",
			},
			NeedsDownload: true,
		}},
	})

	require.NoError(t, err)
	require.Len(t, result.SourceFiles, 1)
	require.Equal(t, oldSourceFileID.String(), result.SourceFiles[0].SourceFileID)
	require.Equal(t, "downloaded", result.SourceFiles[0].Status)

	progress, ok, err := gateway.GetSourceFileProgress(ctx, oldSourceFileID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "downloaded", progress.Status)
	require.EqualValues(t, 1, progress.RowsSeen)

	var rowSnapshot string
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT bulk_snapshot_id::text
		FROM se_workflow.bolagsverket_raw_records
		WHERE source_file_id = $1
	`, oldSourceFileID).Scan(&rowSnapshot))
	require.Equal(t, newSnapshotID.String(), rowSnapshot)
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

func zipBytes(t *testing.T, name string, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	file, err := writer.Create(name)
	require.NoError(t, err)
	_, err = file.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
