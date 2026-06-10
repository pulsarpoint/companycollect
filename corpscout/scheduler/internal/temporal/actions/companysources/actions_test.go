package companysources

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	sourcecore "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	sourceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

func TestDownloadSourceResultJSONIncludesFileSummaries(t *testing.T) {
	result := marshalActionResult(sourceworkflow.DownloadSourceResult{
		ActionRunID: "action-run-1",
		Files: []sourceworkflow.DownloadedSourceFileSummary{
			{FileKey: "source", FileRunID: "file-run-1", Path: "/runs/source.ndjson", RecordsWritten: 2},
		},
	})

	var got map[string]any
	require.NoError(t, json.Unmarshal(result, &got))
	require.Equal(t, "action-run-1", got["action_run_id"])
	files := got["files"].([]any)
	first := files[0].(map[string]any)
	require.Equal(t, "source", first["file_key"])
	require.Equal(t, "file-run-1", first["file_run_id"])
	require.Equal(t, "/runs/source.ndjson", first["path"])
	require.Equal(t, float64(2), first["records_written"])
}

func TestSelectedSourceFilesFromLatestRowsFailsWhenRequiredRunMissing(t *testing.T) {
	_, err := selectedSourceFilesFromLatestRows([]db.ListLatestSuccessfulRequiredSourceFileRunsRow{
		{FileKey: "source", RelativePath: "source.ndjson"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "required source file source has no successful run")
}

func TestSelectedSourceFilesFromLatestRowsUsesSuccessfulPath(t *testing.T) {
	runID := uuid.New()
	sourcePath := filepath.Join(t.TempDir(), "source.ndjson")
	require.NoError(t, os.WriteFile(sourcePath, []byte(`{"id":1}`+"\n"), 0o644))
	status := sourceworkflow.StatusSucceeded

	files, err := selectedSourceFilesFromLatestRows([]db.ListLatestSuccessfulRequiredSourceFileRunsRow{
		{
			ID:           pgtype.UUID{Bytes: runID, Valid: true},
			Status:       &status,
			FileKey:      "source",
			RelativePath: "source.ndjson",
			Path:         &sourcePath,
		},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"source"}, []string{files[0].FileKey})
	require.Equal(t, sourcePath, files[0].Path)
}

func TestSelectedSourceFilesFromActionRowsRequiresEveryEnabledRequiredFile(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.ndjson")
	require.NoError(t, os.WriteFile(sourcePath, []byte(`{"id":1}`+"\n"), 0o644))

	_, err := selectedSourceFilesFromActionRows(
		[]db.ListSuccessfulSourceFileRunsForActionRow{
			{ID: uuid.New(), Status: sourceworkflow.StatusSucceeded, FileKey: "source", RelativePath: "source.ndjson", Path: &sourcePath},
		},
		[]db.ListSourceFilesWithLatestRunRow{
			{FileKey: "source", Enabled: true, Required: true},
			{FileKey: "codelist_REK_en", Enabled: true, Required: true},
			{FileKey: "optional", Enabled: true, Required: false},
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "required source file codelist_REK_en has no successful run")
}

func TestValidateDownloadedFileRequiresExpectedPath(t *testing.T) {
	runDir := t.TempDir()
	sourcePath, err := sourcecore.SafeRunRelativePath(runDir, "source.ndjson")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(sourcePath, []byte(`{"id":1}`+"\n"), 0o644))

	err = validateDownloadedFile(sourcecore.DownloadedFile{Path: sourcePath}, runDir, "source.ndjson")
	require.NoError(t, err)

	err = validateDownloadedFile(sourcecore.DownloadedFile{Path: filepath.Join(runDir, "other.ndjson")}, runDir, "source.ndjson")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match expected path")
}

func TestValidatePRHXBRLWindowInputRequiresDates(t *testing.T) {
	_, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName: "finland_prh_xbrl",
		FileKey:    "statements_manifest",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "registered_date_start is required")
}

func TestValidatePRHXBRLWindowInputRejectsInvertedDates(t *testing.T) {
	_, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName:          "finland_prh_xbrl",
		FileKey:             "statements_manifest",
		RegisteredDateStart: "2026-06-03",
		RegisteredDateEnd:   "2026-06-01",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "registered_date_start must be on or before registered_date_end")
}

func TestValidatePRHXBRLWindowInputDefaultsMaxStatements(t *testing.T) {
	got, err := validatePRHXBRLWindowInput(DownloadSourceFileInput{
		SourceName:          "finland_prh_xbrl",
		FileKey:             "statements_manifest",
		RegisteredDateStart: "2026-06-01",
		RegisteredDateEnd:   "2026-06-03",
	})

	require.NoError(t, err)
	require.Equal(t, int32(50), got.MaxStatements)
}

func TestDeterministicSourceFileRunIDIsStable(t *testing.T) {
	actionRunID := uuid.New()
	sourceFileID := uuid.New()

	first := deterministicSourceFileRunID(actionRunID, sourceFileID)
	second := deterministicSourceFileRunID(actionRunID, sourceFileID)

	require.Equal(t, first, second)
	require.NotEqual(t, first, deterministicSourceFileRunID(uuid.New(), sourceFileID))
}

func TestValidateStoredWorkflowIDRejectsMismatch(t *testing.T) {
	stored := "company-source-action-run-1"

	err := validateStoredWorkflowID(&stored, "other-workflow", "source action run", uuid.New())

	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow id mismatch")
}

func TestFileRunDirectoryNameRejectsUnsafeFileKey(t *testing.T) {
	_, err := fileRunDirectoryName("../source", "file-run-1")

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsafe source file key")
}
