package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type sourceView struct {
	ID                         uuid.UUID           `json:"id"`
	Name                       string              `json:"name"`
	DisplayName                *string             `json:"display_name"`
	Description                *string             `json:"description"`
	SourceGroup                string              `json:"source_group"`
	InputTableName             string              `json:"input_table_name"`
	Enabled                    bool                `json:"enabled"`
	ScheduleEnabled            bool                `json:"schedule_enabled"`
	ScheduleKind               string              `json:"schedule_kind"`
	ScheduleExpression         *string             `json:"schedule_expression"`
	Config                     json.RawMessage     `json:"config"`
	LastStartedAt              pgtype.Timestamptz  `json:"last_started_at"`
	LastSuccessAt              pgtype.Timestamptz  `json:"last_success_at"`
	LastFailedAt               pgtype.Timestamptz  `json:"last_failed_at"`
	NextScheduledAt            *time.Time          `json:"next_scheduled_at"`
	DownloadWorkflowRegistered bool                `json:"download_workflow_registered"`
	ManualTriggerAvailable     bool                `json:"manual_trigger_available"`
	LastSourceMarkerType       *string             `json:"last_source_marker_type"`
	LastSourceMarker           *string             `json:"last_source_marker"`
	LastSourceModifiedAt       pgtype.Timestamptz  `json:"last_source_modified_at"`
	LastError                  *string             `json:"last_error"`
	ConsecutiveFailures        int32               `json:"consecutive_failures"`
	CountryID                  pgtype.UUID         `json:"country_id"`
	Capabilities               []string            `json:"capabilities"`
	RequiresTranslation        bool                `json:"requires_translation"`
	CreatedAt                  time.Time           `json:"created_at"`
	UpdatedAt                  time.Time           `json:"updated_at"`
	SyncCheckpoint             *syncCheckpointView `json:"sync_checkpoint,omitempty"`
}

type syncCheckpointView struct {
	Cursor          string     `json:"cursor"`
	LastCompletedAt *time.Time `json:"last_completed_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
	Mode            string     `json:"mode"`
	BulkDate        string     `json:"bulk_date,omitempty"`
}

func (h *Handlers) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := h.db.ListSources(r.Context())
	if err != nil {
		slog.Error("list sources", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	views := make([]sourceView, 0, len(sources))
	for _, source := range sources {
		views = append(views, sourceViewFromRow(source))
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *Handlers) handleGetSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	source, err := h.db.GetSourceByName(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}
	view := sourceViewFromRow(source)
	if checkpoint, err := h.db.GetSyncCheckpoint(r.Context(), name); err == nil {
		view.SyncCheckpoint = syncCheckpointViewFromRow(checkpoint)
	}
	writeJSON(w, http.StatusOK, view)
}

func sourceViewFromRow(source db.DataSource) sourceView {
	config := json.RawMessage(source.Config)
	if len(config) == 0 {
		config = json.RawMessage("null")
	}
	workflowRegistered, manualTriggerAvailable := sourceWorkflowTriggerMetadata(source.Name)
	return sourceView{
		ID:                         source.ID,
		Name:                       source.Name,
		DisplayName:                source.DisplayName,
		Description:                source.Description,
		SourceGroup:                source.SourceGroup,
		InputTableName:             source.InputTableName,
		Enabled:                    source.Enabled,
		ScheduleEnabled:            source.ScheduleEnabled,
		ScheduleKind:               source.ScheduleKind,
		ScheduleExpression:         source.ScheduleExpression,
		Config:                     config,
		LastStartedAt:              source.LastStartedAt,
		LastSuccessAt:              source.LastSuccessAt,
		LastFailedAt:               source.LastFailedAt,
		NextScheduledAt:            nextScheduledAt(source),
		DownloadWorkflowRegistered: workflowRegistered,
		ManualTriggerAvailable:     manualTriggerAvailable,
		LastSourceMarkerType:       source.LastSourceMarkerType,
		LastSourceMarker:           source.LastSourceMarker,
		LastSourceModifiedAt:       source.LastSourceModifiedAt,
		LastError:                  source.LastError,
		ConsecutiveFailures:        source.ConsecutiveFailures,
		CountryID:                  source.CountryID,
		Capabilities:               source.Capabilities,
		RequiresTranslation:        source.RequiresTranslation,
		CreatedAt:                  source.CreatedAt,
		UpdatedAt:                  source.UpdatedAt,
	}
}

func sourceWorkflowTriggerMetadata(sourceName string) (downloadWorkflowRegistered bool, manualTriggerAvailable bool) {
	switch sourceName {
	case "ariregister", "cvr":
		return true, true
	default:
		return false, false
	}
}

func syncCheckpointViewFromRow(checkpoint db.SourceSyncCheckpoint) *syncCheckpointView {
	view := &syncCheckpointView{
		Cursor:    checkpoint.Cursor,
		UpdatedAt: checkpoint.UpdatedAt,
		Mode:      "none",
	}
	if checkpoint.LastCompletedAt.Valid {
		completedAt := checkpoint.LastCompletedAt.Time
		view.LastCompletedAt = &completedAt
	}
	if strings.HasPrefix(checkpoint.Cursor, "bulk:") {
		view.Mode = "incremental"
		view.BulkDate = strings.TrimPrefix(checkpoint.Cursor, "bulk:")
	} else if checkpoint.Cursor != "" {
		view.Mode = "incremental"
	}
	return view
}

func nextScheduledAt(source db.DataSource) *time.Time {
	if !source.Enabled || !source.ScheduleEnabled || source.ScheduleExpression == nil {
		return nil
	}
	switch source.ScheduleKind {
	case "interval":
		if !source.LastStartedAt.Valid {
			return nil
		}
		interval, err := parsePositiveDuration(*source.ScheduleExpression)
		if err != nil {
			return nil
		}
		next := source.LastStartedAt.Time.Add(interval)
		return &next
	case "cron":
		schedule, err := parseCronSchedule(*source.ScheduleExpression)
		if err != nil {
			return nil
		}
		reference := source.UpdatedAt
		if source.LastStartedAt.Valid {
			reference = source.LastStartedAt.Time
		}
		if reference.IsZero() {
			reference = time.Now()
		}
		next := schedule.Next(reference)
		return &next
	default:
		return nil
	}
}
