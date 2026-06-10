package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type sourceView struct {
	ID                         uuid.UUID `json:"id"`
	Name                       string    `json:"name"`
	Country                    string    `json:"country"`
	Source                     string    `json:"source"`
	RegistryKey                string    `json:"registry_key"`
	DisplayName                *string   `json:"display_name"`
	Description                *string   `json:"description"`
	SourceGroup                string    `json:"source_group"`
	InputTableName             string    `json:"input_table_name"`
	Enabled                    bool      `json:"enabled"`
	AuthRequired               bool      `json:"auth_required"`
	StorageKind                string    `json:"storage_kind"`
	ClickHouseDatabase         string    `json:"clickhouse_database"`
	ClickHouseTablePrefix      string    `json:"clickhouse_table_prefix"`
	SourceURL                  string    `json:"source_url"`
	DocsURL                    string    `json:"docs_url"`
	RawSourceRetention         string    `json:"raw_source_retention"`
	SourceFileName             string    `json:"source_file_name"`
	UserAgentRequired          bool      `json:"user_agent_required"`
	DownloadWorkflowRegistered bool      `json:"download_workflow_registered"`
	ManualTriggerAvailable     bool      `json:"manual_trigger_available"`
	SourceEntriesAvailable     bool      `json:"source_entries_available"`
	Capabilities               []string  `json:"capabilities"`
	RequiresTranslation        bool      `json:"requires_translation"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
}

type sourceFields struct {
	ID                    uuid.UUID
	Name                  string
	Country               string
	Source                string
	RegistryKey           string
	DisplayName           *string
	Description           *string
	SourceGroup           string
	InputTableName        string
	Enabled               bool
	AuthRequired          bool
	StorageKind           string
	ClickhouseDatabase    string
	ClickhouseTablePrefix string
	SourceURL             string
	DocsURL               string
	RawSourceRetention    string
	SourceFileName        string
	UserAgentRequired     bool
	Capabilities          []string
	RequiresTranslation   bool
	CreatedAt             time.Time
	UpdatedAt             time.Time
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
		views = append(views, sourceViewFromFields(sourceFieldsFromListRow(source)))
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
	view := sourceViewFromFields(sourceFieldsFromGetRow(source))
	writeJSON(w, http.StatusOK, view)
}

func sourceFieldsFromListRow(source db.ListSourcesRow) sourceFields {
	return sourceFields{
		ID:                    source.ID,
		Name:                  source.Name,
		Country:               source.Country,
		Source:                source.Source,
		RegistryKey:           source.RegistryKey,
		DisplayName:           source.DisplayName,
		Description:           source.Description,
		SourceGroup:           source.SourceGroup,
		InputTableName:        source.InputTableName,
		Enabled:               source.Enabled,
		AuthRequired:          source.AuthRequired,
		StorageKind:           source.StorageKind,
		ClickhouseDatabase:    source.ClickhouseDatabase,
		ClickhouseTablePrefix: source.ClickhouseTablePrefix,
		SourceURL:             source.SourceUrl,
		DocsURL:               source.DocsUrl,
		RawSourceRetention:    source.RawSourceRetention,
		SourceFileName:        source.SourceFileName,
		UserAgentRequired:     source.UserAgentRequired,
		Capabilities:          source.Capabilities,
		RequiresTranslation:   source.RequiresTranslation,
		CreatedAt:             source.CreatedAt,
		UpdatedAt:             source.UpdatedAt,
	}
}

func sourceFieldsFromGetRow(source db.GetSourceByNameRow) sourceFields {
	return sourceFields{
		ID:                    source.ID,
		Name:                  source.Name,
		Country:               source.Country,
		Source:                source.Source,
		RegistryKey:           source.RegistryKey,
		DisplayName:           source.DisplayName,
		Description:           source.Description,
		SourceGroup:           source.SourceGroup,
		InputTableName:        source.InputTableName,
		Enabled:               source.Enabled,
		AuthRequired:          source.AuthRequired,
		StorageKind:           source.StorageKind,
		ClickhouseDatabase:    source.ClickhouseDatabase,
		ClickhouseTablePrefix: source.ClickhouseTablePrefix,
		SourceURL:             source.SourceUrl,
		DocsURL:               source.DocsUrl,
		RawSourceRetention:    source.RawSourceRetention,
		SourceFileName:        source.SourceFileName,
		UserAgentRequired:     source.UserAgentRequired,
		Capabilities:          source.Capabilities,
		RequiresTranslation:   source.RequiresTranslation,
		CreatedAt:             source.CreatedAt,
		UpdatedAt:             source.UpdatedAt,
	}
}

func sourceViewFromFields(source sourceFields) sourceView {
	workflowRegistered, manualTriggerAvailable := sourceWorkflowTriggerMetadata(source.Name)
	return sourceView{
		ID:                         source.ID,
		Name:                       source.Name,
		Country:                    source.Country,
		Source:                     source.Source,
		RegistryKey:                source.RegistryKey,
		DisplayName:                source.DisplayName,
		Description:                source.Description,
		SourceGroup:                source.SourceGroup,
		InputTableName:             source.InputTableName,
		Enabled:                    source.Enabled,
		AuthRequired:               source.AuthRequired,
		StorageKind:                source.StorageKind,
		ClickHouseDatabase:         source.ClickhouseDatabase,
		ClickHouseTablePrefix:      source.ClickhouseTablePrefix,
		SourceURL:                  source.SourceURL,
		DocsURL:                    source.DocsURL,
		RawSourceRetention:         source.RawSourceRetention,
		SourceFileName:             source.SourceFileName,
		UserAgentRequired:          source.UserAgentRequired,
		DownloadWorkflowRegistered: workflowRegistered,
		ManualTriggerAvailable:     manualTriggerAvailable,
		SourceEntriesAvailable:     sourceEntriesAvailable(source.Name),
		Capabilities:               source.Capabilities,
		RequiresTranslation:        source.RequiresTranslation,
		CreatedAt:                  source.CreatedAt,
		UpdatedAt:                  source.UpdatedAt,
	}
}

func sourceEntriesAvailable(sourceName string) bool {
	switch sourceName {
	case "ariregister", "brreg":
		return true
	default:
		return false
	}
}

func sourceWorkflowTriggerMetadata(sourceName string) (downloadWorkflowRegistered bool, manualTriggerAvailable bool) {
	switch sourceName {
	case "ariregister", "cvr", "france", "se":
		return true, true
	default:
		return false, false
	}
}
