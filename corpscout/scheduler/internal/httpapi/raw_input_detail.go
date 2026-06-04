package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	pgx "github.com/jackc/pgx/v5"
)

type rawInputDetail struct {
	ID                       string          `json:"id"`
	Source                   string          `json:"source"`
	Name                     string          `json:"name"`
	NativeID                 string          `json:"native_id"`
	Status                   string          `json:"status"`
	State                    string          `json:"state"`
	CompanyType              string          `json:"company_type,omitempty"`
	RegistrationStatus       string          `json:"registration_status,omitempty"`
	Website                  string          `json:"website,omitempty"`
	CountryISO2              string          `json:"country_iso2,omitempty"`
	RunID                    string          `json:"run_id,omitempty"`
	ProcessingAttempts       int             `json:"processing_attempts"`
	ProcessingError          string          `json:"processing_error,omitempty"`
	PayloadHash              string          `json:"payload_hash"`
	RawPayload               json.RawMessage `json:"raw_payload"`
	RawPayloadEn             json.RawMessage `json:"raw_payload_en,omitempty"`
	TranslationStatus        string          `json:"translation_status,omitempty"`
	TranslationAttempts      int             `json:"translation_attempts,omitempty"`
	TranslationError         string          `json:"translation_error,omitempty"`
	TranslationModel         string          `json:"translation_model,omitempty"`
	TranslationPromptVersion string          `json:"translation_prompt_version,omitempty"`
	TranslationFxSource      string          `json:"translation_fx_source,omitempty"`
	TranslationFxRateDate    string          `json:"translation_fx_rate_date,omitempty"`
	TranslatedAt             *time.Time      `json:"translated_at,omitempty"`
	FirstSeenAt              time.Time       `json:"first_seen_at"`
	LastSeenAt               time.Time       `json:"last_seen_at"`
	ProcessedAt              *time.Time      `json:"processed_at,omitempty"`
	CreatedAt                time.Time       `json:"created_at"`
	UpdatedAt                time.Time       `json:"updated_at"`
}

// handleGetRawInput returns full detail for a single raw input row.
// URL: GET /api/v1/raw-inputs/{source}/{id}
func (h *Handlers) handleGetRawInput(w http.ResponseWriter, r *http.Request) {
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}
	source := chi.URLParam(r, "source")
	idStr := chi.URLParam(r, "id")

	var row rawInputDetail
	var err error
	cfg, ok := rawInputSourceByName(source)
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown source")
		return
	}
	if cfg.translated {
		row, err = h.getTranslatedRawInputDetail(r.Context(), cfg, idStr)
	} else {
		row, err = h.getBasicRawInputDetail(r.Context(), cfg, idStr)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "raw input not found")
			return
		}
		slog.Error("get raw input detail", "source", source, "id", idStr, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, row)
}

func (h *Handlers) getBasicRawInputDetail(ctx context.Context, cfg rawInputSource, id string) (rawInputDetail, error) {
	var row rawInputDetail
	statusExpr := rawInputSourceExpr(cfg.statusExpr, "processing_status")
	stateExpr := rawInputSourceExpr(cfg.stateExpr, statusExpr)
	err := h.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT ri.id::text, '%s', %s, %s,
		       %s, %s, %s, %s, %s, COALESCE(%s,''),
		       %s, %s, %s,
		       COALESCE(ri.payload_hash,''), ri.raw_payload,
		       %s, %s, %s, %s, %s
		FROM %s ri WHERE ri.id = $1
	`,
		cfg.source,
		rawInputDetailTextExpr(cfg.nameColumn),
		rawInputDetailTextExpr(cfg.nativeColumn),
		rawInputDetailTextExpr(statusExpr),
		rawInputDetailTextExpr(stateExpr),
		rawInputDetailTextExpr(cfg.companyTypeExpr),
		rawInputDetailTextExpr(cfg.registrationColumn),
		rawInputDetailTextExpr(cfg.websiteExpr),
		rawInputAliasedExpr(cfg.countryColumn),
		rawInputDetailTextExpr(rawInputSourceExpr(cfg.runIDExpr, "run_id")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.attemptsExpr, "processing_attempts")),
		rawInputDetailTextExpr(rawInputSourceExpr(cfg.errorExpr, "processing_error")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.firstSeenExpr, "first_seen_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.lastSeenExpr, "last_seen_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.processedAtExpr, "processed_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.createdAtExpr, "created_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.updatedAtExpr, "updated_at")),
		cfg.tableName,
	), id).Scan(
		&row.ID, &row.Source, &row.Name, &row.NativeID,
		&row.Status, &row.State, &row.CompanyType, &row.RegistrationStatus, &row.Website, &row.CountryISO2,
		&row.RunID, &row.ProcessingAttempts, &row.ProcessingError,
		&row.PayloadHash, &row.RawPayload,
		&row.FirstSeenAt, &row.LastSeenAt, &row.ProcessedAt, &row.CreatedAt, &row.UpdatedAt,
	)
	return row, err
}

func rawInputDetailTextExpr(expr string) string {
	return coalesceRawInputDetailText(rawInputAliasedExpr(expr))
}

func coalesceRawInputDetailText(expr string) string {
	if expr == "''" {
		return "''"
	}
	return fmt.Sprintf("COALESCE(%s,'')", expr)
}

func (h *Handlers) getTranslatedRawInputDetail(ctx context.Context, cfg rawInputSource, id string) (rawInputDetail, error) {
	var row rawInputDetail
	var rawPayloadEn []byte
	statusExpr := rawInputSourceExpr(cfg.statusExpr, "processing_status")
	stateExpr := rawInputSourceExpr(cfg.stateExpr, statusExpr)
	err := h.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT ri.id::text, '%s', %s, %s,
		       %s, %s, %s, %s, %s, COALESCE(%s,''),
		       %s, %s, %s,
		       COALESCE(ri.payload_hash,''), ri.raw_payload, ri.raw_payload_en,
		       COALESCE(ri.translation_status,''), ri.translation_attempts, COALESCE(ri.translation_error,''), COALESCE(ri.translation_model,''),
		       COALESCE(ri.translation_prompt_version,''), COALESCE(ri.translation_fx_source,''), COALESCE(ri.translation_fx_rate_date::text,''),
		       ri.translated_at, %s, %s, %s, %s, %s
		FROM %s ri WHERE ri.id = $1
	`,
		cfg.source,
		rawInputDetailTextExpr(cfg.nameColumn),
		rawInputDetailTextExpr(cfg.nativeColumn),
		rawInputDetailTextExpr(statusExpr),
		rawInputDetailTextExpr(stateExpr),
		rawInputDetailTextExpr(cfg.companyTypeExpr),
		rawInputDetailTextExpr(cfg.registrationColumn),
		rawInputDetailTextExpr(cfg.websiteExpr),
		rawInputAliasedExpr(cfg.countryColumn),
		rawInputDetailTextExpr(rawInputSourceExpr(cfg.runIDExpr, "run_id")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.attemptsExpr, "processing_attempts")),
		rawInputDetailTextExpr(rawInputSourceExpr(cfg.errorExpr, "processing_error")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.firstSeenExpr, "first_seen_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.lastSeenExpr, "last_seen_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.processedAtExpr, "processed_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.createdAtExpr, "created_at")),
		rawInputAliasedExpr(rawInputSourceExpr(cfg.updatedAtExpr, "updated_at")),
		cfg.tableName,
	), id).Scan(
		&row.ID, &row.Source, &row.Name, &row.NativeID,
		&row.Status, &row.State, &row.CompanyType, &row.RegistrationStatus, &row.Website, &row.CountryISO2,
		&row.RunID, &row.ProcessingAttempts, &row.ProcessingError,
		&row.PayloadHash, &row.RawPayload, &rawPayloadEn,
		&row.TranslationStatus, &row.TranslationAttempts, &row.TranslationError, &row.TranslationModel,
		&row.TranslationPromptVersion, &row.TranslationFxSource, &row.TranslationFxRateDate,
		&row.TranslatedAt,
		&row.FirstSeenAt, &row.LastSeenAt, &row.ProcessedAt, &row.CreatedAt, &row.UpdatedAt,
	)
	if len(rawPayloadEn) > 0 {
		row.RawPayloadEn = json.RawMessage(rawPayloadEn)
	}
	return row, err
}
