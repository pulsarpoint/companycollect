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
	err := h.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT id::text, '%s', COALESCE(%s,''), COALESCE(%s,''),
		       COALESCE(processing_status,''), COALESCE(processing_status,''), %s, %s, %s, COALESCE(%s,''),
		       COALESCE(run_id,''), processing_attempts, COALESCE(processing_error,''),
		       COALESCE(payload_hash,''), raw_payload,
		       first_seen_at, last_seen_at, processed_at, created_at, updated_at
		FROM %s WHERE id = $1
	`,
		cfg.source,
		cfg.nameColumn,
		cfg.nativeColumn,
		coalesceRawInputDetailText(cfg.companyTypeExpr),
		coalesceRawInputDetailText(cfg.registrationColumn),
		coalesceRawInputDetailText(cfg.websiteExpr),
		cfg.countryColumn,
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

func coalesceRawInputDetailText(expr string) string {
	if expr == "''" {
		return "''"
	}
	return fmt.Sprintf("COALESCE(%s,'')", expr)
}

func (h *Handlers) getTranslatedRawInputDetail(ctx context.Context, cfg rawInputSource, id string) (rawInputDetail, error) {
	var row rawInputDetail
	var rawPayloadEn []byte
	err := h.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT id::text, '%s', COALESCE(%s,''), COALESCE(%s,''),
		       COALESCE(processing_status,''), COALESCE(processing_status,''), %s, %s, %s, COALESCE(%s,''),
		       COALESCE(run_id,''), processing_attempts, COALESCE(processing_error,''),
		       COALESCE(payload_hash,''), raw_payload, raw_payload_en,
		       COALESCE(translation_status,''), translation_attempts, COALESCE(translation_error,''), COALESCE(translation_model,''),
		       COALESCE(translation_prompt_version,''), COALESCE(translation_fx_source,''), COALESCE(translation_fx_rate_date::text,''),
		       translated_at, first_seen_at, last_seen_at, processed_at, created_at, updated_at
		FROM %s WHERE id = $1
	`,
		cfg.source,
		cfg.nameColumn,
		cfg.nativeColumn,
		coalesceRawInputDetailText(cfg.companyTypeExpr),
		coalesceRawInputDetailText(cfg.registrationColumn),
		coalesceRawInputDetailText(cfg.websiteExpr),
		cfg.countryColumn,
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
