package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

type companyEnrichmentSource struct {
	Name        string   `json:"name"`
	DisplayName *string  `json:"display_name"`
	CanProvide  []string `json:"can_provide"`
}

type companyEnrichmentSourcesResponse struct {
	MissingFields []string                  `json:"missing_fields"`
	Sources       []companyEnrichmentSource `json:"sources"`
}

type enrichCompanyFromSourceResponse struct {
	JobID int64 `json:"job_id"`
}

func (h *Handlers) handleGetCompanyEnrichmentSources(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	company, err := h.db.GetCompany(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "company not found")
		return
	}

	missing := []string{}
	if company.EmployeeCount == nil {
		missing = append(missing, "employee_count")
	}
	if company.RevenueUsd == nil {
		missing = append(missing, "revenue")
	}

	sources, err := h.db.GetSourcesWithCapabilities(r.Context())
	if err != nil {
		slog.Error("get sources with capabilities", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	var applicable []companyEnrichmentSource
	for _, src := range sources {
		// country-specific sources: only match if same country
		if src.CountryID.Valid && src.CountryID.Bytes != [16]byte(company.CountryID) {
			continue
		}
		var overlap []string
		for _, cap := range src.Capabilities {
			for _, m := range missing {
				if cap == m {
					overlap = append(overlap, cap)
					break
				}
			}
		}
		if len(overlap) > 0 {
			applicable = append(applicable, companyEnrichmentSource{
				Name:        src.Name,
				DisplayName: src.DisplayName,
				CanProvide:  overlap,
			})
		}
	}
	if applicable == nil {
		applicable = []companyEnrichmentSource{}
	}

	writeJSON(w, http.StatusOK, companyEnrichmentSourcesResponse{
		MissingFields: missing,
		Sources:       applicable,
	})
}

func (h *Handlers) handleEnrichCompanyFromSource(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	var body struct {
		Source string `json:"source"`
	}
	if err := decodeJSON(r, &body); err != nil || body.Source == "" {
		writeError(w, http.StatusBadRequest, "source is required")
		return
	}

	company, err := h.db.GetCompany(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "company not found")
		return
	}
	if company.RegistrationNumber == nil {
		writeError(w, http.StatusUnprocessableEntity, "company has no registration number")
		return
	}

	riverJob, err := h.rv.Insert(r.Context(), workers.EnrichCompanyFinancialsArgs{
		CompanyID:  id.String(),
		OrgNumber:  *company.RegistrationNumber,
		SourceName: body.Source,
	}, &river.InsertOpts{Queue: "enrich_financials"})
	if err != nil {
		slog.Error("insert enrich job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}
	writeJSON(w, http.StatusAccepted, enrichCompanyFromSourceResponse{JobID: riverJob.Job.ID})
}

func (h *Handlers) handlePatchCompanyFinancials(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	var body struct {
		Year          *int32 `json:"year"`
		EmployeeCount *int32 `json:"employee_count"`
		RevenueUsd    *int64 `json:"revenue_usd"`
		ProfitUsd     *int64 `json:"profit_usd"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.EmployeeCount != nil && *body.EmployeeCount < 0 {
		writeError(w, http.StatusBadRequest, "employee_count must be >= 0")
		return
	}

	year := int32(time.Now().Year())
	if body.Year != nil {
		year = *body.Year
	}
	sourceName := "manual"

	rec, err := h.db.CreateCompanyFinancial(r.Context(), db.CreateCompanyFinancialParams{
		CompanyID:     id,
		Year:          year,
		SourceName:    sourceName,
		EmployeeCount: body.EmployeeCount,
		RevenueUsd:    body.RevenueUsd,
		ProfitUsd:     body.ProfitUsd,
		Evidence:      json.RawMessage(`{"source":"manual","kind":"financial"}`),
	})
	if err != nil {
		slog.Error("create company financial", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := h.db.ApproveCompanyFinancial(r.Context(), db.ApproveCompanyFinancialParams{
		ID: rec.ID,
	}); err != nil {
		slog.Error("approve company financial", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (h *Handlers) handlePatchCompany(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid company id")
		return
	}
	var body struct {
		Name             *string `json:"name"`
		ShortName        *string `json:"short_name"`
		ShortDescription *string `json:"short_description"`
		Description      *string `json:"description"`
		Website          *string `json:"website"`
		FoundedYear      *int32  `json:"founded_year"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Name != nil && *body.Name == "" {
		writeError(w, http.StatusBadRequest, "name cannot be empty")
		return
	}

	company, err := h.db.UpdateCompanyInfo(r.Context(), db.UpdateCompanyInfoParams{
		ID:               id,
		Name:             body.Name,
		ShortName:        body.ShortName,
		ShortDescription: body.ShortDescription,
		Description:      body.Description,
		Website:          body.Website,
		FoundedYear:      body.FoundedYear,
	})
	if err != nil {
		slog.Error("patch company", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, company)
}
