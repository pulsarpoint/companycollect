package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	pgx "github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type patchSourceRequest struct {
	Enabled            *bool                      `json:"enabled"`
	ScheduleEnabled    *bool                      `json:"schedule_enabled"`
	ScheduleKind       *string                    `json:"schedule_kind"`
	ScheduleExpression *string                    `json:"schedule_expression"`
	Config             map[string]json.RawMessage `json:"config"`
}

type patchSourceResponse struct {
	Status string `json:"status"`
}

func (h *Handlers) handlePatchSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req patchSourceRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ctx := r.Context()
	configRequested := len(req.Config) > 0
	hasWrites := req.Enabled != nil || req.ScheduleEnabled != nil || req.ScheduleKind != nil || req.ScheduleExpression != nil || configRequested
	if !hasWrites {
		writeError(w, http.StatusBadRequest, "empty patch request")
		return
	}

	src, err := h.db.GetSourceByName(ctx, name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}

	scheduleKind := src.ScheduleKind
	scheduleExpr := src.ScheduleExpression
	if req.ScheduleKind != nil {
		scheduleKind = *req.ScheduleKind
	}
	if req.ScheduleExpression != nil {
		scheduleExpr = req.ScheduleExpression
	}
	if req.ScheduleKind != nil || req.ScheduleExpression != nil {
		if !validScheduleKind(scheduleKind) {
			writeError(w, http.StatusUnprocessableEntity, "invalid schedule kind")
			return
		}
		if scheduleKind == "interval" && scheduleExpr != nil {
			if _, err := parsePositiveDuration(*scheduleExpr); err != nil {
				writeError(w, http.StatusUnprocessableEntity, "invalid schedule expression")
				return
			}
		}
		if scheduleKind == "cron" && scheduleExpr != nil {
			if _, err := parseCronSchedule(*scheduleExpr); err != nil {
				writeError(w, http.StatusUnprocessableEntity, "invalid schedule expression")
				return
			}
		}
	}

	var mergedConfig json.RawMessage
	if configRequested {
		if err := validateConfigPatch(req.Config); err != nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid config patch")
			return
		}
		config, err := mergeConfig(src.Config, req.Config)
		if err != nil {
			slog.Error("merge source config", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		mergedConfig = config
	}

	writeDB := h.db
	var tx pgx.Tx
	if h.pool != nil && hasWrites {
		var err error
		tx, err = h.pool.Begin(ctx)
		if err != nil {
			slog.Error("begin source patch transaction", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		defer func() {
			if tx == nil {
				return
			}
			if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
				slog.Error("rollback source patch transaction", "name", name, "error", err)
			}
		}()
		writeDB = db.New(tx)
	}

	if req.Enabled != nil {
		if err := writeDB.UpdateSourceEnabled(ctx, db.UpdateSourceEnabledParams{
			Name: name, Enabled: *req.Enabled,
		}); err != nil {
			slog.Error("update source enabled", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	if req.ScheduleEnabled != nil {
		if err := writeDB.UpdateSourceScheduleEnabled(ctx, db.UpdateSourceScheduleEnabledParams{
			Name: name, ScheduleEnabled: *req.ScheduleEnabled,
		}); err != nil {
			slog.Error("update source schedule enabled", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	if req.ScheduleKind != nil || req.ScheduleExpression != nil {
		if err := writeDB.UpdateSourceSchedule(ctx, db.UpdateSourceScheduleParams{
			Name:               name,
			ScheduleKind:       scheduleKind,
			ScheduleExpression: scheduleExpr,
		}); err != nil {
			slog.Error("update source schedule", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	if configRequested {
		if err := writeDB.UpdateSourceConfig(ctx, db.UpdateSourceConfigParams{
			Name:   name,
			Config: mergedConfig,
		}); err != nil {
			slog.Error("update source config", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			slog.Error("commit source patch transaction", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		tx = nil
	}
	writeJSON(w, http.StatusOK, patchSourceResponse{Status: "ok"})
}
