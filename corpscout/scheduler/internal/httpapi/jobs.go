package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/riverqueue/river"
)

func (h *Handlers) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	if h.rv == nil {
		writeError(w, http.StatusServiceUnavailable, "river client not available")
		return
	}

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	_, err = h.rv.JobCancel(r.Context(), id)
	if err != nil {
		if errors.Is(err, river.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		slog.Error("cancel job", "job_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}

	writeJSON(w, http.StatusOK, cancelJobResponse{Status: "cancelled", ID: id})
}

type cancelBulkRequest struct {
	IDs []int64 `json:"ids,omitempty"`
}

type cancelJobResponse struct {
	Status string `json:"status"`
	ID     int64  `json:"id"`
}

type cancelBulkResponse struct {
	Cancelled int `json:"cancelled"`
}

func (h *Handlers) handleCancelBulk(w http.ResponseWriter, r *http.Request) {
	if h.rv == nil {
		writeError(w, http.StatusServiceUnavailable, "river client not available")
		return
	}

	var req cancelBulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ctx := r.Context()

	if len(req.IDs) == 0 {
		writeError(w, http.StatusBadRequest, "provide ids")
		return
	}

	cancelled := h.cancelJobIDs(ctx, req.IDs)
	writeJSON(w, http.StatusOK, cancelBulkResponse{Cancelled: cancelled})
}

// cancelJobIDs cancels a list of job IDs via River (handles running jobs),
// using up to 10 concurrent goroutines. Returns count of successful cancellations.
func (h *Handlers) cancelJobIDs(ctx context.Context, ids []int64) int {
	if len(ids) == 0 {
		return 0
	}
	const concurrency = 10
	sem := make(chan struct{}, concurrency)
	var (
		mu        sync.Mutex
		cancelled int
	)
	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := h.rv.JobCancel(ctx, id); err == nil {
				mu.Lock()
				cancelled++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return cancelled
}
