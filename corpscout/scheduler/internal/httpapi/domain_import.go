package httpapi

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func (h *Handlers) handleImportDomains(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not available")
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "request too large (max 10MB)")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing 'file' form field")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, 10<<20))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	batchID := uuid.New()
	s3Key := fmt.Sprintf("imports/%s.csv", batchID)

	if err := h.s3.Upload(r.Context(), s3Key, data, "text/csv"); err != nil {
		slog.Error("upload import csv to s3", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to store file")
		return
	}

	batch, err := h.db.InsertImportBatch(r.Context(), db.InsertImportBatchParams{
		Filename: header.Filename,
		CsvS3Key: s3Key,
	})
	if err != nil {
		slog.Error("insert import batch", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create import batch")
		return
	}

	riverJob, err := h.rv.Insert(r.Context(), workers.DomainImportArgs{
		BatchID:  batch.ID.String(),
		CsvS3Key: s3Key,
	}, &river.InsertOpts{Queue: "domain_import"})
	if err != nil {
		slog.Error("enqueue domain import job", "error", err, "batch_id", batch.ID)
		writeError(w, http.StatusInternalServerError, "failed to enqueue import job")
		return
	}

	if err := h.db.UpdateImportBatchRiverJob(r.Context(), db.UpdateImportBatchRiverJobParams{
		ID:         batch.ID,
		RiverJobID: &riverJob.Job.ID,
	}); err != nil {
		slog.Warn("set river job id on import batch", "error", err)
	}

	writeJSON(w, http.StatusAccepted, batch)
}
