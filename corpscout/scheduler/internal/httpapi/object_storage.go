package httpapi

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

const (
	defaultObjectStorageListLimit = 100
	maxObjectStorageListLimit     = 1000
)

func (h *Handlers) handleListObjectStorageBuckets(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "object storage client not configured")
		return
	}

	buckets, err := h.s3.ListBuckets(r.Context())
	if err != nil {
		slog.ErrorContext(r.Context(), "list object storage buckets", "error", err)
		writeError(w, http.StatusInternalServerError, "list buckets failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"items": buckets})
}

func (h *Handlers) handleListObjectStorageObjects(w http.ResponseWriter, r *http.Request) {
	if h.s3 == nil {
		writeError(w, http.StatusServiceUnavailable, "object storage client not configured")
		return
	}

	bucket := strings.TrimSpace(chi.URLParam(r, "bucket"))
	if bucket == "" {
		writeError(w, http.StatusBadRequest, "bucket is required")
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}
	limit := queryInt(r, "limit", defaultObjectStorageListLimit)
	if limit > maxObjectStorageListLimit {
		limit = maxObjectStorageListLimit
	}

	result, err := h.s3.ListObjects(r.Context(), s3client.ListObjectsInput{
		Bucket:    bucket,
		Prefix:    r.URL.Query().Get("prefix"),
		Delimiter: delimiter,
		Cursor:    r.URL.Query().Get("cursor"),
		Limit:     int32(limit),
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "list object storage objects", "bucket", bucket, "prefix", r.URL.Query().Get("prefix"), "error", err)
		writeError(w, http.StatusInternalServerError, "list objects failed")
		return
	}

	writeJSON(w, http.StatusOK, result)
}
