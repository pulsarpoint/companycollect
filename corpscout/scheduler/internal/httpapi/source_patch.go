package httpapi

import (
	"encoding/json"
	"net/http"
)

type patchSourceRequest struct {
	Enabled *bool            `json:"enabled"`
	Config  *json.RawMessage `json:"config"`
}

func (h *Handlers) handlePatchSource(w http.ResponseWriter, r *http.Request) {
	var req patchSourceRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Enabled == nil && req.Config == nil {
		writeError(w, http.StatusBadRequest, "empty patch request")
		return
	}

	writeError(w, http.StatusUnprocessableEntity, "source metadata is managed by the source catalog")
}
