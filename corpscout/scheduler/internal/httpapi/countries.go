package httpapi

import (
	"log/slog"
	"net/http"
)

func (h *Handlers) handleListCountries(w http.ResponseWriter, r *http.Request) {
	countries, err := h.db.ListCountries(r.Context())
	if err != nil {
		slog.Error("list countries", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, countries)
}
