package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
)

type llmProviderListResponse struct {
	Providers []llmproviders.ProviderPublic `json:"providers"`
}

func (h *Handlers) handleListLLMProviders(w http.ResponseWriter, r *http.Request) {
	if h.llmProviders == nil {
		writeError(w, http.StatusServiceUnavailable, "llm provider store not configured")
		return
	}
	providers, err := h.llmProviders.List(r.Context())
	if err != nil {
		slog.Error("list llm providers", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list llm providers")
		return
	}
	writeJSON(w, http.StatusOK, llmProviderListResponse{Providers: providers})
}

func (h *Handlers) handleCreateLLMProvider(w http.ResponseWriter, r *http.Request) {
	if h.llmProviders == nil {
		writeError(w, http.StatusServiceUnavailable, "llm provider store not configured")
		return
	}
	command, err := decodeLLMProviderCommand(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider, err := h.llmProviders.Create(r.Context(), command)
	if err != nil {
		writeLLMProviderMutationError(w, err, "create llm provider")
		return
	}
	writeJSON(w, http.StatusCreated, provider)
}

func (h *Handlers) handleUpdateLLMProvider(w http.ResponseWriter, r *http.Request) {
	if h.llmProviders == nil {
		writeError(w, http.StatusServiceUnavailable, "llm provider store not configured")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}
	command, err := decodeLLMProviderCommand(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider, err := h.llmProviders.Update(r.Context(), id, command)
	if err != nil {
		writeLLMProviderMutationError(w, err, "update llm provider")
		return
	}
	writeJSON(w, http.StatusOK, provider)
}

func (h *Handlers) handleSetDefaultLLMProvider(w http.ResponseWriter, r *http.Request) {
	if h.llmProviders == nil {
		writeError(w, http.StatusServiceUnavailable, "llm provider store not configured")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}
	provider, err := h.llmProviders.SetDefault(r.Context(), id)
	if err != nil {
		writeLLMProviderMutationError(w, err, "set default llm provider")
		return
	}
	writeJSON(w, http.StatusOK, provider)
}

func (h *Handlers) handleTestLLMProvider(w http.ResponseWriter, r *http.Request) {
	if h.llmProviders == nil || h.llmProbe == nil {
		writeError(w, http.StatusServiceUnavailable, "llm provider test not configured")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a valid UUID")
		return
	}
	command, err := decodeTestProviderCommand(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider, err := h.llmProviders.ProviderConfigForTest(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "llm provider not found")
			return
		}
		slog.Error("load llm provider for test", "error", err, "llm_provider_id", id)
		writeError(w, http.StatusInternalServerError, "failed to load llm provider")
		return
	}
	result := h.llmProbe.TestProvider(r.Context(), provider, command)
	writeJSON(w, http.StatusOK, result)
}

func decodeLLMProviderCommand(r *http.Request) (llmproviders.UpsertProviderCommand, error) {
	var command llmproviders.UpsertProviderCommand
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil && !errors.Is(err, io.EOF) {
		return llmproviders.UpsertProviderCommand{}, errors.New("invalid request body")
	}
	return command, nil
}

func decodeTestProviderCommand(r *http.Request) (llmproviders.TestProviderCommand, error) {
	var command llmproviders.TestProviderCommand
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil && !errors.Is(err, io.EOF) {
		return llmproviders.TestProviderCommand{}, errors.New("invalid request body")
	}
	command.Prompt = strings.TrimSpace(command.Prompt)
	if command.Prompt == "" {
		return llmproviders.TestProviderCommand{}, errors.New("prompt is required")
	}
	if len(command.Prompt) > 4000 {
		return llmproviders.TestProviderCommand{}, errors.New("prompt must be at most 4000 characters")
	}
	if command.MaxTokens < 0 {
		return llmproviders.TestProviderCommand{}, errors.New("max_tokens must be greater than zero when provided")
	}
	if command.TimeoutSeconds < 0 {
		return llmproviders.TestProviderCommand{}, errors.New("timeout_seconds must be greater than zero when provided")
	}
	return command, nil
}

func writeLLMProviderMutationError(w http.ResponseWriter, err error, logMessage string) {
	if strings.Contains(err.Error(), "encryption key is required") {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if strings.Contains(err.Error(), " is required") ||
		strings.Contains(err.Error(), "must ") ||
		strings.Contains(err.Error(), "disabled") ||
		strings.Contains(err.Error(), "not found") {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	slog.Error(logMessage, "error", err)
	writeError(w, http.StatusInternalServerError, "failed to update llm provider")
}
