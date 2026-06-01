package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
)

const testBase64Key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

func TestListLLMProvidersReturnsProvidersWithoutSecrets(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")
	now := time.Now().UTC()
	mock.ExpectQuery(`SELECT`).
		WillReturnRows(llmProviderPublicRows().AddRow(id, "deepseek", "DeepSeek", llmproviders.ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat", true, false, true, []byte(`{"translation":true}`), []byte(`{}`), now, now))
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(db.New(mock), nil), nil))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/llm-providers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"has_api_key":true`)
	require.NotContains(t, w.Body.String(), "secret")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateLLMProviderRejectsAPIKeyWhenEncryptionKeyMissing(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(nil, nil), nil))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers", bytes.NewBufferString(`{
		"slug": "deepseek",
		"display_name": "DeepSeek",
		"provider_type": "openai_compatible",
		"base_url": "https://api.deepseek.com",
		"model": "deepseek-chat",
		"api_key": "secret-value",
		"enabled": false,
		"is_default": false,
		"capabilities": {},
		"metadata": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "encryption key")
}

func TestCreateLLMProviderRejectsEnabledWhenEncryptionKeyMissing(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(nil, nil), nil))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers", bytes.NewBufferString(`{
		"slug": "deepseek",
		"display_name": "DeepSeek",
		"provider_type": "openai_compatible",
		"base_url": "https://api.deepseek.com",
		"model": "deepseek-chat",
		"enabled": true,
		"is_default": false,
		"capabilities": {},
		"metadata": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "encryption key")
}

func TestCreateLLMProviderStoresEncryptedKeyAndReturnsHasAPIKey(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	cipher, err := llmproviders.NewCipher(testBase64Key)
	require.NoError(t, err)
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")
	now := time.Now().UTC()
	mock.ExpectQuery(`INSERT INTO llm_providers`).
		WithArgs(
			"deepseek", "DeepSeek", llmproviders.ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat",
			pgxmock.AnyArg(), pgxmock.AnyArg(), llmproviders.DefaultKeyVersion, llmproviders.EncryptionAlgorithmAES256GCM,
			false, true, pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnRows(llmProviderPublicRows().AddRow(id, "deepseek", "DeepSeek", llmproviders.ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat", true, false, true, []byte(`{"translation":true}`), []byte(`{}`), now, now))
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(db.New(mock), cipher), nil))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers", bytes.NewBufferString(`{
		"slug": "deepseek",
		"display_name": "DeepSeek",
		"provider_type": "openai_compatible",
		"base_url": "https://api.deepseek.com",
		"model": "deepseek-chat",
		"api_key": "secret-value",
		"enabled": true,
		"is_default": false,
		"capabilities": {"translation": true},
		"metadata": {}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code)
	require.Contains(t, w.Body.String(), `"has_api_key":true`)
	require.NotContains(t, w.Body.String(), "secret-value")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateLLMProviderRejectsUnknownFields(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(nil, nil), nil))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers", bytes.NewBufferString(`{"unexpected": true}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "invalid request body")
}

func TestTestLLMProviderReturnsProbeResponse(t *testing.T) {
	providerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/chat/completions", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "deepseek-chat", body["model"])
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"test response"}}]}`))
	}))
	defer providerServer.Close()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")
	now := time.Now().UTC()
	mock.ExpectQuery(`SELECT`).
		WithArgs(id).
		WillReturnRows(llmProviderConfigRows().AddRow(id, "deepseek", "DeepSeek", llmproviders.ProviderTypeOpenAICompatible, providerServer.URL, "deepseek-chat", nil, nil, llmproviders.DefaultKeyVersion, llmproviders.EncryptionAlgorithmAES256GCM, false, false, []byte(`{}`), []byte(`{}`), now, now))
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(db.New(mock), nil), llmproviders.NewProbeClient(providerServer.Client(), nil)))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers/"+id.String()+"/test", bytes.NewBufferString(`{"prompt":"Say ok","max_tokens":32,"timeout_seconds":5}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"status":"succeeded"`)
	require.Contains(t, w.Body.String(), `"response_text":"test response"`)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTestLLMProviderRejectsEmptyPrompt(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", nil, "").
		ConfigureLLMProviders(llmproviders.NewStore(nil, nil), llmproviders.NewProbeClient(nil, nil)))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/llm-providers/1a6531c9-6ea9-4e74-b292-8f8cefe8b4de/test", bytes.NewBufferString(`{"prompt":"   "}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "prompt is required")
}

func llmProviderPublicRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "slug", "display_name", "provider_type", "base_url", "model", "has_api_key",
		"is_default", "enabled", "capabilities", "metadata", "created_at", "updated_at",
	})
}

func llmProviderConfigRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "slug", "display_name", "provider_type", "base_url", "model",
		"api_key_ciphertext", "api_key_nonce", "api_key_key_version", "api_key_algorithm",
		"is_default", "enabled", "capabilities", "metadata", "created_at", "updated_at",
	})
}
