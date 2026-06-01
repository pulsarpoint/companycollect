package llmproviders

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProbeClientCallsOpenAICompatibleEndpoint(t *testing.T) {
	var requestPath string
	var authorization string
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		authorization = r.Header.Get("Authorization")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&requestBody))
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"provider works"}}]}`))
	}))
	defer server.Close()
	cipher, err := NewCipher(testBase64Key)
	require.NoError(t, err)
	secret, err := cipher.Encrypt("secret-value")
	require.NoError(t, err)

	result := NewProbeClient(server.Client(), cipher).TestProvider(t.Context(), ProviderConfig{
		ID:           uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de"),
		Slug:         "deepseek",
		ProviderType: ProviderTypeOpenAICompatible,
		BaseURL:      server.URL,
		Model:        "deepseek-chat",
		APIKey:       &secret,
	}, TestProviderCommand{Prompt: "Say ok", MaxTokens: 32, TimeoutSeconds: 5})

	require.Equal(t, "succeeded", result.Status)
	require.Equal(t, "provider works", result.ResponseText)
	require.Equal(t, "/v1/chat/completions", requestPath)
	require.Equal(t, "Bearer secret-value", authorization)
	require.Equal(t, "deepseek-chat", requestBody["model"])
	require.NotContains(t, result.ResponseText, "secret-value")
}

func TestProbeClientReturnsStructuredFailureForProviderStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	result := NewProbeClient(server.Client(), nil).TestProvider(t.Context(), ProviderConfig{
		ID:           uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de"),
		Slug:         "deepseek",
		ProviderType: ProviderTypeOpenAICompatible,
		BaseURL:      server.URL,
		Model:        "deepseek-chat",
	}, TestProviderCommand{Prompt: "Say ok"})

	require.Equal(t, "failed", result.Status)
	require.NotNil(t, result.Error)
	require.Equal(t, "provider_request_failed", result.Error.Code)
	require.Contains(t, result.Error.Message, "401")
}

func TestProbeClientRequiresCipherForEncryptedAPIKey(t *testing.T) {
	result := NewProbeClient(nil, nil).TestProvider(t.Context(), ProviderConfig{
		ID:           uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de"),
		Slug:         "deepseek",
		ProviderType: ProviderTypeOpenAICompatible,
		BaseURL:      "https://example.com",
		Model:        "deepseek-chat",
		APIKey: &EncryptedSecret{
			Ciphertext: "Y2lwaGVydGV4dA==",
			Nonce:      "bm9uY2U=",
			Algorithm:  EncryptionAlgorithmAES256GCM,
			KeyVersion: DefaultKeyVersion,
		},
	}, TestProviderCommand{Prompt: "Say ok"})

	require.Equal(t, "failed", result.Status)
	require.NotNil(t, result.Error)
	require.Equal(t, "decrypt_failed", result.Error.Code)
}

func TestProbeClientRejectsEmptyPrompt(t *testing.T) {
	result := NewProbeClient(nil, nil).TestProvider(t.Context(), ProviderConfig{
		ID:           uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de"),
		Slug:         "deepseek",
		ProviderType: ProviderTypeOpenAICompatible,
		BaseURL:      "https://example.com",
		Model:        "deepseek-chat",
	}, TestProviderCommand{Prompt: "   "})

	require.Equal(t, "failed", result.Status)
	require.NotNil(t, result.Error)
	require.Equal(t, "invalid_prompt", result.Error.Code)
}
