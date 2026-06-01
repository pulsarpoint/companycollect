package llmproviders

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestStoreCreateRejectsEnabledProviderWhenCipherMissing(t *testing.T) {
	store := NewStore(nil, nil)

	_, err := store.Create(t.Context(), validProviderCommand("deepseek", "", true))

	require.ErrorContains(t, err, "enabling llm providers")
}

func TestStoreCreateRejectsAPIKeyWhenCipherMissing(t *testing.T) {
	store := NewStore(nil, nil)

	_, err := store.Create(t.Context(), validProviderCommand("deepseek", "secret-value", false))

	require.ErrorContains(t, err, "storing api keys")
}

func TestStoreCreateMasksAPIKeyInPublicResponse(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	cipher, err := NewCipher(testBase64Key)
	require.NoError(t, err)
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")
	now := time.Now().UTC()
	mock.ExpectQuery(`INSERT INTO llm_providers`).
		WithArgs(
			"deepseek", "DeepSeek", ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat",
			pgxmock.AnyArg(), pgxmock.AnyArg(), DefaultKeyVersion, EncryptionAlgorithmAES256GCM,
			false, false, pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "slug", "display_name", "provider_type", "base_url", "model", "has_api_key",
			"is_default", "enabled", "capabilities", "metadata", "created_at", "updated_at",
		}).AddRow(id, "deepseek", "DeepSeek", ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat", true, false, false, []byte(`{"translation":true}`), []byte(`{}`), now, now))

	result, err := NewStore(db.New(mock), cipher).Create(t.Context(), validProviderCommand("deepseek", "secret-value", false))

	require.NoError(t, err)
	require.True(t, result.HasAPIKey)
	require.Equal(t, "deepseek", result.Slug)
	require.NotContains(t, string(mustJSON(t, result)), "secret-value")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProviderConfigFromRowIncludesEncryptedSecret(t *testing.T) {
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")

	config := providerConfigFromRow(db.LlmProvider{
		ID:               id,
		Slug:             "deepseek",
		ProviderType:     ProviderTypeOpenAICompatible,
		BaseUrl:          "https://api.deepseek.com",
		Model:            "deepseek-chat",
		ApiKeyCiphertext: textPtr("ciphertext"),
		ApiKeyNonce:      textPtr("nonce"),
		ApiKeyKeyVersion: DefaultKeyVersion,
		ApiKeyAlgorithm:  EncryptionAlgorithmAES256GCM,
		Enabled:          true,
	})

	require.Equal(t, "deepseek", config.Slug)
	require.NotNil(t, config.APIKey)
	require.Equal(t, "ciphertext", config.APIKey.Ciphertext)
}

func TestRuntimeConfigBySlugDecryptsEnabledProvider(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	cipher, err := NewCipher(testBase64Key)
	require.NoError(t, err)
	secret, err := cipher.Encrypt("secret-value")
	require.NoError(t, err)
	id := uuid.MustParse("1a6531c9-6ea9-4e74-b292-8f8cefe8b4de")
	now := time.Now().UTC()
	mock.ExpectQuery(`FROM llm_providers`).
		WithArgs("deepseek").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "slug", "display_name", "provider_type", "base_url", "model",
			"api_key_ciphertext", "api_key_nonce", "api_key_key_version", "api_key_algorithm",
			"is_default", "enabled", "capabilities", "metadata", "created_at", "updated_at",
		}).AddRow(
			id, "deepseek", "DeepSeek", ProviderTypeOpenAICompatible, "https://api.deepseek.com", "deepseek-chat",
			&secret.Ciphertext, &secret.Nonce, DefaultKeyVersion, EncryptionAlgorithmAES256GCM,
			false, true, []byte(`{}`), []byte(`{}`), now, now,
		))

	config, err := NewStore(db.New(mock), cipher).RuntimeConfigBySlug(t.Context(), " deepseek ")

	require.NoError(t, err)
	require.Equal(t, RuntimeProviderConfig{
		Slug:    "deepseek",
		BaseURL: "https://api.deepseek.com",
		Model:   "deepseek-chat",
		APIKey:  "secret-value",
	}, config)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRuntimeConfigBySlugReturnsDefaultWithoutQuery(t *testing.T) {
	config, err := NewStore(nil, nil).RuntimeConfigBySlug(t.Context(), " default ")

	require.NoError(t, err)
	require.Equal(t, RuntimeProviderConfig{Slug: "default"}, config)
}

func validProviderCommand(slug string, apiKey string, enabled bool) UpsertProviderCommand {
	return UpsertProviderCommand{
		Slug:         slug,
		DisplayName:  "DeepSeek",
		ProviderType: ProviderTypeOpenAICompatible,
		BaseURL:      "https://api.deepseek.com",
		Model:        "deepseek-chat",
		APIKey:       apiKey,
		Enabled:      enabled,
		Capabilities: json.RawMessage(`{"translation":true}`),
		Metadata:     json.RawMessage(`{}`),
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}
