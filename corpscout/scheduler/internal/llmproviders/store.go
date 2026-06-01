package llmproviders

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type Store struct {
	q      db.Querier
	cipher *Cipher
}

func NewStore(q db.Querier, cipher *Cipher) *Store {
	return &Store{q: q, cipher: cipher}
}

func (s *Store) List(ctx context.Context) ([]ProviderPublic, error) {
	rows, err := s.q.ListLLMProviders(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list llm providers")
	}
	result := make([]ProviderPublic, 0, len(rows))
	for _, row := range rows {
		result = append(result, providerPublicFromList(row))
	}
	return result, nil
}

func (s *Store) ListEnabled(ctx context.Context) ([]ProviderPublic, error) {
	rows, err := s.q.ListEnabledLLMProviders(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list enabled llm providers")
	}
	result := make([]ProviderPublic, 0, len(rows))
	for _, row := range rows {
		result = append(result, providerPublicFromListEnabled(row))
	}
	return result, nil
}

func (s *Store) ProviderConfig(ctx context.Context, id uuid.UUID) (ProviderConfig, error) {
	row, err := s.q.GetLLMProviderForUse(ctx, id)
	if err != nil {
		return ProviderConfig{}, errors.Wrap(err, "get llm provider")
	}
	if !row.Enabled {
		return ProviderConfig{}, errors.New("llm provider is disabled")
	}
	return providerConfigFromRow(row), nil
}

func (s *Store) ProviderConfigForTest(ctx context.Context, id uuid.UUID) (ProviderConfig, error) {
	row, err := s.q.GetLLMProviderForUse(ctx, id)
	if err != nil {
		return ProviderConfig{}, errors.Wrap(err, "get llm provider")
	}
	return providerConfigFromRow(row), nil
}

func (s *Store) Create(ctx context.Context, command UpsertProviderCommand) (ProviderPublic, error) {
	command = normalizeCommand(command)
	if err := validateCommand(command); err != nil {
		return ProviderPublic{}, err
	}
	if err := s.requireCipherForEnabledOrSecret(command.Enabled || command.IsDefault, strings.TrimSpace(command.APIKey) != ""); err != nil {
		return ProviderPublic{}, err
	}
	secret, err := s.encryptSecretForWrite(command.APIKey)
	if err != nil {
		return ProviderPublic{}, err
	}
	if command.IsDefault {
		if err := s.q.ClearDefaultLLMProvider(ctx); err != nil {
			return ProviderPublic{}, errors.Wrap(err, "clear default llm provider")
		}
	}
	row, err := s.q.CreateLLMProvider(ctx, db.CreateLLMProviderParams{
		Slug:             command.Slug,
		DisplayName:      command.DisplayName,
		ProviderType:     command.ProviderType,
		BaseUrl:          command.BaseURL,
		Model:            command.Model,
		ApiKeyCiphertext: textPtr(secret.Ciphertext),
		ApiKeyNonce:      textPtr(secret.Nonce),
		ApiKeyKeyVersion: DefaultKeyVersion,
		ApiKeyAlgorithm:  EncryptionAlgorithmAES256GCM,
		IsDefault:        command.IsDefault,
		Enabled:          command.Enabled || command.IsDefault,
		Capabilities:     normalizedJSON(command.Capabilities),
		Metadata:         normalizedJSON(command.Metadata),
	})
	if err != nil {
		return ProviderPublic{}, errors.Wrap(err, "create llm provider")
	}
	return providerPublicFromCreate(row), nil
}

func (s *Store) Update(ctx context.Context, id uuid.UUID, command UpsertProviderCommand) (ProviderPublic, error) {
	command = normalizeCommand(command)
	if err := validateCommand(command); err != nil {
		return ProviderPublic{}, err
	}
	if err := s.requireCipherForEnabledOrSecret(command.Enabled || command.IsDefault, strings.TrimSpace(command.APIKey) != ""); err != nil {
		return ProviderPublic{}, err
	}
	current, err := s.q.GetLLMProviderForUse(ctx, id)
	if err != nil {
		return ProviderPublic{}, errors.Wrap(err, "get llm provider for update")
	}
	ciphertext := current.ApiKeyCiphertext
	nonce := current.ApiKeyNonce
	if strings.TrimSpace(command.APIKey) != "" {
		secret, err := s.encryptSecretForWrite(command.APIKey)
		if err != nil {
			return ProviderPublic{}, err
		}
		ciphertext = textPtr(secret.Ciphertext)
		nonce = textPtr(secret.Nonce)
	}
	row, err := s.q.UpdateLLMProvider(ctx, db.UpdateLLMProviderParams{
		ID:               id,
		DisplayName:      command.DisplayName,
		ProviderType:     command.ProviderType,
		BaseUrl:          command.BaseURL,
		Model:            command.Model,
		ApiKeyCiphertext: ciphertext,
		ApiKeyNonce:      nonce,
		ApiKeyKeyVersion: DefaultKeyVersion,
		ApiKeyAlgorithm:  EncryptionAlgorithmAES256GCM,
		Enabled:          command.Enabled || command.IsDefault,
		Capabilities:     normalizedJSON(command.Capabilities),
		Metadata:         normalizedJSON(command.Metadata),
	})
	if err != nil {
		return ProviderPublic{}, errors.Wrap(err, "update llm provider")
	}
	if command.IsDefault {
		return s.SetDefault(ctx, id)
	}
	return providerPublicFromUpdate(row), nil
}

func (s *Store) SetDefault(ctx context.Context, id uuid.UUID) (ProviderPublic, error) {
	if s.cipher == nil {
		return ProviderPublic{}, errors.New("llm provider encryption key is required before enabling llm providers")
	}
	row, err := s.q.SetDefaultLLMProvider(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ProviderPublic{}, errors.New("llm provider not found")
		}
		return ProviderPublic{}, errors.Wrap(err, "set default llm provider")
	}
	return providerPublicFromSetDefault(row), nil
}

func (s *Store) requireCipherForEnabledOrSecret(enabledOrDefault bool, hasSecret bool) error {
	if hasSecret && s.cipher == nil {
		return errors.New("llm provider encryption key is required before storing api keys")
	}
	if enabledOrDefault && s.cipher == nil {
		return errors.New("llm provider encryption key is required before enabling llm providers")
	}
	return nil
}

func (s *Store) encryptSecretForWrite(apiKey string) (EncryptedSecret, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return EncryptedSecret{Algorithm: EncryptionAlgorithmAES256GCM, KeyVersion: DefaultKeyVersion}, nil
	}
	return s.cipher.Encrypt(apiKey)
}

func providerConfigFromRow(row db.LlmProvider) ProviderConfig {
	config := ProviderConfig{
		ID:           row.ID,
		Slug:         row.Slug,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
	}
	if row.ApiKeyCiphertext != nil && row.ApiKeyNonce != nil {
		config.APIKey = &EncryptedSecret{
			Ciphertext: *row.ApiKeyCiphertext,
			Nonce:      *row.ApiKeyNonce,
			Algorithm:  row.ApiKeyAlgorithm,
			KeyVersion: row.ApiKeyKeyVersion,
		}
	}
	return config
}

func normalizeCommand(command UpsertProviderCommand) UpsertProviderCommand {
	command.Slug = strings.ToLower(strings.TrimSpace(command.Slug))
	command.DisplayName = strings.TrimSpace(command.DisplayName)
	command.ProviderType = strings.TrimSpace(command.ProviderType)
	command.BaseURL = strings.TrimSpace(command.BaseURL)
	command.Model = strings.TrimSpace(command.Model)
	if command.ProviderType == "" {
		command.ProviderType = ProviderTypeOpenAICompatible
	}
	return command
}

func validateCommand(command UpsertProviderCommand) error {
	if command.Slug == "" {
		return errors.New("slug is required")
	}
	if command.DisplayName == "" {
		return errors.New("display_name is required")
	}
	if command.ProviderType != ProviderTypeOpenAICompatible {
		return errors.New("provider_type must be openai_compatible")
	}
	if !strings.HasPrefix(command.BaseURL, "http://") && !strings.HasPrefix(command.BaseURL, "https://") {
		return errors.New("base_url must start with http:// or https://")
	}
	if command.Model == "" {
		return errors.New("model is required")
	}
	if !isJSONObject(command.Capabilities) {
		return errors.New("capabilities must be a JSON object")
	}
	if !isJSONObject(command.Metadata) {
		return errors.New("metadata must be a JSON object")
	}
	return nil
}

func isJSONObject(value json.RawMessage) bool {
	if len(value) == 0 {
		return true
	}
	var object map[string]any
	return json.Unmarshal(value, &object) == nil
}

func normalizedJSON(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return value
}

func textPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func providerPublicFromList(row db.ListLLMProvidersRow) ProviderPublic {
	return ProviderPublic{
		ID:           row.ID,
		Slug:         row.Slug,
		DisplayName:  row.DisplayName,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
		HasAPIKey:    row.HasApiKey,
		IsDefault:    row.IsDefault,
		Enabled:      row.Enabled,
		Capabilities: row.Capabilities,
		Metadata:     row.Metadata,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func providerPublicFromListEnabled(row db.ListEnabledLLMProvidersRow) ProviderPublic {
	return ProviderPublic{
		ID:           row.ID,
		Slug:         row.Slug,
		DisplayName:  row.DisplayName,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
		HasAPIKey:    row.HasApiKey,
		IsDefault:    row.IsDefault,
		Enabled:      row.Enabled,
		Capabilities: row.Capabilities,
		Metadata:     row.Metadata,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func providerPublicFromCreate(row db.CreateLLMProviderRow) ProviderPublic {
	return ProviderPublic{
		ID:           row.ID,
		Slug:         row.Slug,
		DisplayName:  row.DisplayName,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
		HasAPIKey:    row.HasApiKey,
		IsDefault:    row.IsDefault,
		Enabled:      row.Enabled,
		Capabilities: row.Capabilities,
		Metadata:     row.Metadata,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func providerPublicFromUpdate(row db.UpdateLLMProviderRow) ProviderPublic {
	return ProviderPublic{
		ID:           row.ID,
		Slug:         row.Slug,
		DisplayName:  row.DisplayName,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
		HasAPIKey:    row.HasApiKey,
		IsDefault:    row.IsDefault,
		Enabled:      row.Enabled,
		Capabilities: row.Capabilities,
		Metadata:     row.Metadata,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}

func providerPublicFromSetDefault(row db.SetDefaultLLMProviderRow) ProviderPublic {
	return ProviderPublic{
		ID:           row.ID,
		Slug:         row.Slug,
		DisplayName:  row.DisplayName,
		ProviderType: row.ProviderType,
		BaseURL:      row.BaseUrl,
		Model:        row.Model,
		HasAPIKey:    row.HasApiKey,
		IsDefault:    row.IsDefault,
		Enabled:      row.Enabled,
		Capabilities: row.Capabilities,
		Metadata:     row.Metadata,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
}
