package llmproviders

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	ProviderTypeOpenAICompatible = "openai_compatible"
	EncryptionAlgorithmAES256GCM = "aes-256-gcm"
	DefaultKeyVersion            = "v1"
)

type EncryptedSecret struct {
	Ciphertext string `json:"ciphertext"`
	Nonce      string `json:"nonce"`
	Algorithm  string `json:"algorithm"`
	KeyVersion string `json:"key_version"`
}

type ProviderConfig struct {
	ID           uuid.UUID        `json:"id"`
	Slug         string           `json:"slug"`
	ProviderType string           `json:"provider_type"`
	BaseURL      string           `json:"base_url"`
	Model        string           `json:"model"`
	APIKey       *EncryptedSecret `json:"api_key,omitempty"`
}

type RuntimeProviderConfig struct {
	Slug    string
	BaseURL string
	Model   string
	APIKey  string
}

type ProviderPublic struct {
	ID           uuid.UUID       `json:"id"`
	Slug         string          `json:"slug"`
	DisplayName  string          `json:"display_name"`
	ProviderType string          `json:"provider_type"`
	BaseURL      string          `json:"base_url"`
	Model        string          `json:"model"`
	HasAPIKey    bool            `json:"has_api_key"`
	IsDefault    bool            `json:"is_default"`
	Enabled      bool            `json:"enabled"`
	Capabilities json.RawMessage `json:"capabilities"`
	Metadata     json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type UpsertProviderCommand struct {
	Slug         string          `json:"slug"`
	DisplayName  string          `json:"display_name"`
	ProviderType string          `json:"provider_type"`
	BaseURL      string          `json:"base_url"`
	Model        string          `json:"model"`
	APIKey       string          `json:"api_key,omitempty"`
	Enabled      bool            `json:"enabled"`
	IsDefault    bool            `json:"is_default"`
	Capabilities json.RawMessage `json:"capabilities"`
	Metadata     json.RawMessage `json:"metadata"`
}

type TestProviderCommand struct {
	Prompt         string `json:"prompt"`
	MaxTokens      int    `json:"max_tokens"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type TestProviderError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type TestProviderResult struct {
	Status       string             `json:"status"`
	ProviderID   uuid.UUID          `json:"provider_id"`
	ProviderSlug string             `json:"provider_slug"`
	Model        string             `json:"model"`
	ResponseText string             `json:"response_text"`
	DurationMS   int64              `json:"duration_ms"`
	Error        *TestProviderError `json:"error,omitempty"`
}
