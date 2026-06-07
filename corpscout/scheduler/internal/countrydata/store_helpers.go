package countrydata

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func isJSONObjectPayload(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

func nonZeroTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func optionalBool(value bool) *bool {
	return &value
}

func optionalPositiveInt32(value int) *int32 {
	if value <= 0 {
		return nil
	}
	converted := int32(value)
	return &converted
}

func optionalPositiveInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}

func optionalUUID(value *uuid.UUID) pgtype.UUID {
	if value == nil || *value == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: [16]byte(*value), Valid: true}
}

func optionalDate(value string) (pgtype.Date, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return pgtype.Date{}, nil
	}
	if len(trimmed) > len(time.DateOnly) {
		trimmed = trimmed[:len(time.DateOnly)]
	}
	parsed, err := time.Parse(time.DateOnly, trimmed)
	if err != nil {
		return pgtype.Date{}, err
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}

func optionalPRHTimestamp(value string) (pgtype.Timestamptz, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return pgtype.Timestamptz{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return pgtype.Timestamptz{Time: parsed.UTC(), Valid: true}, nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05.999999999", trimmed); err == nil {
		return pgtype.Timestamptz{Time: parsed.UTC(), Valid: true}, nil
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05", trimmed); err == nil {
		return pgtype.Timestamptz{Time: parsed.UTC(), Valid: true}, nil
	}
	parsed, err := time.Parse(time.DateOnly, trimmed)
	if err != nil {
		return pgtype.Timestamptz{}, err
	}
	return pgtype.Timestamptz{Time: parsed.UTC(), Valid: true}, nil
}

func optionalTimestamp(value time.Time) pgtype.Timestamptz {
	if value.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: value, Valid: true}
}

func jsonObject(value any) []byte {
	if value == nil {
		return []byte(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil || !json.Valid(payload) {
		return []byte(`{}`)
	}
	return payload
}
