package cvrrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultCountry = "DK"

type Record struct {
	CVRNumber          string
	CompanyName        string
	RegistrationStatus string
	CompanyType        string
	Website            string
	Email              string
	Phone              string
	MarketingProtected *bool
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

func NewRecord(raw json.RawMessage) (Record, error) {
	source, err := sourceObject(raw)
	if err != nil {
		return Record{}, err
	}
	company := nestedObject(source, "Vrvirksomhed")
	if company == nil {
		company = source
	}
	cvrNumber := normalizeCVRNumber(firstString(
		pathValue(company, "cvrNummer"),
		pathValue(company, "cvrnummer"),
		pathValue(company, "cvr_number"),
		pathValue(source, "cvrNummer"),
		pathValue(source, "cvr_number"),
	))
	if cvrNumber == "" {
		return Record{}, errors.New("cvr number is required")
	}
	canonical, err := canonicalJSON(source)
	if err != nil {
		return Record{}, err
	}
	hash := sha256.Sum256(canonical)

	return Record{
		CVRNumber: cvrNumber,
		CompanyName: firstString(
			pathValue(company, "virksomhedMetadata", "nyesteNavn", "navn"),
			pathValue(company, "navn", "navn"),
			pathValue(company, "company_name"),
			pathValue(company, "name"),
		),
		RegistrationStatus: firstString(
			pathValue(company, "virksomhedMetadata", "nyesteStatus"),
			pathValue(company, "status"),
			pathValue(company, "registration_status"),
		),
		CompanyType: firstString(
			pathValue(company, "virksomhedMetadata", "nyesteVirksomhedsform", "kortBeskrivelse"),
			pathValue(company, "virksomhedMetadata", "nyesteVirksomhedsform", "langBeskrivelse"),
			pathValue(company, "virksomhedsform", "kortBeskrivelse"),
			pathValue(company, "company_type"),
		),
		Website:            firstContact(company, "hjemmeside", "website"),
		Email:              firstContact(company, "elektroniskPost", "email", "emailadresse"),
		Phone:              firstContact(company, "telefonNummer", "telefonnummer", "phone"),
		MarketingProtected: optionalBool(pathValue(company, "reklamebeskyttet")),
		SourceUpdatedAt: parseOptionalTime(firstString(
			pathValue(company, "sidstOpdateret"),
			pathValue(company, "virksomhedMetadata", "sidstOpdateret"),
			pathValue(source, "sidstOpdateret"),
		)),
		RawPayload:  canonical,
		PayloadHash: hex.EncodeToString(hash[:]),
	}, nil
}

func (r Record) UpsertParams(scrollSessionID pgtype.UUID, metadata []byte) db.UpsertCVRWorkflowRawRecordParams {
	return db.UpsertCVRWorkflowRawRecordParams{
		ScrollSessionID:    scrollSessionID,
		SourceNativeID:     r.CVRNumber,
		CvrNumber:          r.CVRNumber,
		CompanyName:        optionalString(r.CompanyName),
		RegistrationStatus: optionalString(r.RegistrationStatus),
		CompanyType:        optionalString(r.CompanyType),
		Website:            optionalString(r.Website),
		Email:              optionalString(r.Email),
		Phone:              optionalString(r.Phone),
		MarketingProtected: r.MarketingProtected,
		CountryIso2:        optionalString(defaultCountry),
		SourceUpdatedAt:    optionalTimestamptz(r.SourceUpdatedAt),
		RawPayload:         r.RawPayload,
		PayloadHash:        r.PayloadHash,
		Metadata:           metadata,
	}
}

func sourceObject(raw json.RawMessage) (map[string]any, error) {
	var decoded map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, errors.Wrap(err, "decode cvr raw record")
	}
	for _, key := range []string{"_source", "source", "data"} {
		if nested := nestedObject(decoded, key); nested != nil {
			return nested, nil
		}
	}
	return decoded, nil
}

func nestedObject(parent map[string]any, key string) map[string]any {
	for actualKey, value := range parent {
		if normalizeKey(actualKey) != normalizeKey(key) {
			continue
		}
		if typed, ok := value.(map[string]any); ok {
			return typed
		}
	}
	return nil
}

func pathValue(root map[string]any, path ...string) any {
	var current any = root
	for _, part := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		var next any
		found := false
		normalizedPart := normalizeKey(part)
		for key, value := range object {
			if normalizeKey(key) == normalizedPart {
				next = value
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		current = next
	}
	return current
}

func firstString(values ...any) string {
	for _, value := range values {
		text := valueString(value)
		if text != "" {
			return text
		}
	}
	return ""
}

func valueString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%.0f", typed))
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return strings.Trim(strings.TrimSpace(string(data)), `"`)
	}
}

func normalizeCVRNumber(value string) string {
	var b strings.Builder
	for _, r := range value {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func firstContact(company map[string]any, keys ...string) string {
	for _, key := range keys {
		value := pathValue(company, key)
		if contact := contactString(value); contact != "" {
			return contact
		}
	}
	return ""
}

func contactString(value any) string {
	switch typed := value.(type) {
	case string, json.Number:
		return valueString(typed)
	case []any:
		for _, item := range typed {
			if contact := contactString(item); contact != "" {
				return contact
			}
		}
	case map[string]any:
		for _, key := range []string{"kontaktoplysning", "vaerdi", "value"} {
			if contact := firstString(pathValue(typed, key)); contact != "" {
				return contact
			}
		}
	}
	return ""
}

func optionalBool(value any) *bool {
	switch typed := value.(type) {
	case bool:
		return &typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "ja", "yes":
			out := true
			return &out
		case "false", "0", "nej", "no":
			out := false
			return &out
		}
	}
	return nil
}

func parseOptionalTime(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000-07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return &parsed
		}
	}
	return nil
}

func canonicalJSON(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "canonicalize cvr record json")
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, errors.Wrap(err, "decode canonical cvr record json")
	}
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, errors.Wrap(err, "encode canonical cvr record json")
	}
	return out, nil
}

func normalizeKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		switch r {
		case 'æ':
			r = 'a'
		case 'ø':
			r = 'o'
		case 'å':
			r = 'a'
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func DefaultCountry() string {
	return defaultCountry
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func optionalTimestamptz(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *value, Valid: true}
}
