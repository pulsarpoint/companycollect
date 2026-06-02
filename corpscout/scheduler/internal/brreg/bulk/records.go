package bulk

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const DefaultSourceURL = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"

type Record struct {
	OrganizationNumber string
	OrganizationName   string
	Website            string
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

type StreamResult struct {
	RowsSeen int32
}

func StreamRecords(ctx context.Context, reader io.Reader, limit int32, emit func(Record) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	body, closeBody, err := maybeGzipReader(reader)
	if err != nil {
		return StreamResult{}, err
	}
	if closeBody != nil {
		defer closeBody()
	}
	decoder := json.NewDecoder(body)
	token, err := decoder.Token()
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "read brreg bulk json root")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return StreamResult{}, errors.New("brreg bulk json root must be array or object")
	}
	switch delim {
	case '[':
		return streamRecordArray(ctx, decoder, limit, emit)
	case '{':
		return streamRecordObject(ctx, decoder, limit, emit)
	default:
		return StreamResult{}, errors.New("brreg bulk json root must be array or object")
	}
}

func NewRecord(raw json.RawMessage) (Record, error) {
	var fields struct {
		OrganizationNumber string `json:"organisasjonsnummer"`
		OrganizationName   string `json:"navn"`
		Website            string `json:"hjemmeside"`
		UpdatedAt          string `json:"oppdateringsdato"`
		LastUpdatedAt      string `json:"sistOppdatert"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Record{}, errors.Wrap(err, "decode brreg raw record fields")
	}
	fields.OrganizationNumber = strings.TrimSpace(fields.OrganizationNumber)
	if fields.OrganizationNumber == "" {
		return Record{}, errors.New("organization number is required")
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return Record{}, err
	}
	hash := sha256.Sum256(canonical)
	return Record{
		OrganizationNumber: fields.OrganizationNumber,
		OrganizationName:   strings.TrimSpace(fields.OrganizationName),
		Website:            strings.TrimSpace(fields.Website),
		SourceUpdatedAt:    parseOptionalTime(firstNonEmpty(fields.UpdatedAt, fields.LastUpdatedAt)),
		RawPayload:         canonical,
		PayloadHash:        hex.EncodeToString(hash[:]),
	}, nil
}

func (r Record) UpsertParams(bulkSnapshotID pgtype.UUID, metadata []byte) db.UpsertBrregWorkflowRawRecordParams {
	return db.UpsertBrregWorkflowRawRecordParams{
		PayloadHash:        r.PayloadHash,
		OrganizationNumber: r.OrganizationNumber,
		BulkSnapshotID:     bulkSnapshotID,
		SourceNativeID:     r.OrganizationNumber,
		OrganizationName:   optionalString(r.OrganizationName),
		RegistrationStatus: optionalString("active"),
		Website:            optionalString(r.Website),
		CountryIso2:        optionalString("NO"),
		SourceUpdatedAt:    optionalTimestamptz(r.SourceUpdatedAt),
		RawPayload:         r.RawPayload,
		Metadata:           metadata,
	}
}

func streamRecordObject(
	ctx context.Context,
	decoder *json.Decoder,
	limit int32,
	emit func(Record) error,
) (StreamResult, error) {
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return StreamResult{}, errors.Wrap(err, "read brreg bulk object key")
		}
		key, ok := keyToken.(string)
		if !ok {
			return StreamResult{}, errors.New("brreg bulk object key must be string")
		}
		switch key {
		case "enheter":
			return streamArrayValue(ctx, decoder, limit, emit)
		case "_embedded":
			return streamEmbeddedObject(ctx, decoder, limit, emit)
		default:
			if err := skipValue(decoder); err != nil {
				return StreamResult{}, err
			}
		}
	}
	_, err := decoder.Token()
	return StreamResult{}, errors.Wrap(err, "close brreg bulk object")
}

func streamEmbeddedObject(
	ctx context.Context,
	decoder *json.Decoder,
	limit int32,
	emit func(Record) error,
) (StreamResult, error) {
	token, err := decoder.Token()
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "read brreg embedded object")
	}
	if token != json.Delim('{') {
		return StreamResult{}, errors.New("brreg _embedded value must be object")
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return StreamResult{}, errors.Wrap(err, "read brreg embedded key")
		}
		key, ok := keyToken.(string)
		if !ok {
			return StreamResult{}, errors.New("brreg embedded key must be string")
		}
		if key == "enheter" {
			return streamArrayValue(ctx, decoder, limit, emit)
		}
		if err := skipValue(decoder); err != nil {
			return StreamResult{}, err
		}
	}
	_, err = decoder.Token()
	return StreamResult{}, errors.Wrap(err, "close brreg embedded object")
}

func streamArrayValue(
	ctx context.Context,
	decoder *json.Decoder,
	limit int32,
	emit func(Record) error,
) (StreamResult, error) {
	token, err := decoder.Token()
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "read brreg record array")
	}
	if token != json.Delim('[') {
		return StreamResult{}, errors.New("brreg enheter value must be array")
	}
	return streamRecordArray(ctx, decoder, limit, emit)
}

func streamRecordArray(
	ctx context.Context,
	decoder *json.Decoder,
	limit int32,
	emit func(Record) error,
) (StreamResult, error) {
	var result StreamResult
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return result, errors.Wrap(err, "stream brreg bulk records")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return result, errors.Wrap(err, "decode brreg bulk record")
		}
		record, err := NewRecord(raw)
		if err != nil {
			return result, err
		}
		if err := emit(record); err != nil {
			return result, err
		}
		result.RowsSeen++
		if limit > 0 && result.RowsSeen >= limit {
			return result, nil
		}
	}
	_, err := decoder.Token()
	return result, errors.Wrap(err, "close brreg record array")
}

func maybeGzipReader(reader io.Reader) (io.Reader, func(), error) {
	buffered := bufio.NewReader(reader)
	header, err := buffered.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, errors.Wrap(err, "peek brreg bulk payload")
	}
	if len(header) == 2 && header[0] == 0x1f && header[1] == 0x8b {
		gzipReader, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, nil, errors.Wrap(err, "open brreg bulk gzip")
		}
		return gzipReader, func() { _ = gzipReader.Close() }, nil
	}
	return buffered, nil, nil
}

func skipValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return errors.Wrap(err, "skip brreg bulk json value")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return errors.Wrap(err, "skip brreg bulk object key")
			}
			if err := skipValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	case '[':
		for decoder.More() {
			if err := skipValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	}
	return errors.Wrap(err, "close skipped brreg bulk json value")
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.Wrap(err, "decode brreg raw payload for hashing")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "canonicalize brreg raw payload")
	}
	return canonical, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseOptionalTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.DateOnly} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return &parsed
		}
	}
	return nil
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func optionalTimestamptz(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *value, Valid: true}
}
