package bulk

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	DefaultSourceURL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip"
	defaultCountry   = "EE"
)

type Record struct {
	RegistryCode       string
	LegalName          string
	RegistrationStatus string
	LegalForm          string
	VATNumber          string
	Website            string
	Email              string
	Phone              string
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

type StreamResult struct {
	RowsSeen int32
	FileName string
}

func StreamRecords(ctx context.Context, reader io.Reader, limit int32, emit func(Record) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	body, fileName, closeBody, err := sourcePayloadReader(reader)
	if err != nil {
		return StreamResult{}, err
	}
	if closeBody != nil {
		defer closeBody()
	}
	buffered := bufio.NewReader(body)
	first, err := firstNonSpaceByte(buffered)
	if err != nil {
		return StreamResult{}, err
	}
	var result StreamResult
	result.FileName = fileName
	if first == '[' || first == '{' {
		result.RowsSeen, err = streamJSONRecords(ctx, buffered, limit, emit)
	} else {
		result.RowsSeen, err = streamCSVRecords(ctx, buffered, limit, emit)
	}
	if err != nil {
		return StreamResult{}, err
	}
	return result, nil
}

func NewRecord(raw json.RawMessage) (Record, error) {
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Record{}, errors.Wrap(err, "decode ariregister raw record fields")
	}
	return recordFromMap(fields)
}

func (r Record) UpsertParams(bulkSnapshotID, sourceFileID pgtype.UUID, metadata []byte) db.UpsertAriregisterWorkflowRawRecordParams {
	return db.UpsertAriregisterWorkflowRawRecordParams{
		BulkSnapshotID:     bulkSnapshotID,
		SourceFileID:       sourceFileID,
		SourceNativeID:     r.RegistryCode,
		RegistryCode:       r.RegistryCode,
		LegalName:          optionalString(r.LegalName),
		RegistrationStatus: optionalString(r.RegistrationStatus),
		LegalForm:          optionalString(r.LegalForm),
		VatNumber:          optionalString(r.VATNumber),
		Website:            optionalString(r.Website),
		Email:              optionalString(r.Email),
		Phone:              optionalString(r.Phone),
		CountryIso2:        optionalString(defaultCountry),
		SourceUpdatedAt:    optionalTimestamptz(r.SourceUpdatedAt),
		RawPayload:         r.RawPayload,
		PayloadHash:        r.PayloadHash,
		Metadata:           metadata,
	}
}

func streamCSVRecords(ctx context.Context, reader *bufio.Reader, limit int32, emit func(Record) error) (int32, error) {
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	csvReader.TrimLeadingSpace = true
	csvReader.Comma = detectCSVDelimiter(reader)
	header, err := csvReader.Read()
	if err != nil {
		return 0, errors.Wrap(err, "read ariregister csv header")
	}
	var rowsSeen int32
	for {
		if err := ctx.Err(); err != nil {
			return rowsSeen, err
		}
		if limit > 0 && rowsSeen >= limit {
			return rowsSeen, nil
		}
		row, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rowsSeen, nil
			}
			return rowsSeen, errors.Wrap(err, "read ariregister csv row")
		}
		raw := map[string]any{}
		for i, heading := range header {
			if i >= len(row) {
				continue
			}
			raw[strings.TrimSpace(heading)] = strings.TrimSpace(row[i])
		}
		record, err := recordFromMap(raw)
		if err != nil {
			return rowsSeen, err
		}
		rowsSeen++
		if err := emit(record); err != nil {
			return rowsSeen, err
		}
	}
}

func streamJSONRecords(ctx context.Context, reader io.Reader, limit int32, emit func(Record) error) (int32, error) {
	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return 0, errors.Wrap(err, "read ariregister json root")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return 0, errors.New("ariregister json root must be array or object")
	}
	switch delim {
	case '[':
		return streamJSONArray(ctx, decoder, limit, emit)
	case '{':
		return streamJSONObject(ctx, decoder, limit, emit)
	default:
		return 0, errors.New("ariregister json root must be array or object")
	}
}

func streamJSONArray(ctx context.Context, decoder *json.Decoder, limit int32, emit func(Record) error) (int32, error) {
	var rowsSeen int32
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return rowsSeen, err
		}
		if limit > 0 && rowsSeen >= limit {
			if err := skipValue(decoder); err != nil {
				return rowsSeen, err
			}
			continue
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return rowsSeen, errors.Wrap(err, "decode ariregister json record")
		}
		record, err := NewRecord(raw)
		if err != nil {
			return rowsSeen, err
		}
		rowsSeen++
		if err := emit(record); err != nil {
			return rowsSeen, err
		}
	}
	_, err := decoder.Token()
	return rowsSeen, errors.Wrap(err, "close ariregister json array")
}

func streamJSONObject(ctx context.Context, decoder *json.Decoder, limit int32, emit func(Record) error) (int32, error) {
	decoded := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return 0, errors.Wrap(err, "read ariregister json object key")
		}
		key, ok := keyToken.(string)
		if !ok {
			return 0, errors.New("ariregister json object key must be string")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return 0, errors.Wrap(err, "decode ariregister json object value")
		}
		decoded[key] = raw
	}
	if _, err := decoder.Token(); err != nil {
		return 0, errors.Wrap(err, "close ariregister json object")
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return 0, errors.Wrap(err, "encode ariregister json object")
	}
	if record, err := NewRecord(canonical); err == nil {
		if err := emit(record); err != nil {
			return 0, err
		}
		return 1, nil
	}
	for _, key := range []string{"records", "items", "data", "companies", "ettevotjad"} {
		raw, ok := decoded[key]
		if !ok {
			continue
		}
		return streamJSONRecords(ctx, bytes.NewReader(raw), limit, emit)
	}
	return 0, errors.New("ariregister json object does not contain records")
}

func recordFromMap(fields map[string]any) (Record, error) {
	generalData := selectedObject(fields, "yldandmed", "generaldata", "general_data")
	registryCode := selectedField(fields, "registrikood", "ariregistrikood", "registrycode", "regcode", "code")
	if registryCode == "" {
		return Record{}, errors.New("registry code is required")
	}
	canonical, err := canonicalJSON(fields)
	if err != nil {
		return Record{}, err
	}
	hash := sha256.Sum256(canonical)
	website := selectedNestedField(fields, generalData, "www", "veebileht", "koduleht", "website", "homepage")
	if website == "" {
		website = selectedCommunicationValue(generalData, "WWW")
	}
	email := selectedNestedField(fields, generalData, "email", "epost", "emailiaadress")
	if email == "" {
		email = selectedCommunicationValue(generalData, "EMAIL")
	}
	phone := selectedNestedField(fields, generalData, "telefon", "phone")
	if phone == "" {
		phone = selectedCommunicationValue(generalData, "TEL", "MOB")
	}
	return Record{
		RegistryCode:       registryCode,
		LegalName:          selectedField(fields, "arinimi", "nimi", "ettevotjanimi", "legalname", "name"),
		RegistrationStatus: selectedNestedField(fields, generalData, "staatusetekstina", "staatus_tekstina", "ettevotjastaatustekstina", "registrationstatuslabel", "statuslabel", "staatus", "ettevotjastaatus", "registrationstatus", "status"),
		LegalForm:          selectedNestedField(fields, generalData, "oiguslikuvormitekstina", "oiguslik_vorm_tekstina", "ettevotjaoiguslikvormtekstina", "legalformlabel", "companytypelabel", "oiguslikvorm", "ettevotjaoiguslikvorm", "legalform", "companytype"),
		VATNumber:          selectedNestedField(fields, generalData, "kmkrnumber", "kmkr_number", "kmkrnr", "vatnumber", "taxnumber"),
		Website:            website,
		Email:              email,
		Phone:              phone,
		SourceUpdatedAt:    parseOptionalTime(selectedNestedField(fields, generalData, "muutmiskp", "andmeteuuendamisekp", "updatedat", "lastupdated")),
		RawPayload:         canonical,
		PayloadHash:        hex.EncodeToString(hash[:]),
	}, nil
}

func selectedNestedField(fields, nestedFields map[string]any, candidates ...string) string {
	if value := selectedField(fields, candidates...); value != "" {
		return value
	}
	return selectedField(nestedFields, candidates...)
}

func selectedObject(fields map[string]any, candidates ...string) map[string]any {
	normalizedCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		normalizedCandidates[normalizeKey(candidate)] = struct{}{}
	}
	for key, value := range fields {
		if _, ok := normalizedCandidates[normalizeKey(key)]; !ok {
			continue
		}
		if typed, ok := value.(map[string]any); ok {
			return typed
		}
	}
	return nil
}

func selectedCommunicationValue(fields map[string]any, kinds ...string) string {
	if len(fields) == 0 {
		return ""
	}
	raw, ok := selectedValue(fields, "sidevahendid", "communicationmeans", "contacts")
	if !ok {
		return ""
	}
	items, ok := raw.([]any)
	if !ok {
		return ""
	}
	acceptedKinds := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		acceptedKinds[strings.ToUpper(strings.TrimSpace(kind))] = struct{}{}
	}
	for _, item := range items {
		contact, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind := strings.ToUpper(selectedField(contact, "liik", "type", "kind"))
		if _, ok := acceptedKinds[kind]; !ok {
			continue
		}
		if value := selectedField(contact, "sisu", "value", "content"); value != "" {
			return value
		}
	}
	return ""
}

func selectedValue(fields map[string]any, candidates ...string) (any, bool) {
	normalizedCandidates := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		normalizedCandidates[normalizeKey(candidate)] = struct{}{}
	}
	for key, value := range fields {
		if _, ok := normalizedCandidates[normalizeKey(key)]; ok {
			return value, true
		}
	}
	return nil, false
}

func selectedField(fields map[string]any, candidates ...string) string {
	if len(fields) == 0 {
		return ""
	}
	normalizedFields := make(map[string]any, len(fields))
	for key, value := range fields {
		normalizedFields[normalizeKey(key)] = value
	}
	for _, candidate := range candidates {
		value, ok := normalizedFields[normalizeKey(candidate)]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			return strings.TrimSpace(typed)
		case nil:
			return ""
		default:
			return strings.TrimSpace(strings.Trim(stringFromJSONValue(typed), `"`))
		}
	}
	return ""
}

func normalizeKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		switch r {
		case '\u00e4', '\u00c4':
			r = 'a'
		case '\u00f5', '\u00d5', '\u00f6', '\u00d6':
			r = 'o'
		case '\u00fc', '\u00dc':
			r = 'u'
		case '\u0161', '\u0160':
			r = 's'
		case '\u017e', '\u017d':
			r = 'z'
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func stringFromJSONValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func sourcePayloadReader(reader io.Reader) (io.Reader, string, func() error, error) {
	buffered := bufio.NewReader(reader)
	signature, _ := buffered.Peek(4)
	if len(signature) >= 2 && signature[0] == 0x1f && signature[1] == 0x8b {
		gzipReader, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, "", nil, errors.Wrap(err, "open ariregister gzip payload")
		}
		return gzipReader, "", gzipReader.Close, nil
	}
	if len(signature) >= 4 && bytes.Equal(signature[:4], []byte{'P', 'K', 0x03, 0x04}) {
		data, err := io.ReadAll(buffered)
		if err != nil {
			return nil, "", nil, errors.Wrap(err, "read ariregister zip payload")
		}
		zipReader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, "", nil, errors.Wrap(err, "open ariregister zip payload")
		}
		for _, file := range zipReader.File {
			ext := strings.ToLower(filepath.Ext(file.Name))
			if ext != ".csv" && ext != ".json" {
				continue
			}
			rc, err := file.Open()
			if err != nil {
				return nil, "", nil, errors.Wrap(err, "open ariregister zip member")
			}
			return rc, file.Name, rc.Close, nil
		}
		return nil, "", nil, errors.New("ariregister zip payload contains no csv or json file")
	}
	return buffered, "", nil, nil
}

func firstNonSpaceByte(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.Peek(1)
		if err != nil {
			return 0, errors.Wrap(err, "read ariregister payload")
		}
		if b[0] != ' ' && b[0] != '\n' && b[0] != '\r' && b[0] != '\t' {
			return b[0], nil
		}
		if _, err := reader.ReadByte(); err != nil {
			return 0, errors.Wrap(err, "skip ariregister payload whitespace")
		}
	}
}

func detectCSVDelimiter(reader *bufio.Reader) rune {
	line, _ := reader.Peek(4096)
	firstLine := string(line)
	if newline := strings.IndexAny(firstLine, "\r\n"); newline >= 0 {
		firstLine = firstLine[:newline]
	}
	if strings.Count(firstLine, ";") > strings.Count(firstLine, ",") {
		return ';'
	}
	return ','
}

func canonicalJSON(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "canonicalize ariregister record json")
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, errors.Wrap(err, "decode canonical ariregister record json")
	}
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, errors.Wrap(err, "encode canonical ariregister record json")
	}
	return out, nil
}

func parseOptionalTime(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	layouts := []string{time.RFC3339, "2006-01-02", "02.01.2006", "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return &parsed
		}
	}
	return nil
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

func skipValue(decoder *json.Decoder) error {
	var discard any
	if err := decoder.Decode(&discard); err != nil {
		return errors.Wrap(err, "skip ariregister json value")
	}
	return nil
}
