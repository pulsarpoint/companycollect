package bulk

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

const OrganizationDatasetKey = "organisationer"

type StreamResult struct {
	RowsSeen int32
}

type Record struct {
	OrganizationNumber  string
	OrganizationName    string
	RegistrationStatus  string
	LegalForm           string
	LegalFormCode       string
	BusinessDescription string
	SNICodes            json.RawMessage
	PostalAddress       json.RawMessage
	RawPayload          json.RawMessage
	PayloadHash         string
}

type payload struct {
	OrganizationNumber  string          `json:"organization_number"`
	OrganizationName    string          `json:"organization_name,omitempty"`
	RegistrationStatus  string          `json:"registration_status,omitempty"`
	LegalForm           string          `json:"legal_form,omitempty"`
	LegalFormCode       string          `json:"legal_form_code,omitempty"`
	BusinessDescription string          `json:"business_description,omitempty"`
	SNICodes            []sniCode       `json:"sni_codes,omitempty"`
	PostalAddress       map[string]any  `json:"postal_address,omitempty"`
	SourceRecord        json.RawMessage `json:"source_record"`
}

type sniCode struct {
	Code  string `json:"code,omitempty"`
	Label string `json:"label,omitempty"`
}

func StreamRecordsFile(ctx context.Context, path string, format string, limit int32, emit func(Record) error) (StreamResult, error) {
	reader, err := openRecordsFile(path, format)
	if err != nil {
		return StreamResult{}, err
	}
	defer reader.Close()
	return StreamRecords(ctx, reader, limit, emit)
}

func StreamRecords(ctx context.Context, reader io.Reader, limit int32, emit func(Record) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	var result StreamResult
	err := streamJSONRecords(ctx, reader, []string{
		"data",
		"records",
		"organisationer",
		"organizations",
		"documents",
		"arsredovisningar",
		"årsredovisningar",
		"annual_reports",
		"reports",
	}, func(rawRecord json.RawMessage) error {
		if limit > 0 && result.RowsSeen >= limit {
			return nil
		}
		record, ok, err := RecordFromRaw(rawRecord)
		if err != nil || !ok {
			return err
		}
		result.RowsSeen++
		return emit(record)
	})
	if err != nil {
		return StreamResult{}, err
	}
	return result, nil
}

func RecordFromRaw(rawRecord json.RawMessage) (Record, bool, error) {
	var source map[string]any
	decoder := json.NewDecoder(bytes.NewReader(rawRecord))
	decoder.UseNumber()
	if err := decoder.Decode(&source); err != nil {
		return Record{}, false, errors.Wrap(err, "decode sweden hvd record")
	}

	organizationNumber := normalizeOrganizationNumber(firstNonEmpty(
		stringValue(source, "organization_number"),
		stringValue(source, "organisation_number"),
		stringValue(source, "organisationsnummer"),
		stringValue(source, "identitetsbeteckning"),
		stringPath(source, "organisationsidentitet", "identitetsbeteckning"),
		stringPath(source, "organisation", "identitetsbeteckning"),
	))
	if organizationNumber == "" {
		return Record{}, false, nil
	}

	legalForm, legalFormCode := legalForm(source)
	postalAddress := postalAddress(source)
	normalized := payload{
		OrganizationNumber: organizationNumber,
		OrganizationName: firstNonEmpty(
			stringValue(source, "organization_name"),
			stringValue(source, "organisation_name"),
			stringValue(source, "organisationsnamn"),
			stringValue(source, "namn"),
		),
		RegistrationStatus: firstNonEmpty(
			stringValue(source, "registration_status"),
			stringValue(source, "status"),
			registrationStatus(source),
		),
		LegalForm:           legalForm,
		LegalFormCode:       legalFormCode,
		BusinessDescription: firstNonEmpty(stringValue(source, "business_description"), stringValue(source, "verksamhetsbeskrivning"), stringValue(source, "verksamhetstext")),
		SNICodes:            sniCodes(source),
		PostalAddress:       postalAddress,
		SourceRecord:        cloneRaw(rawRecord),
	}
	rawPayload, err := json.Marshal(normalized)
	if err != nil {
		return Record{}, false, errors.Wrap(err, "encode sweden hvd normalized payload")
	}
	sniPayload, err := json.Marshal(emptySNICodes(normalized.SNICodes))
	if err != nil {
		return Record{}, false, errors.Wrap(err, "encode sweden hvd sni codes")
	}
	postalPayload, err := json.Marshal(emptyMap(postalAddress))
	if err != nil {
		return Record{}, false, errors.Wrap(err, "encode sweden hvd postal address")
	}
	return Record{
		OrganizationNumber:  normalized.OrganizationNumber,
		OrganizationName:    normalized.OrganizationName,
		RegistrationStatus:  normalized.RegistrationStatus,
		LegalForm:           normalized.LegalForm,
		LegalFormCode:       normalized.LegalFormCode,
		BusinessDescription: normalized.BusinessDescription,
		SNICodes:            sniPayload,
		PostalAddress:       postalPayload,
		RawPayload:          rawPayload,
		PayloadHash:         hashBytes(rawPayload),
	}, true, nil
}

func streamJSONRecords(ctx context.Context, reader io.Reader, wrapperFields []string, emit func(json.RawMessage) error) error {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return errors.Wrap(err, "read sweden hvd json root")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return errors.New("sweden hvd json root must be array or object")
	}
	switch delim {
	case '[':
		return streamJSONArray(ctx, decoder, emit)
	case '{':
		return streamJSONObject(ctx, decoder, wrapperFields, emit)
	default:
		return errors.New("sweden hvd json root must be array or object")
	}
}

func streamJSONArray(ctx context.Context, decoder *json.Decoder, emit func(json.RawMessage) error) error {
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return errors.Wrap(err, "decode sweden hvd json record")
		}
		if err := emit(raw); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return errors.Wrap(err, "close sweden hvd json array")
	}
	return nil
}

func streamJSONObject(ctx context.Context, decoder *json.Decoder, wrapperFields []string, emit func(json.RawMessage) error) error {
	object := make(map[string]json.RawMessage)
	foundRecords := false
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		keyToken, err := decoder.Token()
		if err != nil {
			return errors.Wrap(err, "read sweden hvd json object key")
		}
		key, ok := keyToken.(string)
		if !ok {
			return errors.New("sweden hvd json object key must be string")
		}
		if stringInSlice(key, wrapperFields) {
			valueToken, err := decoder.Token()
			if err != nil {
				return errors.Wrapf(err, "read sweden hvd %s array", key)
			}
			if valueToken != json.Delim('[') {
				return errors.Newf("sweden hvd %s value must be array", key)
			}
			if err := streamJSONArray(ctx, decoder, emit); err != nil {
				return err
			}
			foundRecords = true
			continue
		}
		var rawValue json.RawMessage
		if err := decoder.Decode(&rawValue); err != nil {
			return errors.Wrapf(err, "decode sweden hvd json field %s", key)
		}
		object[key] = append(json.RawMessage(nil), rawValue...)
	}
	if _, err := decoder.Token(); err != nil {
		return errors.Wrap(err, "close sweden hvd json object")
	}
	if foundRecords {
		return nil
	}
	rawRecord, err := json.Marshal(object)
	if err != nil {
		return errors.Wrap(err, "encode sweden hvd singleton object")
	}
	return emit(rawRecord)
}

func openRecordsFile(path string, format string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open sweden hvd source file")
	}
	compression, err := compressionType(file, path, format)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	switch compression {
	case "gzip":
		reader, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			return nil, errors.Wrap(err, "open sweden hvd gzip")
		}
		return compoundReadCloser{Reader: reader, close: func() error { return closeAll(reader, file) }}, nil
	case "zip":
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, errors.Wrap(err, "stat sweden hvd zip")
		}
		reader, err := zip.NewReader(file, stat.Size())
		if err != nil {
			_ = file.Close()
			return nil, errors.Wrap(err, "open sweden hvd zip")
		}
		for _, zippedFile := range reader.File {
			if zippedFile.FileInfo().IsDir() {
				continue
			}
			rc, err := zippedFile.Open()
			if err != nil {
				_ = file.Close()
				return nil, errors.Wrapf(err, "open sweden hvd zipped file %s", zippedFile.Name)
			}
			return compoundReadCloser{Reader: rc, close: func() error { return closeAll(rc, file) }}, nil
		}
		_ = file.Close()
		return nil, errors.New("sweden hvd zip contains no files")
	default:
		return file, nil
	}
}

func compressionType(file *os.File, path string, format string) (string, error) {
	magic := make([]byte, 4)
	n, err := io.ReadFull(file, magic)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", errors.Wrap(err, "read sweden hvd source magic")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", errors.Wrap(err, "rewind sweden hvd source file")
	}
	if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		return "gzip", nil
	}
	if n >= 4 && magic[0] == 'P' && magic[1] == 'K' {
		return "zip", nil
	}
	normalizedFormat := strings.ToLower(format)
	ext := strings.ToLower(filepath.Ext(path))
	if normalizedFormat == "gzip" || normalizedFormat == "gz" || strings.HasSuffix(normalizedFormat, ".gz") || ext == ".gz" {
		return "gzip", nil
	}
	if normalizedFormat == "zip" || strings.HasSuffix(normalizedFormat, ".zip") || ext == ".zip" {
		return "zip", nil
	}
	return "", nil
}

type compoundReadCloser struct {
	io.Reader
	close func() error
}

func (c compoundReadCloser) Close() error {
	return c.close()
}

func closeAll(closers ...io.Closer) error {
	var closeErr error
	for _, closer := range closers {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func normalizeOrganizationNumber(value string) string {
	value = strings.TrimSpace(value)
	return strings.NewReplacer("-", "", " ", "").Replace(value)
}

func legalForm(source map[string]any) (string, string) {
	form := mapValue(source, "organisationsform")
	label := firstNonEmpty(
		stringValue(source, "legal_form"),
		stringValue(source, "company_type"),
		stringValue(form, "klartext"),
		stringValue(form, "text"),
		stringValue(form, "namn"),
	)
	code := firstNonEmpty(
		stringValue(source, "legal_form_code"),
		stringValue(form, "kod"),
		stringValue(form, "code"),
	)
	if label == "" {
		label = code
	}
	return label, code
}

func registrationStatus(source map[string]any) string {
	deregistered, ok := deregistered(source)
	if !ok {
		return ""
	}
	if deregistered {
		return "inactive"
	}
	return "active"
}

func deregistered(source map[string]any) (bool, bool) {
	if value, ok := boolValue(source, "avregistrerad"); ok {
		return value, true
	}
	for _, key := range []string{"avregistreradOrganisation", "deregistered_organization"} {
		if value, ok := boolValue(mapValue(source, key), "avregistrerad"); ok {
			return value, true
		}
		if value, ok := boolValue(mapValue(source, key), "deregistered"); ok {
			return value, true
		}
	}
	return false, false
}

func sniCodes(source map[string]any) []sniCode {
	var codes []sniCode
	seen := make(map[string]struct{})
	addCode := func(values map[string]any) {
		if values == nil {
			return
		}
		code := firstNonEmpty(stringValue(values, "code"), stringValue(values, "kod"), stringValue(values, "sni_code"))
		label := firstNonEmpty(stringValue(values, "label"), stringValue(values, "klartext"), stringValue(values, "text"), stringValue(values, "description"))
		if code == "" && label == "" {
			return
		}
		key := code + "\x00" + label
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		codes = append(codes, sniCode{Code: code, Label: label})
	}
	for _, item := range mapArrayValue(source, "sni_codes") {
		addCode(item)
	}
	for _, key := range []string{"naringsgrenOrganisation", "näringsgrenOrganisation"} {
		for _, item := range mapArrayValue(source, key) {
			if nested := mapValue(item, "sni"); nested != nil {
				addCode(nested)
				continue
			}
			addCode(item)
		}
	}
	return codes
}

func postalAddress(source map[string]any) map[string]any {
	address := mapValue(source, "postal_address")
	if address == nil {
		address = mapValue(mapValue(source, "postadressOrganisation"), "postadress")
	}
	out := make(map[string]any)
	add := func(key, value string) {
		if value != "" {
			out[key] = value
		}
	}
	add("post_code", firstNonEmpty(stringValue(address, "post_code"), stringValue(address, "postnummer"), stringValue(address, "postal_code")))
	add("city", firstNonEmpty(stringValue(address, "city"), stringValue(address, "postort"), stringValue(address, "ort")))
	add("street_address", firstNonEmpty(stringValue(address, "street_address"), stringValue(address, "utdelningsadress"), stringValue(address, "utdelningsadress1"), stringValue(address, "adress")))
	add("care_of", firstNonEmpty(stringValue(address, "care_of"), stringValue(address, "co"), stringValue(address, "co_adress")))
	if len(out) == 0 {
		return nil
	}
	return out
}

func stringPath(values map[string]any, path ...string) string {
	current := values
	for i, key := range path {
		if i == len(path)-1 {
			return stringValue(current, key)
		}
		current = mapValue(current, key)
		if current == nil {
			return ""
		}
	}
	return ""
}

func stringValue(values map[string]any, key string) string {
	switch typed := anyValue(values, key).(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	default:
		return ""
	}
}

func boolValue(values map[string]any, key string) (bool, bool) {
	switch typed := anyValue(values, key).(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "ja", "yes", "1":
			return true, true
		case "false", "nej", "no", "0":
			return false, true
		}
	}
	return false, false
}

func mapValue(values map[string]any, key string) map[string]any {
	nested, _ := anyValue(values, key).(map[string]any)
	return nested
}

func mapArrayValue(values map[string]any, key string) []map[string]any {
	items, _ := anyValue(values, key).([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if nested, ok := item.(map[string]any); ok {
			out = append(out, nested)
		}
	}
	return out
}

func anyValue(values map[string]any, key string) any {
	if values == nil {
		return nil
	}
	if value, ok := values[key]; ok {
		return value
	}
	normalizedKey := normalizeKey(key)
	for existingKey, value := range values {
		if normalizeKey(existingKey) == normalizedKey {
			return value
		}
	}
	return nil
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	replacer := strings.NewReplacer("_", "", "-", "", " ", "", "å", "a", "ä", "a", "ö", "o")
	return replacer.Replace(key)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func emptySNICodes(values []sniCode) []sniCode {
	if values == nil {
		return []sniCode{}
	}
	return values
}

func emptyMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	return values
}

func stringInSlice(value string, candidates []string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}
