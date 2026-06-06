package bulk

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"golang.org/x/text/encoding/charmap"
)

const OrganizationDatasetKey = "organisationer"
const BolagsverketDatasetKey = "bolagsverket"
const SCBDatasetKey = "scb"

type StreamResult struct {
	RowsSeen int32
}

type RecordIssue struct {
	Code  string
	Field string
	Count int
}

type Record struct {
	RowNumber           int32
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
	Issues              []RecordIssue
}

type BolagsverketRecord struct {
	RowNumber                                         int32
	SourceRecordKey                                   string
	Organisationsidentitet                            string
	OrganizationNumber                                string
	Namnskyddslopnummer                               string
	Registreringsland                                 string
	Organisationsnamn                                 string
	OrganizationName                                  string
	Organisationsform                                 string
	Avregistreringsdatum                              string
	Avregistreringsorsak                              string
	PagandeAvvecklingsEllerOmstruktureringsforfarande string
	Registreringsdatum                                string
	Verksamhetsbeskrivning                            string
	Postadress                                        string
	PostalAddress                                     json.RawMessage
	RawPayload                                        json.RawMessage
	PayloadHash                                       string
	Issues                                            []RecordIssue
}

type SCBRecord struct {
	RowNumber          int32
	SourceRecordKey    string
	ForAndrTyp         string
	COAdress           string
	Foretagsnamn       string
	FtgStat            string
	Gatuadress         string
	JEStat             string
	JurForm            string
	Namn               string
	Ng1                string
	Ng2                string
	Ng3                string
	Ng4                string
	Ng5                string
	PeOrgNr            string
	OrganizationNumber string
	PostNr             string
	PostOrt            string
	RegDatKtid         string
	Reklamsparrtyp     string
	MCOAdress          string
	MForetagsnamn      string
	MFtgStat           string
	MGatuadress        string
	MJEStat            string
	MJurForm           string
	MNamn              string
	MNg1               string
	MNg2               string
	MNg3               string
	MNg4               string
	MNg5               string
	MPostNr            string
	MPostOrt           string
	MRegDatKtid        string
	MReklamsparrtyp    string
	MaskColumns        json.RawMessage
	SNICodes           json.RawMessage
	PostalAddress      json.RawMessage
	RawPayload         json.RawMessage
	PayloadHash        string
	Issues             []RecordIssue
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

type orderedSniCode struct {
	Code     string `json:"code"`
	Position int    `json:"position"`
}

func StreamRecordsFile(ctx context.Context, path string, format string, limit int32, emit func(Record) error) (StreamResult, error) {
	return StreamRecordsFileFromOffset(ctx, path, format, limit, 0, emit)
}

func StreamRecordsFileFromOffset(ctx context.Context, path string, format string, limit int32, skipRows int32, emit func(Record) error) (StreamResult, error) {
	reader, err := openRecordsFile(path, format)
	if err != nil {
		return StreamResult{}, err
	}
	defer reader.Close()
	return StreamRecordsFromOffset(ctx, reader, limit, skipRows, emit)
}

func StreamBolagsverketRecordsFile(ctx context.Context, path string, format string, limit int32, emit func(BolagsverketRecord) error) (StreamResult, error) {
	return StreamBolagsverketRecordsFileFromOffset(ctx, path, format, limit, 0, emit)
}

func StreamBolagsverketRecordsFileFromOffset(ctx context.Context, path string, format string, limit int32, skipRows int32, emit func(BolagsverketRecord) error) (StreamResult, error) {
	reader, err := openRecordsFile(path, format)
	if err != nil {
		return StreamResult{}, err
	}
	defer reader.Close()
	return StreamBolagsverketRecordsFromOffset(ctx, reader, limit, skipRows, emit)
}

func StreamBolagsverketRecords(ctx context.Context, reader io.Reader, limit int32, emit func(BolagsverketRecord) error) (StreamResult, error) {
	return StreamBolagsverketRecordsFromOffset(ctx, reader, limit, 0, emit)
}

func StreamBolagsverketRecordsFromOffset(ctx context.Context, reader io.Reader, limit int32, skipRows int32, emit func(BolagsverketRecord) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	csvReader := csv.NewReader(reader)
	csvReader.Comma = ';'
	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true

	header, err := readCSVHeader(csvReader)
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "read Bolagsverket HVD header")
	}
	var result StreamResult
	var rowNumber int32
	for {
		if err := ctx.Err(); err != nil {
			return StreamResult{}, err
		}
		if limit > 0 && result.RowsSeen >= limit {
			break
		}
		row, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return StreamResult{}, errors.Wrap(err, "read Bolagsverket HVD row")
		}
		if emptyCSVRow(row) {
			continue
		}
		rowNumber++
		record, ok, err := BolagsverketRecordFromRow(header, row, rowNumber)
		if err != nil || !ok {
			return StreamResult{}, err
		}
		result.RowsSeen++
		if result.RowsSeen <= skipRows {
			continue
		}
		if err := emit(record); err != nil {
			return StreamResult{}, err
		}
	}
	return result, nil
}

func BolagsverketRecordFromRow(header []string, row []string, rowNumber int32) (BolagsverketRecord, bool, error) {
	values := csvRowMap(header, row)
	values, issues := sanitizeTextMap(values)
	rawPayload, err := json.Marshal(values)
	if err != nil {
		return BolagsverketRecord{}, false, errors.Wrap(err, "encode Bolagsverket HVD raw payload")
	}
	organizationNumber := normalizeTaggedOrganizationNumber(values["organisationsidentitet"])
	if organizationNumber == "" {
		return BolagsverketRecord{}, false, nil
	}
	postalAddress, err := json.Marshal(parseBolagsverketPostadress(values["postadress"]))
	if err != nil {
		return BolagsverketRecord{}, false, errors.Wrap(err, "encode Bolagsverket HVD postal address")
	}
	nameProtectionSequence := strings.TrimSpace(values["namnskyddslopnummer"])
	sourceRecordKey := organizationNumber
	if nameProtectionSequence != "" {
		sourceRecordKey += "|" + nameProtectionSequence
	}
	return BolagsverketRecord{
		RowNumber:              rowNumber,
		SourceRecordKey:        sourceRecordKey,
		Organisationsidentitet: strings.TrimSpace(values["organisationsidentitet"]),
		OrganizationNumber:     organizationNumber,
		Namnskyddslopnummer:    nameProtectionSequence,
		Registreringsland:      strings.TrimSpace(values["registreringsland"]),
		Organisationsnamn:      strings.TrimSpace(values["organisationsnamn"]),
		OrganizationName:       firstTaggedPart(values["organisationsnamn"]),
		Organisationsform:      strings.TrimSpace(values["organisationsform"]),
		Avregistreringsdatum:   strings.TrimSpace(values["avregistreringsdatum"]),
		Avregistreringsorsak:   strings.TrimSpace(values["avregistreringsorsak"]),
		PagandeAvvecklingsEllerOmstruktureringsforfarande: strings.TrimSpace(values["pagandeAvvecklingsEllerOmstruktureringsforfarande"]),
		Registreringsdatum:     strings.TrimSpace(values["registreringsdatum"]),
		Verksamhetsbeskrivning: strings.TrimSpace(values["verksamhetsbeskrivning"]),
		Postadress:             strings.TrimSpace(values["postadress"]),
		PostalAddress:          postalAddress,
		RawPayload:             rawPayload,
		PayloadHash:            hashBytes(rawPayload),
		Issues:                 issues,
	}, true, nil
}

func StreamSCBRecordsFile(ctx context.Context, path string, format string, limit int32, emit func(SCBRecord) error) (StreamResult, error) {
	return StreamSCBRecordsFileFromOffset(ctx, path, format, limit, 0, emit)
}

func StreamSCBRecordsFileFromOffset(ctx context.Context, path string, format string, limit int32, skipRows int32, emit func(SCBRecord) error) (StreamResult, error) {
	reader, err := openRecordsFile(path, format)
	if err != nil {
		return StreamResult{}, err
	}
	defer reader.Close()
	return StreamSCBRecordsFromOffset(ctx, reader, limit, skipRows, emit)
}

func StreamSCBRecords(ctx context.Context, reader io.Reader, limit int32, emit func(SCBRecord) error) (StreamResult, error) {
	return StreamSCBRecordsFromOffset(ctx, reader, limit, 0, emit)
}

func StreamSCBRecordsFromOffset(ctx context.Context, reader io.Reader, limit int32, skipRows int32, emit func(SCBRecord) error) (StreamResult, error) {
	if emit == nil {
		return StreamResult{}, errors.New("emit callback is required")
	}
	decoded := charmap.ISO8859_1.NewDecoder().Reader(reader)
	csvReader := csv.NewReader(decoded)
	csvReader.Comma = '\t'
	csvReader.FieldsPerRecord = -1

	header, err := readCSVHeader(csvReader)
	if err != nil {
		return StreamResult{}, errors.Wrap(err, "read SCB HVD header")
	}
	var result StreamResult
	var rowNumber int32
	for {
		if err := ctx.Err(); err != nil {
			return StreamResult{}, err
		}
		if limit > 0 && result.RowsSeen >= limit {
			break
		}
		row, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return StreamResult{}, errors.Wrap(err, "read SCB HVD row")
		}
		if emptyCSVRow(row) {
			continue
		}
		rowNumber++
		record, ok, err := SCBRecordFromRow(header, row, rowNumber)
		if err != nil || !ok {
			return StreamResult{}, err
		}
		result.RowsSeen++
		if result.RowsSeen <= skipRows {
			continue
		}
		if err := emit(record); err != nil {
			return StreamResult{}, err
		}
	}
	return result, nil
}

func SCBRecordFromRow(header []string, row []string, rowNumber int32) (SCBRecord, bool, error) {
	values := csvRowMap(header, row)
	values, issues := sanitizeTextMap(values)
	rawPayload, err := json.Marshal(values)
	if err != nil {
		return SCBRecord{}, false, errors.Wrap(err, "encode SCB HVD raw payload")
	}
	peOrgNr := strings.TrimSpace(values["PeOrgNr"])
	if peOrgNr == "" {
		return SCBRecord{}, false, nil
	}
	maskColumns, err := json.Marshal(map[string]string{
		"mCOAdress":       strings.TrimSpace(values["mCOAdress"]),
		"mForetagsnamn":   strings.TrimSpace(values["mForetagsnamn"]),
		"mFtgStat":        strings.TrimSpace(values["mFtgStat"]),
		"mGatuadress":     strings.TrimSpace(values["mGatuadress"]),
		"mJEStat":         strings.TrimSpace(values["mJEStat"]),
		"mJurForm":        strings.TrimSpace(values["mJurForm"]),
		"mNamn":           strings.TrimSpace(values["mNamn"]),
		"mNg1":            strings.TrimSpace(values["mNg1"]),
		"mNg2":            strings.TrimSpace(values["mNg2"]),
		"mNg3":            strings.TrimSpace(values["mNg3"]),
		"mNg4":            strings.TrimSpace(values["mNg4"]),
		"mNg5":            strings.TrimSpace(values["mNg5"]),
		"mPostNr":         strings.TrimSpace(values["mPostNr"]),
		"mPostOrt":        strings.TrimSpace(values["mPostOrt"]),
		"mRegDatKtid":     strings.TrimSpace(values["mRegDatKtid"]),
		"mReklamsparrtyp": strings.TrimSpace(values["mReklamsparrtyp"]),
	})
	if err != nil {
		return SCBRecord{}, false, errors.Wrap(err, "encode SCB HVD mask columns")
	}
	sniPayload, err := json.Marshal(orderedSNICodes(values["Ng1"], values["Ng2"], values["Ng3"], values["Ng4"], values["Ng5"]))
	if err != nil {
		return SCBRecord{}, false, errors.Wrap(err, "encode SCB HVD SNI codes")
	}
	postalAddress, err := json.Marshal(parseSCBPostalAddress(values))
	if err != nil {
		return SCBRecord{}, false, errors.Wrap(err, "encode SCB HVD postal address")
	}
	return SCBRecord{
		RowNumber:          rowNumber,
		SourceRecordKey:    peOrgNr,
		ForAndrTyp:         strings.TrimSpace(values["ForAndrTyp"]),
		COAdress:           strings.TrimSpace(values["COAdress"]),
		Foretagsnamn:       strings.TrimSpace(values["Foretagsnamn"]),
		FtgStat:            strings.TrimSpace(values["FtgStat"]),
		Gatuadress:         strings.TrimSpace(values["Gatuadress"]),
		JEStat:             strings.TrimSpace(values["JEStat"]),
		JurForm:            strings.TrimSpace(values["JurForm"]),
		Namn:               strings.TrimSpace(values["Namn"]),
		Ng1:                strings.TrimSpace(values["Ng1"]),
		Ng2:                strings.TrimSpace(values["Ng2"]),
		Ng3:                strings.TrimSpace(values["Ng3"]),
		Ng4:                strings.TrimSpace(values["Ng4"]),
		Ng5:                strings.TrimSpace(values["Ng5"]),
		PeOrgNr:            peOrgNr,
		OrganizationNumber: organizationNumberFromPeOrgNr(peOrgNr),
		PostNr:             strings.TrimSpace(values["PostNr"]),
		PostOrt:            strings.TrimSpace(values["PostOrt"]),
		RegDatKtid:         strings.TrimSpace(values["RegDatKtid"]),
		Reklamsparrtyp:     strings.TrimSpace(values["Reklamsparrtyp"]),
		MCOAdress:          strings.TrimSpace(values["mCOAdress"]),
		MForetagsnamn:      strings.TrimSpace(values["mForetagsnamn"]),
		MFtgStat:           strings.TrimSpace(values["mFtgStat"]),
		MGatuadress:        strings.TrimSpace(values["mGatuadress"]),
		MJEStat:            strings.TrimSpace(values["mJEStat"]),
		MJurForm:           strings.TrimSpace(values["mJurForm"]),
		MNamn:              strings.TrimSpace(values["mNamn"]),
		MNg1:               strings.TrimSpace(values["mNg1"]),
		MNg2:               strings.TrimSpace(values["mNg2"]),
		MNg3:               strings.TrimSpace(values["mNg3"]),
		MNg4:               strings.TrimSpace(values["mNg4"]),
		MNg5:               strings.TrimSpace(values["mNg5"]),
		MPostNr:            strings.TrimSpace(values["mPostNr"]),
		MPostOrt:           strings.TrimSpace(values["mPostOrt"]),
		MRegDatKtid:        strings.TrimSpace(values["mRegDatKtid"]),
		MReklamsparrtyp:    strings.TrimSpace(values["mReklamsparrtyp"]),
		MaskColumns:        maskColumns,
		SNICodes:           sniPayload,
		PostalAddress:      postalAddress,
		RawPayload:         rawPayload,
		PayloadHash:        hashBytes(rawPayload),
		Issues:             issues,
	}, true, nil
}

func sanitizeTextMap(values map[string]string) (map[string]string, []RecordIssue) {
	var sanitized map[string]string
	var issues []RecordIssue
	for field, value := range values {
		nextValue, fieldIssues := sanitizeTextValue(field, value)
		if len(fieldIssues) == 0 {
			continue
		}
		if sanitized == nil {
			sanitized = make(map[string]string, len(values))
			for key, existing := range values {
				sanitized[key] = existing
			}
		}
		sanitized[field] = nextValue
		issues = append(issues, fieldIssues...)
	}
	if sanitized == nil {
		return values, nil
	}
	return sanitized, issues
}

func sanitizeTextValue(field string, value string) (string, []RecordIssue) {
	sanitized := value
	var issues []RecordIssue
	if strings.Contains(sanitized, "\x00") {
		count := strings.Count(sanitized, "\x00")
		sanitized = strings.ReplaceAll(sanitized, "\x00", "")
		issues = append(issues, RecordIssue{Code: "nul_bytes_removed", Field: field, Count: count})
	}
	if !utf8.ValidString(sanitized) {
		sanitized = strings.ToValidUTF8(sanitized, "")
		issues = append(issues, RecordIssue{Code: "invalid_utf8_removed", Field: field, Count: 1})
	}
	return sanitized, issues
}

func StreamRecords(ctx context.Context, reader io.Reader, limit int32, emit func(Record) error) (StreamResult, error) {
	return StreamRecordsFromOffset(ctx, reader, limit, 0, emit)
}

func StreamRecordsFromOffset(ctx context.Context, reader io.Reader, limit int32, skipRows int32, emit func(Record) error) (StreamResult, error) {
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
		record.RowNumber = result.RowsSeen
		if result.RowsSeen <= skipRows {
			return nil
		}
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
	source, issues := sanitizeJSONMap(source)
	sourceRecord, err := json.Marshal(source)
	if err != nil {
		return Record{}, false, errors.Wrap(err, "encode sanitized sweden hvd source record")
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
		SourceRecord:        sourceRecord,
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
		Issues:              issues,
	}, true, nil
}

func sanitizeJSONMap(source map[string]any) (map[string]any, []RecordIssue) {
	var sanitized map[string]any
	var issues []RecordIssue
	for key, value := range source {
		nextValue, fieldIssues, changed := sanitizeJSONValue(key, value)
		if !changed {
			continue
		}
		if sanitized == nil {
			sanitized = make(map[string]any, len(source))
			for existingKey, existingValue := range source {
				sanitized[existingKey] = existingValue
			}
		}
		sanitized[key] = nextValue
		issues = append(issues, fieldIssues...)
	}
	if sanitized == nil {
		return source, nil
	}
	return sanitized, issues
}

func sanitizeJSONValue(path string, value any) (any, []RecordIssue, bool) {
	switch typed := value.(type) {
	case string:
		sanitized, issues := sanitizeTextValue(path, typed)
		return sanitized, issues, len(issues) > 0
	case map[string]any:
		sanitized, issues := sanitizeJSONMapWithPrefix(path, typed)
		return sanitized, issues, len(issues) > 0
	case []any:
		var sanitized []any
		var issues []RecordIssue
		for index, item := range typed {
			nextItem, itemIssues, changed := sanitizeJSONValue(path+"."+strconv.Itoa(index), item)
			if !changed {
				continue
			}
			if sanitized == nil {
				sanitized = append([]any(nil), typed...)
			}
			sanitized[index] = nextItem
			issues = append(issues, itemIssues...)
		}
		if sanitized == nil {
			return value, nil, false
		}
		return sanitized, issues, true
	default:
		return value, nil, false
	}
}

func sanitizeJSONMapWithPrefix(prefix string, source map[string]any) (map[string]any, []RecordIssue) {
	var sanitized map[string]any
	var issues []RecordIssue
	for key, value := range source {
		nextValue, fieldIssues, changed := sanitizeJSONValue(prefix+"."+key, value)
		if !changed {
			continue
		}
		if sanitized == nil {
			sanitized = make(map[string]any, len(source))
			for existingKey, existingValue := range source {
				sanitized[existingKey] = existingValue
			}
		}
		sanitized[key] = nextValue
		issues = append(issues, fieldIssues...)
	}
	if sanitized == nil {
		return source, nil
	}
	return sanitized, issues
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

func readCSVHeader(reader *csv.Reader) ([]string, error) {
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	return trimTrailingEmptyFields(header), nil
}

func emptyCSVRow(row []string) bool {
	for _, value := range row {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}
	return true
}

func csvRowMap(header []string, row []string) map[string]string {
	row = trimTrailingEmptyFields(row)
	values := make(map[string]string, len(header))
	for i, key := range header {
		key = strings.TrimPrefix(strings.TrimSpace(key), "\ufeff")
		if key == "" {
			continue
		}
		if i >= len(row) {
			values[key] = ""
			continue
		}
		values[key] = strings.TrimSpace(row[i])
	}
	return values
}

func trimTrailingEmptyFields(values []string) []string {
	end := len(values)
	for end > 0 && strings.TrimSpace(values[end-1]) == "" {
		end--
	}
	return values[:end]
}

func firstTaggedPart(value string) string {
	parts := strings.Split(strings.TrimSpace(value), "$")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func normalizeTaggedOrganizationNumber(value string) string {
	return normalizeOrganizationNumber(firstTaggedPart(value))
}

func parseBolagsverketPostadress(value string) map[string]string {
	parts := strings.Split(strings.TrimSpace(value), "$")
	out := make(map[string]string)
	add := func(key string, index int) {
		if index < len(parts) {
			if trimmed := strings.TrimSpace(parts[index]); trimmed != "" {
				out[key] = trimmed
			}
		}
	}
	add("address_line_1", 0)
	add("address_line_2", 1)
	add("city", 2)
	add("post_code", 3)
	add("country_code", 4)
	return out
}

func parseSCBPostalAddress(values map[string]string) map[string]string {
	out := make(map[string]string)
	add := func(key string, value string) {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out[key] = trimmed
		}
	}
	add("care_of", values["COAdress"])
	add("street_address", values["Gatuadress"])
	add("post_code", values["PostNr"])
	add("city", values["PostOrt"])
	return out
}

func orderedSNICodes(values ...string) []orderedSniCode {
	codes := make([]orderedSniCode, 0, len(values))
	for i, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			codes = append(codes, orderedSniCode{Code: trimmed, Position: i + 1})
		}
	}
	return codes
}

func organizationNumberFromPeOrgNr(value string) string {
	value = normalizeOrganizationNumber(value)
	if len(value) == 12 && strings.HasPrefix(value, "16") {
		return value[2:]
	}
	return ""
}
