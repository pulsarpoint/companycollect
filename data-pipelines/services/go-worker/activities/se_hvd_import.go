package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"

	"github.com/pulsarpoint/data-pipelines/contracts"
)

type seCompanyPayload struct {
	OrganizationNumber  string          `json:"organization_number"`
	OrganizationName    string          `json:"organization_name,omitempty"`
	RegistrationStatus  string          `json:"registration_status,omitempty"`
	LegalForm           string          `json:"legal_form,omitempty"`
	LegalFormCode       string          `json:"legal_form_code,omitempty"`
	BusinessDescription string          `json:"business_description,omitempty"`
	SNICodes            []seSNICode     `json:"sni_codes,omitempty"`
	PostalAddress       map[string]any  `json:"postal_address,omitempty"`
	SourceRecord        json.RawMessage `json:"source_record"`
}

type seSNICode struct {
	Code  string `json:"code,omitempty"`
	Label string `json:"label,omitempty"`
}

func (a *GoActivities) ImportSEHVDBulk(ctx context.Context, params contracts.ImportSEHVDBulkParams) (int, error) {
	written := 0
	sourceDatasets := sourceDatasetSummary(params.Files)
	for _, file := range params.Files {
		fileWritten := 0
		records := make([]seCompanyPayload, 0, sourceImportBatchSize)
		flush := func() error {
			if len(records) == 0 {
				return nil
			}
			batchWritten, err := a.insertSERawRecords(ctx, records, params.RunID, sourceDatasets)
			if err != nil {
				return err
			}
			fileWritten += batchWritten
			written += batchWritten
			records = records[:0]
			recordHeartbeat(ctx, map[string]any{
				"source":  file.Source,
				"dataset": file.Dataset,
				"file":    file.FilePath,
				"written": written,
			})
			return nil
		}

		var flushErr error
		err := streamSECompanyRecords(file, func(record seCompanyPayload) error {
			records = append(records, record)
			if len(records) < sourceImportBatchSize {
				return nil
			}
			flushErr = flush()
			return flushErr
		})
		if err != nil {
			if flushErr != nil {
				return written, errors.Wrapf(err, "upsert %s %s %s", file.Source, file.Dataset, file.FilePath)
			}
			return written, errors.Wrapf(err, "import %s %s %s", file.Source, file.Dataset, file.FilePath)
		}
		if err := flush(); err != nil {
			return written, errors.Wrapf(err, "upsert %s %s %s", file.Source, file.Dataset, file.FilePath)
		}

		recordHeartbeat(ctx, map[string]any{
			"source":  file.Source,
			"dataset": file.Dataset,
			"file":    file.FilePath,
			"written": written,
		})
		slog.Info("imported Sweden HVD source file",
			"source", file.Source,
			"dataset", file.Dataset,
			"file_path", file.FilePath,
			"records", fileWritten,
			"run_id", params.RunID,
		)
	}
	return written, nil
}

func streamSECompanyRecords(file contracts.DownloadedSourceFile, handle func(seCompanyPayload) error) error {
	reader, err := openDownloadedSourceFile(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	handleRaw := func(rawRecord json.RawMessage) error {
		record, ok, err := seCompanyPayloadFromRaw(rawRecord)
		if err != nil || !ok {
			return err
		}
		return handle(record)
	}
	if isJSONLSource(file) {
		return forEachJSONLine(reader, handleRaw)
	}
	return streamJSONRecords(reader, []string{"data", "records", "organisationer", "organizations", "documents"}, handleRaw)
}

func seCompanyPayloadFromRaw(rawRecord json.RawMessage) (seCompanyPayload, bool, error) {
	var record map[string]any
	decoder := json.NewDecoder(bytes.NewReader(rawRecord))
	decoder.UseNumber()
	if err := decoder.Decode(&record); err != nil {
		return seCompanyPayload{}, false, errors.Wrap(err, "parse Sweden HVD record")
	}

	organizationNumber := normalizeSEOrganizationNumber(firstNonEmptyString(
		anyString(record, "organization_number"),
		anyString(record, "organisation_number"),
		anyString(record, "organisationsnummer"),
		anyString(record, "identitetsbeteckning"),
		anyStringPath(record, "organisationsidentitet", "identitetsbeteckning"),
		anyStringPath(record, "organisation", "identitetsbeteckning"),
	))
	if organizationNumber == "" {
		return seCompanyPayload{}, false, nil
	}

	legalForm, legalFormCode := seLegalForm(record)
	status := firstNonEmptyString(
		anyString(record, "registration_status"),
		anyString(record, "status"),
		seRegistrationStatus(record),
	)
	payload := seCompanyPayload{
		OrganizationNumber: organizationNumber,
		OrganizationName: firstNonEmptyString(
			anyString(record, "organization_name"),
			anyString(record, "organisation_name"),
			anyString(record, "organisationsnamn"),
			anyString(record, "namn"),
		),
		RegistrationStatus:  status,
		LegalForm:           legalForm,
		LegalFormCode:       legalFormCode,
		BusinessDescription: firstNonEmptyString(anyString(record, "business_description"), anyString(record, "verksamhetsbeskrivning"), anyString(record, "verksamhetstext")),
		SNICodes:            seSNICodes(record),
		PostalAddress:       sePostalAddress(record),
		SourceRecord:        cloneRawMessage(rawRecord),
	}
	return payload, true, nil
}

func normalizeSEOrganizationNumber(value string) string {
	value = strings.TrimSpace(value)
	value = strings.NewReplacer("-", "", " ", "").Replace(value)
	return value
}

func seLegalForm(record map[string]any) (string, string) {
	form := anyMap(record, "organisationsform")
	label := firstNonEmptyString(
		anyString(record, "legal_form"),
		anyString(record, "company_type"),
		anyString(form, "klartext"),
		anyString(form, "text"),
		anyString(form, "namn"),
	)
	code := firstNonEmptyString(
		anyString(record, "legal_form_code"),
		anyString(form, "kod"),
		anyString(form, "code"),
	)
	if label == "" {
		label = code
	}
	return label, code
}

func seRegistrationStatus(record map[string]any) string {
	deregistered, ok := seDeregistered(record)
	if !ok {
		return ""
	}
	if deregistered {
		return "inactive"
	}
	return "active"
}

func seDeregistered(record map[string]any) (bool, bool) {
	if value, ok := anyBool(record, "avregistrerad"); ok {
		return value, true
	}
	for _, key := range []string{"avregistreradOrganisation", "deregistered_organization"} {
		if value, ok := anyBool(anyMap(record, key), "avregistrerad"); ok {
			return value, true
		}
		if value, ok := anyBool(anyMap(record, key), "deregistered"); ok {
			return value, true
		}
	}
	return false, false
}

func anyBool(values map[string]any, key string) (bool, bool) {
	value := anyValue(values, key)
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		if normalized == "true" || normalized == "ja" || normalized == "yes" || normalized == "1" {
			return true, true
		}
		if normalized == "false" || normalized == "nej" || normalized == "no" || normalized == "0" {
			return false, true
		}
	default:
		return false, false
	}
	return false, false
}

func seSNICodes(record map[string]any) []seSNICode {
	var codes []seSNICode
	addCode := func(values map[string]any) {
		if values == nil {
			return
		}
		code := firstNonEmptyString(anyString(values, "code"), anyString(values, "kod"), anyString(values, "sni_code"))
		label := firstNonEmptyString(anyString(values, "label"), anyString(values, "klartext"), anyString(values, "text"), anyString(values, "description"))
		if code == "" && label == "" {
			return
		}
		codes = append(codes, seSNICode{Code: code, Label: label})
	}

	for _, item := range anyMapArray(record, "sni_codes") {
		addCode(item)
	}
	for _, key := range []string{"naringsgrenOrganisation", "näringsgrenOrganisation"} {
		for _, item := range anyMapArray(record, key) {
			if nested := anyMap(item, "sni"); nested != nil {
				addCode(nested)
				continue
			}
			addCode(item)
		}
	}
	return codes
}

func sePostalAddress(record map[string]any) map[string]any {
	address := anyMap(record, "postal_address")
	if address == nil {
		address = anyMap(anyMap(record, "postadressOrganisation"), "postadress")
	}
	out := make(map[string]any)
	addString := func(key, value string) {
		if value != "" {
			out[key] = value
		}
	}
	addString("post_code", firstNonEmptyString(anyString(address, "post_code"), anyString(address, "postnummer"), anyString(address, "postal_code")))
	addString("city", firstNonEmptyString(anyString(address, "city"), anyString(address, "postort"), anyString(address, "ort")))
	addString("street_address", firstNonEmptyString(anyString(address, "street_address"), anyString(address, "utdelningsadress"), anyString(address, "utdelningsadress1"), anyString(address, "adress")))
	addString("care_of", firstNonEmptyString(anyString(address, "care_of"), anyString(address, "co"), anyString(address, "co_adress")))
	if len(out) == 0 {
		return nil
	}
	return out
}

func (a *GoActivities) insertSERawRecords(ctx context.Context, records []seCompanyPayload, runID, sourceDatasets string) (int, error) {
	written := 0
	for start := 0; start < len(records); start += sourceImportBatchSize {
		end := min(start+sourceImportBatchSize, len(records))
		batch := &pgx.Batch{}
		for _, record := range records[start:end] {
			rawPayload, err := json.Marshal(record)
			if err != nil {
				return written, errors.Wrapf(err, "marshal Sweden HVD payload %s", record.OrganizationNumber)
			}
			sniCodes, err := json.Marshal(emptySNICodesIfNil(record.SNICodes))
			if err != nil {
				return written, errors.Wrapf(err, "marshal Sweden HVD SNI codes %s", record.OrganizationNumber)
			}
			postalAddress, err := json.Marshal(emptyMapIfNil(record.PostalAddress))
			if err != nil {
				return written, errors.Wrapf(err, "marshal Sweden HVD postal address %s", record.OrganizationNumber)
			}
			batch.Queue(`
				INSERT INTO se_workflow.raw_records (
					source_native_id, organization_number, organization_name, registration_status,
					legal_form, business_description, country_iso2, sni_codes, postal_address,
					raw_payload, payload_hash, run_id
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (organization_number, payload_hash) DO UPDATE
					SET last_seen_at = now(), is_current = true, run_id = EXCLUDED.run_id
			`, record.OrganizationNumber, record.OrganizationNumber, nullableString(record.OrganizationName),
				nullableString(record.RegistrationStatus), nullableString(record.LegalForm),
				nullableString(record.BusinessDescription), "SE", sniCodes, postalAddress,
				rawPayload, hashBytes(rawPayload), runID)
		}
		if err := execBatch(ctx, a.pool, batch); err != nil {
			return written, errors.Wrapf(err, "%s batch offset %d", sourceDatasets, start)
		}
		written += end - start
		recordHeartbeat(ctx, written)
	}
	return written, nil
}

func emptyMapIfNil(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	return values
}

func emptySNICodesIfNil(values []seSNICode) []seSNICode {
	if values == nil {
		return []seSNICode{}
	}
	return values
}
