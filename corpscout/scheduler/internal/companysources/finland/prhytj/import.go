package prhytj

import (
	"context"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	chwriter "github.com/pulsarpoint/corpscout/scheduler/internal/clickhouse"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

type Source struct{}

func (Source) Key() companysources.Key {
	return companysources.Key{Country: "finland", Source: SourceKey}
}

func (Source) DisplayName() string {
	return SourceName
}

func (Source) Import(ctx context.Context, opts companysources.ImportOptions) (companysources.ImportResult, error) {
	if opts.RunDir == "" {
		return companysources.ImportResult{}, errors.New("run dir is required")
	}
	if opts.ClickHouseNativeURL == "" {
		return companysources.ImportResult{}, errors.New("clickhouse native url is required")
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
	if err != nil {
		return companysources.ImportResult{}, err
	}
	defer writer.Close()

	runID := filepath.Base(opts.RunDir)
	run := RunContext{
		RunID:          runID,
		SourceExportID: uuid.New(),
		IngestedAt:     time.Now().UTC(),
	}
	snapshotPath := filepath.Join(opts.RunDir, "source.ndjson")
	entries := make([]NormalizedEntry, 0, batchSize)
	var seen int64

	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		if err := flushNormalizedEntries(ctx, writer, entries); err != nil {
			return err
		}
		entries = entries[:0]
		return nil
	}

	err = ParseSnapshot(ctx, snapshotPath, func(record ParsedRecord) error {
		if opts.Limit > 0 && seen >= opts.Limit {
			return nil
		}
		entries = append(entries, NormalizeParsedRecord(run, record))
		seen++
		if len(entries) < batchSize {
			return nil
		}
		return flush()
	})
	if err != nil {
		return companysources.ImportResult{}, err
	}
	if err := flush(); err != nil {
		return companysources.ImportResult{}, err
	}

	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: NormalizedTableNames(),
		ImportedRows:   seen,
	}, nil
}

func flushNormalizedEntries(ctx context.Context, writer *chwriter.Writer, entries []NormalizedEntry) error {
	insert := func(table string, columns []string, rows []map[string]any) error {
		if len(rows) == 0 {
			return nil
		}
		return writer.Insert(ctx, chwriter.Insert{
			Table:   table,
			Columns: columns,
			Rows:    rows,
		})
	}

	identifierRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.Identifiers {
			identifierRows = append(identifierRows, row.ClickHouseRow())
		}
	}
	if err := insert(identifiersTable, identifierColumns, identifierRows); err != nil {
		return err
	}

	statusRows := make([]map[string]any, 0)
	for _, entry := range entries {
		if entry.Status != nil {
			statusRows = append(statusRows, entry.Status.ClickHouseRow())
		}
	}
	if err := insert(statusesTable, statusColumns, statusRows); err != nil {
		return err
	}

	nameRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.Names {
			nameRows = append(nameRows, row.ClickHouseRow())
		}
	}
	if err := insert(namesTable, nameColumns, nameRows); err != nil {
		return err
	}

	businessLineRows := make([]map[string]any, 0)
	for _, entry := range entries {
		if entry.BusinessLine != nil {
			businessLineRows = append(businessLineRows, entry.BusinessLine.ClickHouseRow())
		}
	}
	if err := insert(businessLinesTable, businessLineColumns, businessLineRows); err != nil {
		return err
	}

	businessLineDescriptionRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.BusinessLineDescriptions {
			businessLineDescriptionRows = append(businessLineDescriptionRows, row.ClickHouseRow())
		}
	}
	if err := insert(businessLineDescriptionsTable, businessLineDescriptionColumns, businessLineDescriptionRows); err != nil {
		return err
	}

	websiteRows := make([]map[string]any, 0)
	for _, entry := range entries {
		if entry.Website != nil {
			websiteRows = append(websiteRows, entry.Website.ClickHouseRow())
		}
	}
	if err := insert(websitesTable, websiteColumns, websiteRows); err != nil {
		return err
	}

	companyFormRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.CompanyForms {
			companyFormRows = append(companyFormRows, row.ClickHouseRow())
		}
	}
	if err := insert(companyFormsTable, companyFormColumns, companyFormRows); err != nil {
		return err
	}

	companyFormDescriptionRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.CompanyFormDescriptions {
			companyFormDescriptionRows = append(companyFormDescriptionRows, row.ClickHouseRow())
		}
	}
	if err := insert(companyFormDescriptionsTable, companyFormDescriptionColumns, companyFormDescriptionRows); err != nil {
		return err
	}

	companySituationRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.CompanySituations {
			companySituationRows = append(companySituationRows, row.ClickHouseRow())
		}
	}
	if err := insert(companySituationsTable, companySituationColumns, companySituationRows); err != nil {
		return err
	}

	companySituationDescriptionRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.CompanySituationDescriptions {
			companySituationDescriptionRows = append(companySituationDescriptionRows, row.ClickHouseRow())
		}
	}
	if err := insert(companySituationDescriptionsTable, companySituationDescriptionColumns, companySituationDescriptionRows); err != nil {
		return err
	}

	registeredEntryRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.RegisteredEntries {
			registeredEntryRows = append(registeredEntryRows, row.ClickHouseRow())
		}
	}
	if err := insert(registeredEntriesTable, registeredEntryColumns, registeredEntryRows); err != nil {
		return err
	}

	registeredEntryDescriptionRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.RegisteredEntryDescriptions {
			registeredEntryDescriptionRows = append(registeredEntryDescriptionRows, row.ClickHouseRow())
		}
	}
	if err := insert(registeredEntryDescriptionsTable, registeredEntryDescriptionColumns, registeredEntryDescriptionRows); err != nil {
		return err
	}

	addressRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.Addresses {
			addressRows = append(addressRows, row.ClickHouseRow())
		}
	}
	if err := insert(addressesTable, addressColumns, addressRows); err != nil {
		return err
	}

	addressPostOfficeRows := make([]map[string]any, 0)
	for _, entry := range entries {
		for _, row := range entry.AddressPostOffices {
			addressPostOfficeRows = append(addressPostOfficeRows, row.ClickHouseRow())
		}
	}
	if err := insert(addressPostOfficesTable, addressPostOfficeColumns, addressPostOfficeRows); err != nil {
		return err
	}

	return nil
}
