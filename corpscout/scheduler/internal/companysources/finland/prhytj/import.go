package prhytj

import (
	"context"
	"path/filepath"
	"strings"
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

	hasSourceSnapshot := hasSelectedSourceFile(opts.Files)
	codeListFiles := selectedCodeListFiles(opts.Files)
	if !hasSourceSnapshot && len(codeListFiles) == 0 {
		return companysources.ImportResult{}, errors.New("selected source or code list file is required")
	}

	writer, err := chwriter.Open(ctx, opts.ClickHouseNativeURL)
	if err != nil {
		return companysources.ImportResult{}, err
	}
	defer writer.Close()

	importedTables := make([]string, 0)
	var importedRows int64
	if hasSourceSnapshot {
		snapshotPath, err := opts.RequireFilePath("source")
		if err != nil {
			return companysources.ImportResult{}, err
		}
		if opts.Truncate {
			if err := writer.TruncateTables(ctx, NormalizedTableNames()); err != nil {
				return companysources.ImportResult{}, err
			}
		}
		rows, err := importSourceSnapshot(ctx, writer, opts.RunDir, snapshotPath, batchSize, opts.Limit)
		if err != nil {
			return companysources.ImportResult{}, err
		}
		importedTables = append(importedTables, NormalizedTableNames()...)
		importedRows += rows
	}

	if len(codeListFiles) > 0 {
		rows, err := importCodeLists(ctx, writer, codeListFiles, opts.Truncate && hasSourceSnapshot, batchSize)
		if err != nil {
			return companysources.ImportResult{}, err
		}
		importedTables = append(importedTables, CodeListTableNames()...)
		importedRows += rows
	}

	return companysources.ImportResult{
		RunDir:         opts.RunDir,
		ImportedTables: importedTables,
		ImportedRows:   importedRows,
	}, nil
}

func importSourceSnapshot(ctx context.Context, writer *chwriter.Writer, runDir string, snapshotPath string, batchSize int, limit int64) (int64, error) {
	runID := filepath.Base(runDir)
	run := RunContext{
		RunID:          runID,
		SourceExportID: uuid.New(),
		IngestedAt:     time.Now().UTC(),
	}
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

	err := ParseSnapshot(ctx, snapshotPath, func(record ParsedRecord) error {
		if limit > 0 && seen >= limit {
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
		return 0, err
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return seen, nil
}

func hasSelectedSourceFile(files []companysources.SelectedSourceFile) bool {
	for _, file := range files {
		if file.FileKey == "source" {
			return true
		}
	}
	return false
}

func selectedCodeListFiles(files []companysources.SelectedSourceFile) []companysources.SelectedSourceFile {
	selected := make([]companysources.SelectedSourceFile, 0)
	for _, file := range files {
		if file.Kind == "code_list" || strings.HasPrefix(file.FileKey, "codelist_") {
			selected = append(selected, file)
		}
	}
	return selected
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
