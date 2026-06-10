package prhxbrl

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultMaxStatements = 50

type DownloadOptions struct {
	Queries              *db.Queries
	HTTPClient           *http.Client
	SourceID             uuid.UUID
	ActionRunID          uuid.UUID
	TemporalWorkflowID   string
	TemporalRunID        string
	RunDir               string
	ManifestRelativePath string
	SourceURL            string
	UserAgentRequired    bool
	RegisteredDateStart  string
	RegisteredDateEnd    string
	MaxStatements        int32
	RetryFailed          bool
}

type StatementXMLDownload struct {
	Path      string
	SHA256    string
	SizeBytes int64
	SourceURL string
}

type ManifestStatement struct {
	BusinessID       string `json:"business_id"`
	FinancialDate    string `json:"financial_date"`
	RegistrationDate string `json:"registration_date,omitempty"`
	SourceURL        string `json:"source_url"`
	DownloadStatus   string `json:"download_status"`
	XMLPath          string `json:"xml_path,omitempty"`
	XMLSHA256        string `json:"xml_sha256,omitempty"`
	XMLSizeBytes     int64  `json:"xml_size_bytes,omitempty"`
	ErrorMessage     string `json:"error_message,omitempty"`
}

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	_ = ctx
	_ = opts
	return companysources.DownloadedFile{}, errors.New("Finland PRH financial XBRL download requires the source-specific Temporal action")
}

func Download(ctx context.Context, opts DownloadOptions) (companysources.DownloadedFile, error) {
	if opts.Queries == nil {
		return companysources.DownloadedFile{}, errors.New("PRH XBRL queries are required")
	}
	maxStatements := opts.MaxStatements
	if maxStatements <= 0 {
		maxStatements = defaultMaxStatements
	}
	if maxStatements > 1000 {
		return companysources.DownloadedFile{}, errors.New("max_statements must be 1000 or less")
	}
	sourceURL := strings.TrimSpace(opts.SourceURL)
	if sourceURL == "" {
		sourceURL = DefaultURL
	}
	manifestRelativePath := strings.TrimSpace(opts.ManifestRelativePath)
	if manifestRelativePath == "" {
		manifestRelativePath = "statements.ndjson"
	}

	startDate, err := parseDateToPgtype(opts.RegisteredDateStart)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	endDate, err := parseDateToPgtype(opts.RegisteredDateEnd)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	registeredDateStart := startDate.Time.Format(time.DateOnly)
	registeredDateEnd := endDate.Time.Format(time.DateOnly)

	window, err := opts.Queries.UpsertFinlandPRHXBRLDiscoveryWindow(ctx, db.UpsertFinlandPRHXBRLDiscoveryWindowParams{
		SourceID:            opts.SourceID,
		RegisteredDateStart: startDate,
		RegisteredDateEnd:   endDate,
		ActionRunID:         pgUUID(opts.ActionRunID),
		TemporalWorkflowID:  optionalText(opts.TemporalWorkflowID),
		TemporalRunID:       optionalText(opts.TemporalRunID),
	})
	if err != nil {
		return companysources.DownloadedFile{}, errors.Wrap(err, "upsert PRH XBRL discovery window")
	}

	totalResults, pagesDiscovered, statementsDiscovered, lastCompletedPage, err := discoverStatementArtifacts(ctx, opts, window.ID, sourceURL, registeredDateStart, registeredDateEnd, maxStatements)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	if _, err := opts.Queries.CompleteFinlandPRHXBRLDiscoveryWindow(ctx, db.CompleteFinlandPRHXBRLDiscoveryWindowParams{
		TotalResults:         totalResults,
		PagesDiscovered:      pagesDiscovered,
		StatementsDiscovered: statementsDiscovered,
		LastCompletedPage:    lastCompletedPage,
		ID:                   window.ID,
	}); err != nil {
		return companysources.DownloadedFile{}, errors.Wrap(err, "complete PRH XBRL discovery window")
	}

	result, err := downloadClaimedStatementArtifacts(ctx, opts, sourceURL, startDate, endDate, maxStatements)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	manifestRows, err := listStatementManifestRows(ctx, opts, startDate, endDate)
	if err != nil {
		if result.downloadErr != nil {
			return companysources.DownloadedFile{}, errors.WithSecondaryError(err, result.downloadErr)
		}
		return companysources.DownloadedFile{}, err
	}

	manifestPath, err := companysources.SafeRunRelativePath(opts.RunDir, manifestRelativePath)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	written, err := writeStatementsManifest(manifestPath, manifestRows)
	if err != nil {
		if result.downloadErr != nil {
			return companysources.DownloadedFile{}, errors.WithSecondaryError(err, result.downloadErr)
		}
		return companysources.DownloadedFile{}, err
	}
	if result.downloadErr != nil {
		return companysources.DownloadedFile{}, result.downloadErr
	}
	return companysources.DownloadedFile{
		FileKey:            "statements_manifest",
		Kind:               "source_manifest",
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       manifestRelativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}

type statementDownloadResult struct {
	downloadFailureCount int
	firstDownloadFailure error
	downloadErr          error
}

func (result *statementDownloadResult) addDownloadFailure(err error) {
	result.downloadFailureCount++
	if result.firstDownloadFailure == nil {
		result.firstDownloadFailure = err
	}
	result.downloadErr = errors.Wrapf(result.firstDownloadFailure, "%d PRH XBRL statement XML downloads failed", result.downloadFailureCount)
}

func discoverStatementArtifacts(ctx context.Context, opts DownloadOptions, windowID uuid.UUID, sourceURL string, registeredDateStart string, registeredDateEnd string, maxStatements int32) (int64, int32, int64, int32, error) {
	var totalResults int64
	var pagesDiscovered int32
	var statementsDiscovered int64
	var lastCompletedPage int32

	for pageNumber := 1; ; pageNumber++ {
		page, err := downloadDiscoveryPage(ctx, opts.HTTPClient, sourceURL, registeredDateStart, registeredDateEnd, pageNumber, opts.UserAgentRequired)
		if err != nil {
			return 0, 0, 0, 0, errors.Wrapf(err, "download PRH XBRL discovery page %d", pageNumber)
		}
		if page.TotalResults > 0 {
			totalResults = page.TotalResults
		}
		pagesDiscovered++
		lastCompletedPage = int32(pageNumber)

		for _, statement := range page.Financials {
			if statementsDiscovered >= int64(maxStatements) {
				break
			}
			businessID := strings.TrimSpace(statement.BusinessID)
			if businessID == "" {
				return 0, 0, 0, 0, errors.New("PRH XBRL statement business id is required")
			}
			financialDateValue := strings.TrimSpace(statement.FinancialDate)
			financialDate, err := parseDateToPgtype(financialDateValue)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			registrationDate, err := nullableDate(statement.RegistrationDate)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			statementURL, err := buildFinancialStatementURL(sourceURL, businessID, financialDateValue)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			if _, err := opts.Queries.UpsertFinlandPRHXBRLStatementArtifact(ctx, db.UpsertFinlandPRHXBRLStatementArtifactParams{
				SourceID:             opts.SourceID,
				BusinessID:           businessID,
				FinancialDate:        financialDate,
				RegistrationDate:     registrationDate,
				SourceUrl:            statementURL,
				FirstDiscoveredRunID: pgUUID(opts.ActionRunID),
				LatestActionRunID:    pgUUID(opts.ActionRunID),
			}); err != nil {
				return 0, 0, 0, 0, errors.Wrap(err, "upsert PRH XBRL statement artifact")
			}
			statementsDiscovered++
		}

		if _, err := opts.Queries.UpdateFinlandPRHXBRLDiscoveryProgress(ctx, db.UpdateFinlandPRHXBRLDiscoveryProgressParams{
			TotalResults:         totalResults,
			PagesDiscovered:      pagesDiscovered,
			StatementsDiscovered: statementsDiscovered,
			LastCompletedPage:    lastCompletedPage,
			ID:                   windowID,
		}); err != nil {
			return 0, 0, 0, 0, errors.Wrap(err, "update PRH XBRL discovery progress")
		}

		if len(page.Financials) == 0 {
			break
		}
		if totalResults > 0 && statementsDiscovered >= totalResults {
			break
		}
		if statementsDiscovered >= int64(maxStatements) && pagesDiscovered > 0 {
			break
		}
	}
	return totalResults, pagesDiscovered, statementsDiscovered, lastCompletedPage, nil
}

func downloadClaimedStatementArtifacts(ctx context.Context, opts DownloadOptions, sourceURL string, registeredDateStart pgtype.Date, registeredDateEnd pgtype.Date, maxStatements int32) (statementDownloadResult, error) {
	artifacts, err := opts.Queries.ListFinlandPRHXBRLStatementArtifactsToDownload(ctx, db.ListFinlandPRHXBRLStatementArtifactsToDownloadParams{
		SourceID:            opts.SourceID,
		RegisteredDateStart: registeredDateStart,
		RegisteredDateEnd:   registeredDateEnd,
		RetryFailed:         opts.RetryFailed,
		LatestActionRunID:   pgUUID(opts.ActionRunID),
		RowLimit:            maxStatements,
	})
	if err != nil {
		return statementDownloadResult{}, errors.Wrap(err, "list PRH XBRL statement artifacts to download")
	}

	result := statementDownloadResult{}
	for _, artifact := range artifacts {
		claimed, err := opts.Queries.MarkFinlandPRHXBRLStatementArtifactDownloading(ctx, db.MarkFinlandPRHXBRLStatementArtifactDownloadingParams{
			LatestActionRunID:   pgUUID(opts.ActionRunID),
			ID:                  artifact.ID,
			SourceID:            opts.SourceID,
			RegisteredDateStart: registeredDateStart,
			RegisteredDateEnd:   registeredDateEnd,
			RetryFailed:         opts.RetryFailed,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return result, errors.Wrapf(err, "claim PRH XBRL statement artifact id=%s business_id=%s financial_date=%s", artifact.ID, artifact.BusinessID, pgDateString(artifact.FinancialDate))
		}

		financialDate := pgDateString(claimed.FinancialDate)
		xml, err := downloadStatementXML(ctx, opts.HTTPClient, sourceURL, claimed.BusinessID, financialDate, opts.RunDir, opts.UserAgentRequired)
		if err != nil {
			downloadErr := errors.Wrapf(err, "download PRH XBRL statement XML artifact_id=%s business_id=%s financial_date=%s", claimed.ID, claimed.BusinessID, financialDate)
			_, markErr := markClaimedStatementArtifactFailed(ctx, opts, claimed, downloadErr)
			if markErr != nil {
				return result, markErr
			}
			result.addDownloadFailure(downloadErr)
			continue
		}

		if _, err := opts.Queries.MarkFinlandPRHXBRLStatementArtifactSucceeded(ctx, db.MarkFinlandPRHXBRLStatementArtifactSucceededParams{
			XmlPath:           xml.Path,
			XmlSha256:         xml.SHA256,
			XmlSizeBytes:      xml.SizeBytes,
			LatestActionRunID: pgUUID(opts.ActionRunID),
			ID:                claimed.ID,
			SourceID:          opts.SourceID,
		}); err != nil {
			return result, errors.Wrapf(err, "mark PRH XBRL statement artifact succeeded id=%s business_id=%s financial_date=%s", claimed.ID, claimed.BusinessID, financialDate)
		}
	}
	return result, nil
}

func listStatementManifestRows(ctx context.Context, opts DownloadOptions, registeredDateStart pgtype.Date, registeredDateEnd pgtype.Date) ([]ManifestStatement, error) {
	artifacts, err := opts.Queries.ListFinlandPRHXBRLStatementManifestArtifacts(ctx, db.ListFinlandPRHXBRLStatementManifestArtifactsParams{
		SourceID:            opts.SourceID,
		RegisteredDateStart: registeredDateStart,
		RegisteredDateEnd:   registeredDateEnd,
		LatestActionRunID:   pgUUID(opts.ActionRunID),
	})
	if err != nil {
		return nil, errors.Wrap(err, "list PRH XBRL statement manifest artifacts")
	}

	rows := make([]ManifestStatement, 0, len(artifacts))
	for _, artifact := range artifacts {
		rows = append(rows, manifestRowFromArtifact(artifact))
	}
	return rows, nil
}

func markClaimedStatementArtifactFailed(ctx context.Context, opts DownloadOptions, artifact db.FinancialXbrlFinlandPrhXbrlStatementArtifact, failure error) (db.FinancialXbrlFinlandPrhXbrlStatementArtifact, error) {
	failed, err := opts.Queries.MarkFinlandPRHXBRLStatementArtifactFailed(ctx, db.MarkFinlandPRHXBRLStatementArtifactFailedParams{
		LatestActionRunID: pgUUID(opts.ActionRunID),
		LastErrorMessage:  failure.Error(),
		ID:                artifact.ID,
		SourceID:          opts.SourceID,
	})
	if err != nil {
		return db.FinancialXbrlFinlandPrhXbrlStatementArtifact{}, errors.WithSecondaryError(
			errors.Wrapf(err, "mark PRH XBRL statement artifact failed id=%s business_id=%s financial_date=%s", artifact.ID, artifact.BusinessID, pgDateString(artifact.FinancialDate)),
			failure,
		)
	}
	return failed, nil
}

func parseDateToPgtype(value string) (pgtype.Date, error) {
	parsed, err := time.Parse(time.DateOnly, strings.TrimSpace(value))
	if err != nil {
		return pgtype.Date{}, errors.Wrap(err, "parse PRH XBRL date")
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}

func nullableDate(value string) (pgtype.Date, error) {
	if strings.TrimSpace(value) == "" {
		return pgtype.Date{}, nil
	}
	return parseDateToPgtype(value)
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

func optionalText(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func pgDateString(value pgtype.Date) string {
	if !value.Valid {
		return ""
	}
	return value.Time.Format(time.DateOnly)
}

func manifestRowFromArtifact(artifact db.FinancialXbrlFinlandPrhXbrlStatementArtifact) ManifestStatement {
	row := ManifestStatement{
		BusinessID:       artifact.BusinessID,
		FinancialDate:    pgDateString(artifact.FinancialDate),
		RegistrationDate: pgDateString(artifact.RegistrationDate),
		SourceURL:        artifact.SourceUrl,
		DownloadStatus:   artifact.DownloadStatus,
	}
	if artifact.XmlPath != nil {
		row.XMLPath = *artifact.XmlPath
	}
	if artifact.XmlSha256 != nil {
		row.XMLSHA256 = *artifact.XmlSha256
	}
	if artifact.XmlSizeBytes != nil {
		row.XMLSizeBytes = *artifact.XmlSizeBytes
	}
	if artifact.LastErrorMessage != nil {
		row.ErrorMessage = *artifact.LastErrorMessage
	}
	return row
}

func downloadStatementXML(ctx context.Context, client *http.Client, baseURL string, businessID string, financialDate string, runDir string, userAgentRequired bool) (StatementXMLDownload, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if err := validateStatementPathSegment("business id", businessID); err != nil {
		return StatementXMLDownload{}, err
	}
	if err := validateStatementPathSegment("financial date", financialDate); err != nil {
		return StatementXMLDownload{}, err
	}
	statementURL, err := buildFinancialStatementURL(baseURL, businessID, financialDate)
	if err != nil {
		return StatementXMLDownload{}, err
	}
	outputPath, err := companysources.SafeRunRelativePath(runDir, filepath.Join("statements", businessID, financialDate+".xml"))
	if err != nil {
		return StatementXMLDownload{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement directory")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statementURL, nil)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement request")
	}
	if userAgentRequired {
		req.Header.Set("User-Agent", companysources.DownloadUserAgent)
	}
	resp, err := client.Do(req)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "download PRH XBRL statement XML")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return StatementXMLDownload{}, errors.Errorf("download PRH XBRL statement XML: status %d", resp.StatusCode)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "create PRH XBRL statement XML")
	}
	defer file.Close()

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, hasher), resp.Body)
	if err != nil {
		return StatementXMLDownload{}, errors.Wrap(err, "write PRH XBRL statement XML")
	}
	return StatementXMLDownload{
		Path:      outputPath,
		SHA256:    hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes: size,
		SourceURL: statementURL,
	}, nil
}

func validateStatementPathSegment(name string, value string) error {
	if value == "" {
		return errors.Errorf("PRH XBRL statement %s is required", name)
	}
	if filepath.IsAbs(value) || value == "." || value == ".." || strings.ContainsAny(value, `/\`) || filepath.Clean(value) != value {
		return errors.Errorf("PRH XBRL statement %s %q is not a safe path segment", name, value)
	}
	return nil
}

func writeStatementsManifest(path string, rows []ManifestStatement) (companysources.FileWriteResult, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create PRH XBRL manifest directory")
	}
	file, err := os.Create(path)
	if err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "create PRH XBRL manifest")
	}
	defer file.Close()

	hasher := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hasher))
	var bytesWritten int64
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "marshal PRH XBRL manifest row")
		}
		n, err := writer.Write(line)
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "write PRH XBRL manifest row")
		}
		bytesWritten += int64(n)
		n, err = writer.WriteString("\n")
		if err != nil {
			return companysources.FileWriteResult{}, errors.Wrap(err, "write PRH XBRL manifest newline")
		}
		bytesWritten += int64(n)
	}
	if err := writer.Flush(); err != nil {
		return companysources.FileWriteResult{}, errors.Wrap(err, "flush PRH XBRL manifest")
	}
	return companysources.FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: bytesWritten,
		RecordsWritten:     int64(len(rows)),
	}, nil
}
