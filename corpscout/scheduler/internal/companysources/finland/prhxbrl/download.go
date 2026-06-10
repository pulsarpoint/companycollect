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

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

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

func downloadStatementXML(ctx context.Context, client *http.Client, baseURL string, businessID string, financialDate string, runDir string, userAgentRequired bool) (StatementXMLDownload, error) {
	if client == nil {
		client = http.DefaultClient
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
