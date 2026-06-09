package companysources

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
)

const DownloadUserAgent = "Corpscout Company Source Downloader"

type DirectFileDownload struct {
	URL               string
	RunDir            string
	SourceFileName    string
	UserAgentRequired bool
}

type FileWriteResult struct {
	SourceFilePath     string
	ContentSHA256      string
	ContentLengthBytes int64
	RecordsWritten     int64
}

func DownloadDirectFile(ctx context.Context, client *http.Client, req DirectFileDownload) (FileWriteResult, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if req.URL == "" {
		return FileWriteResult{}, errors.New("source url is required")
	}
	if req.RunDir == "" {
		return FileWriteResult{}, errors.New("run dir is required")
	}
	if req.SourceFileName == "" {
		return FileWriteResult{}, errors.New("source file name is required")
	}
	if err := os.MkdirAll(req.RunDir, 0o755); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source run directory")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source download request")
	}
	if req.UserAgentRequired {
		httpReq.Header.Set("User-Agent", DownloadUserAgent)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "download source file")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return FileWriteResult{}, errors.Errorf("download source file: status %d", resp.StatusCode)
	}

	path := filepath.Join(req.RunDir, req.SourceFileName)
	file, err := os.Create(path)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create source file")
	}
	defer file.Close()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hasher), resp.Body)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "write source file")
	}

	return FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: written,
		RecordsWritten:     0,
	}, nil
}

func WriteRawMessagesAsNDJSON(path string, records []json.RawMessage) (FileWriteResult, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create ndjson directory")
	}
	file, err := os.Create(path)
	if err != nil {
		return FileWriteResult{}, errors.Wrap(err, "create ndjson file")
	}
	defer file.Close()

	hasher := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hasher))
	var bytesWritten int64
	for _, record := range records {
		n, err := writer.Write(record)
		if err != nil {
			return FileWriteResult{}, errors.Wrap(err, "write ndjson record")
		}
		bytesWritten += int64(n)
		n, err = writer.WriteString("\n")
		if err != nil {
			return FileWriteResult{}, errors.Wrap(err, "write ndjson newline")
		}
		bytesWritten += int64(n)
	}
	if err := writer.Flush(); err != nil {
		return FileWriteResult{}, errors.Wrap(err, "flush ndjson file")
	}

	return FileWriteResult{
		SourceFilePath:     path,
		ContentSHA256:      hex.EncodeToString(hasher.Sum(nil)),
		ContentLengthBytes: bytesWritten,
		RecordsWritten:     int64(len(records)),
	}, nil
}
