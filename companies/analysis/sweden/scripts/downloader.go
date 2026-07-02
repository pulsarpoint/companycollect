// Sweden public bulk-file downloader.
//
// This helper downloads the public Bolagsverket high-value-dataset ZIP files.
// It intentionally does not use the authenticated API. Company bulk files should
// not be downloaded more often than the allowed 7-day cadence unless a manual
// refresh is explicitly intended.
//
// Usage:
//   go run downloader.go
//
// Optional:
//   SWEDEN_DATA_ROOT=/path/to/companycollect/companies/data/sweden go run downloader.go

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type source struct {
	Name string
	Slug string
	URL  string
}

type downloadMeta struct {
	SourceName    string `json:"source_name"`
	SourceURL     string `json:"source_url"`
	RetrievedAt   string `json:"retrieved_at"`
	HTTPStatus    int    `json:"http_status"`
	ContentType   string `json:"content_type"`
	ContentLength int64  `json:"content_length"`
	SavedAs       string `json:"saved_as"`
	SHA256        string `json:"sha256"`
}

var sources = []source{
	{
		Name: "SCB/FDB bulk file",
		Slug: "scb_bulkfil.zip",
		URL:  "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip",
	},
	{
		Name: "Bolagsverket legal-register bulk file",
		Slug: "bolagsverket_bulkfil.zip",
		URL:  "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip",
	},
}

func main() {
	dataRoot := os.Getenv("SWEDEN_DATA_ROOT")
	if dataRoot == "" {
		dataRoot = "/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/sweden"
	}

	client := &http.Client{Timeout: 30 * time.Minute}
	ctx := context.Background()

	for _, src := range sources {
		outPath := filepath.Join(dataRoot, "raw", "bulk", src.Slug)
		if err := download(ctx, client, src, outPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func download(ctx context.Context, client *http.Client, src source, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "companycollect-sweden-open-data-research/0.1")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned HTTP %d", src.URL, resp.StatusCode)
	}

	tmpPath := outPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hasher), resp.Body)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	meta := downloadMeta{
		SourceName:    src.Name,
		SourceURL:     src.URL,
		RetrievedAt:   time.Now().UTC().Format(time.RFC3339),
		HTTPStatus:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: written,
		SavedAs:       outPath,
		SHA256:        hex.EncodeToString(hasher.Sum(nil)),
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath+".metadata.json", metaBytes, 0o644); err != nil {
		return err
	}

	fmt.Printf("downloaded %s -> %s (%d bytes)\n", src.Name, outPath, written)
	return nil
}
