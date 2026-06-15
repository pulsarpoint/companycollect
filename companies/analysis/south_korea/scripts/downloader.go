package main

// Simple open-data collector for the South Korea company-source investigation.
//
// Supports:
//   - "bulk":     direct file download (e.g. Sirene stock zip/parquet)
//   - "api_page": page/offset-stepped JSON API download (Recherche, BODACC)
//
// It writes raw files under <data-root>/raw/{bulk,api}/, a per-file
// .metadata.json (with SHA-256), and is safe to run repeatedly.
//
// Usage:
//   go run downloader.go \
//     --data-root /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/france \
//     --sources      ./sources.example.json

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Source struct {
	Name      string            `json:"name"`
	Slug      string            `json:"slug"`
	Type      string            `json:"type"` // "bulk" or "api_page"
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	PageParam string            `json:"page_param"`
	PageStart int               `json:"page_start"`
	MaxPages  int               `json:"max_pages"`
}

type DownloadMeta struct {
	SourceName    string    `json:"source_name"`
	SourceURL     string    `json:"source_url"`
	RetrievedAt   time.Time `json:"retrieved_at"`
	HTTPStatus    int       `json:"http_status"`
	ContentType   string    `json:"content_type"`
	ContentLength int64     `json:"content_length"`
	SavedAs       string    `json:"saved_as"`
	SHA256        string    `json:"sha256"`
}

func main() {
	dataRoot := flag.String("data-root", "", "absolute path to the country data folder")
	sourcesPath := flag.String("sources", "", "path to a sources JSON file")
	flag.Parse()

	if *dataRoot == "" || *sourcesPath == "" {
		fmt.Fprintln(os.Stderr, "usage: downloader --data-root <dir> --sources <file>")
		os.Exit(2)
	}

	raw, err := os.ReadFile(*sourcesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read sources:", err)
		os.Exit(1)
	}
	var sources []Source
	if err := json.Unmarshal(raw, &sources); err != nil {
		fmt.Fprintln(os.Stderr, "parse sources:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	client := &http.Client{Timeout: 10 * time.Minute}
	for _, src := range sources {
		fmt.Printf("==> %s (%s)\n", src.Name, src.Type)
		if err := CollectSource(ctx, client, *dataRoot, src); err != nil {
			fmt.Fprintf(os.Stderr, "    error: %v\n", err)
			continue
		}
		fmt.Println("    done")
	}
}

func CollectSource(ctx context.Context, client *http.Client, dataRoot string, src Source) error {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	if src.Slug == "" {
		src.Slug = safeSlug(src.Name)
	}
	if src.Method == "" {
		src.Method = http.MethodGet
	}

	switch src.Type {
	case "bulk":
		out := filepath.Join(dataRoot, "raw", "bulk", src.Slug+extensionFromURL(src.URL))
		return downloadOne(ctx, client, src, src.URL, out)

	case "api_page":
		if src.PageParam == "" {
			return errors.New("api_page source requires page_param")
		}
		if src.MaxPages <= 0 {
			src.MaxPages = 1
		}
		for i := 0; i < src.MaxPages; i++ {
			page := src.PageStart + i
			u, err := addQueryParam(src.URL, src.PageParam, fmt.Sprintf("%d", page))
			if err != nil {
				return err
			}
			out := filepath.Join(dataRoot, "raw", "api", fmt.Sprintf("%s_%s_%d.json", src.Slug, src.PageParam, page))
			if err := downloadOne(ctx, client, src, u, out); err != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
		}
		return nil

	default:
		return fmt.Errorf("unsupported source type %q", src.Type)
	}
}

func downloadOne(ctx context.Context, client *http.Client, src Source, sourceURL string, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, src.Method, sourceURL, nil)
	if err != nil {
		return err
	}
	for k, v := range src.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "companycollect/0.1 open-data-research")
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download failed: %s returned HTTP %d", sourceURL, resp.StatusCode)
	}

	file, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer file.Close()

	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, h), resp.Body)
	if err != nil {
		return err
	}

	meta := DownloadMeta{
		SourceName:    src.Name,
		SourceURL:     sourceURL,
		RetrievedAt:   time.Now().UTC(),
		HTTPStatus:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: written,
		SavedAs:       outPath,
		SHA256:        hex.EncodeToString(h.Sum(nil)),
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outPath+".metadata.json", metaBytes, 0o644)
}

func addQueryParam(rawURL, key, value string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func extensionFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ".dat"
	}
	ext := filepath.Ext(u.Path)
	if ext == "" || len(ext) > 10 {
		return ".dat"
	}
	return ext
}

func safeSlug(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
