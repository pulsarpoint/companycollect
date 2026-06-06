package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Source describes one downloadable open-data source.
//
// For Serbia the relevant sources are the three APR OpenAPI endpoints, all of
// type "bulk" (a single GET returns the whole dataset as JSON). See
// sources.example.json.
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

func CollectSource(ctx context.Context, client *http.Client, dataRoot string, src Source) error {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
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
		// APR endpoints have no file extension; force .json for clarity.
		if extensionFromURL(src.URL) == ".dat" {
			out = filepath.Join(dataRoot, "raw", "api", src.Slug+".json")
		}
		return downloadOne(ctx, client, src, src.URL, out)

	case "api_page":
		if src.PageParam == "" {
			return errors.New("api_page source requires page_param")
		}
		if src.PageStart == 0 {
			src.PageStart = 1
		}
		if src.MaxPages <= 0 {
			src.MaxPages = 1
		}

		for page := src.PageStart; page < src.PageStart+src.MaxPages; page++ {
			u, err := addQueryParam(src.URL, src.PageParam, fmt.Sprintf("%d", page))
			if err != nil {
				return err
			}
			out := filepath.Join(dataRoot, "raw", "api", fmt.Sprintf("%s_page_%d.json", src.Slug, page))
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

// main reads sources.example.json and collects every source into the country
// data folder. Usage:
//
//	go run ./scripts/downloader.go
func main() {
	dataRoot := "/Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/serbia"
	sourcesPath := "/Users/graovic/pulsarpoint/ppoint/companycollect/companies/analysis/serbia/scripts/sources.example.json"

	raw, err := os.ReadFile(sourcesPath)
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
	client := &http.Client{Timeout: 5 * time.Minute}
	for _, src := range sources {
		fmt.Printf("collecting %s ...\n", src.Name)
		if err := CollectSource(ctx, client, dataRoot, src); err != nil {
			fmt.Fprintf(os.Stderr, "  error: %v\n", err)
			continue
		}
		fmt.Printf("  done: %s\n", src.Slug)
	}
}
