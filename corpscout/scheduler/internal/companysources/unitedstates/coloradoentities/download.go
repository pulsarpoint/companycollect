package coloradoentities

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

const defaultSocrataPageLimit = 50000
const defaultSocrataOrder = "entityid"

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	if opts.FileKind != "" && opts.FileKind != "source_snapshot" {
		return companysources.DownloadedFile{}, errors.Errorf("unsupported Colorado entities file kind %q", opts.FileKind)
	}
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}
	path, err := companysources.SafeRunRelativePath(opts.RunDir, relativePath)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}

	records, err := downloadAllSocrataRecords(ctx, opts.SourceURL, opts.UserAgentRequired, intConfig(opts.Config, "page_limit", defaultSocrataPageLimit))
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	written, err := companysources.WriteRawMessagesAsNDJSON(path, records)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       relativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}

func downloadAllSocrataRecords(ctx context.Context, sourceURL string, userAgentRequired bool, pageLimit int) ([]json.RawMessage, error) {
	if pageLimit <= 0 {
		pageLimit = defaultSocrataPageLimit
	}
	var all []json.RawMessage
	for offset := 0; ; offset += pageLimit {
		pageURL, err := socrataPageURL(sourceURL, pageLimit, offset)
		if err != nil {
			return nil, err
		}
		page, err := companysources.DownloadJSONArray(ctx, http.DefaultClient, pageURL, userAgentRequired)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < pageLimit {
			return all, nil
		}
	}
}

func socrataPageURL(sourceURL string, limit int, offset int) (string, error) {
	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return "", errors.Wrap(err, "parse Colorado entities source url")
	}
	query := parsed.Query()
	query.Set("$limit", strconv.Itoa(limit))
	query.Set("$offset", strconv.Itoa(offset))
	query.Set("$order", defaultSocrataOrder)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func intConfig(config map[string]any, key string, fallback int) int {
	value, ok := config[key]
	if !ok {
		return fallback
	}
	switch typed := value.(type) {
	case int:
		if typed > 0 {
			return typed
		}
	case int32:
		if typed > 0 {
			return int(typed)
		}
	case int64:
		if typed > 0 {
			return int(typed)
		}
	case float64:
		if typed > 0 {
			return int(typed)
		}
	case string:
		parsed, err := strconv.Atoi(typed)
		if err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
