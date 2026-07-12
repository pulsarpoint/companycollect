package worklistbuilder

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

//go:embed builder.py
var builderScript string

type Config struct {
	Python         string
	CrawlID        string
	PagesPerDomain int
	Part           int
	OutputPath     string
	Rebuild        bool
}

type Result struct {
	Path   string
	Rows   int64
	Reused bool
}

func Selection(pagesPerDomain int) string {
	return fmt.Sprintf("pages%d", pagesPerDomain)
}

func DefaultDirectory(baseDirectory, crawlID string, pagesPerDomain int) string {
	return filepath.Join(baseDirectory, crawlID, "download", "worklists", Selection(pagesPerDomain))
}

func Path(directory string, part int) string {
	return filepath.Join(directory, fmt.Sprintf("part_%03d.parquet", part))
}

func Key(pagesPerDomain, part int) string {
	return filepath.ToSlash(filepath.Join("download", "worklists", Selection(pagesPerDomain), fmt.Sprintf("part_%03d.parquet", part)))
}

func Ensure(ctx context.Context, config Config) (Result, error) {
	if err := validate(config); err != nil {
		return Result{}, err
	}
	if !config.Rebuild {
		rows, err := parquetRows(config.OutputPath)
		if err == nil {
			return Result{Path: config.OutputPath, Rows: rows, Reused: true}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			// A corrupt or incomplete cached file is safe to replace.
			if removeErr := os.Remove(config.OutputPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return Result{}, errors.Wrap(removeErr, "remove invalid cached worklist")
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(config.OutputPath), 0o755); err != nil {
		return Result{}, errors.Wrap(err, "create worklist directory")
	}
	temporary, err := os.CreateTemp(filepath.Dir(config.OutputPath), ".worklist-*.parquet")
	if err != nil {
		return Result{}, errors.Wrap(err, "create temporary worklist path")
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Close(); err != nil {
		return Result{}, errors.Wrap(err, "close temporary worklist file")
	}
	var output []byte
	var commandErr error
	for attempt := 1; attempt <= 5; attempt++ {
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return Result{}, errors.Wrap(err, "prepare temporary worklist path")
		}
		command := exec.CommandContext(ctx, config.Python, "-c", builderScript,
			"--crawl", config.CrawlID,
			"--pages", fmt.Sprint(config.PagesPerDomain),
			"--part", fmt.Sprint(config.Part),
			"--out", temporaryPath,
		)
		output, commandErr = command.CombinedOutput()
		if commandErr == nil {
			break
		}
		if attempt == 5 || !isTransientBuildFailure(output) {
			return Result{}, errors.Wrapf(commandErr, "build worklist after %d attempt(s): %s", attempt, strings.TrimSpace(string(output)))
		}
		delay := time.Second << (attempt - 1)
		select {
		case <-ctx.Done():
			return Result{}, errors.Wrap(ctx.Err(), "wait to retry worklist build")
		case <-time.After(delay):
		}
	}
	rows, err := parquetRows(temporaryPath)
	if err != nil {
		return Result{}, errors.Wrap(err, "validate generated worklist")
	}
	if err := os.Rename(temporaryPath, config.OutputPath); err != nil {
		return Result{}, errors.Wrap(err, "commit generated worklist")
	}
	return Result{Path: config.OutputPath, Rows: rows}, nil
}

func isTransientBuildFailure(output []byte) bool {
	message := strings.ToLower(string(output))
	for _, indicator := range []string{
		"http 429", "http 500", "http 502", "http 503", "http 504",
		"connection reset", "temporarily unavailable", "timed out", "timeout", "unexpected eof",
	} {
		if strings.Contains(message, indicator) {
			return true
		}
	}
	return false
}

func validate(config Config) error {
	if strings.TrimSpace(config.Python) == "" || strings.TrimSpace(config.CrawlID) == "" || strings.TrimSpace(config.OutputPath) == "" {
		return errors.New("Python executable, crawl ID, and output path are required")
	}
	if config.PagesPerDomain < 1 {
		return errors.New("pages per domain must be at least 1")
	}
	if config.Part < 0 {
		return errors.New("part must be non-negative")
	}
	return nil
}

func parquetRows(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "stat worklist")
	}
	worklist, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return 0, errors.Wrap(err, "open worklist Parquet")
	}
	if worklist.NumRows() == 0 {
		return 0, errors.New("worklist is empty")
	}
	required := map[string]bool{
		"root_domain": false, "url": false, "warc_filename": false,
		"warc_record_offset": false, "warc_record_length": false,
	}
	for _, column := range worklist.Schema().Columns() {
		if len(column) == 1 {
			if _, exists := required[column[0]]; exists {
				required[column[0]] = true
			}
		}
	}
	for column, found := range required {
		if !found {
			return 0, errors.Newf("worklist is missing required column %q", column)
		}
	}
	return worklist.NumRows(), nil
}
