// Package warcinput prepares one WARC-oriented enrichment unit from the static catalog.
package warcinput

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/model"
	"cc-raw/fetch"
)

type Mode string

const (
	ModeEmpty     Mode = "empty"
	ModeRange     Mode = "range"
	ModeWholeFile Mode = "whole"
)

// Plan contains the selected pages for one catalog WARC. Opening it performs no WARC-object I/O
// for an empty selection.
type Plan struct {
	Items         []model.WorklistItem
	WARCIndex     uint32
	WARCFilename  string
	SelectedBytes int64
}

func (plan Plan) Empty() bool {
	return len(plan.Items) == 0
}

// Input is ready for the existing worker API. SelectedBytes is the sum after optional primary-page
// filtering; ObjectBytes is zero only for ModeEmpty because empty plans intentionally skip HEAD.
type Input struct {
	Items         []model.WorklistItem
	Getter        fetch.RangeGetter
	Mode          Mode
	WARCIndex     uint32
	WARCFilename  string
	SelectedBytes int64
	ObjectBytes   int64

	localFile *os.File
	localPath string
	closeOnce sync.Once
	closeErr  error
}

// LoadPlan derives the catalog paths and loads exactly one WARC index.
func LoadPlan(baseDirectory, crawlID, selection string, warcIndex uint32, primaryPagesOnly bool) (Plan, error) {
	if baseDirectory == "" {
		return Plan{}, errors.New("base directory is required")
	}
	if crawlID == "" {
		return Plan{}, errors.New("crawl ID is required")
	}
	if selection == "" {
		return Plan{}, errors.New("selection is required")
	}

	catalogDirectory := filepath.Join(baseDirectory, crawlID, "warc-index", selection)
	warc, items, err := catalog.LoadWARC(
		context.Background(),
		filepath.Join(catalogDirectory, "catalog.duckdb"),
		warcIndex,
	)
	if err != nil {
		return Plan{}, errors.Wrapf(err, "load WARC catalog index %d", warcIndex)
	}
	return buildPlan(warc, items, primaryPagesOnly)
}

// LoadS3Plan synchronizes the committed RustFS catalog into the local base directory, then reads it.
func LoadS3Plan(
	ctx context.Context,
	config catalog.S3Config,
	baseDirectory, crawlID, selection string,
	warcIndex uint32,
	primaryPagesOnly bool,
) (Plan, error) {
	warc, items, err := catalog.LoadS3WARC(
		ctx,
		config,
		baseDirectory,
		crawlID,
		selection,
		warcIndex,
	)
	if err != nil {
		return Plan{}, errors.Wrapf(err, "load RustFS WARC catalog index %d", warcIndex)
	}
	return buildPlan(warc, items, primaryPagesOnly)
}

func buildPlan(
	warc catalog.Warc,
	items []model.WorklistItem,
	primaryPagesOnly bool,
) (Plan, error) {
	selectedItems := make([]model.WorklistItem, 0, len(items))
	var selectedBytes int64
	for _, item := range items {
		if primaryPagesOnly && !item.Primary {
			continue
		}
		if item.Length > math.MaxInt64-selectedBytes {
			return Plan{}, errors.Newf("selected byte total overflows for WARC index %d", warc.WarcIndex)
		}
		selectedItems = append(selectedItems, item)
		selectedBytes += item.Length
	}

	return Plan{
		Items:         selectedItems,
		WARCIndex:     warc.WarcIndex,
		WARCFilename:  warc.WarcFilename,
		SelectedBytes: selectedBytes,
	}, nil
}

// Open validates the selected ranges against the current object size, then either keeps the
// network range getter or downloads the WARC once and serves its selected records from disk.
// It computes the whole-vs-range mode from the threshold and delegates to OpenAs.
func (plan Plan) Open(
	ctx context.Context,
	objects fetch.ObjectGetter,
	bucket string,
	wholeWARCThresholdPercent float64,
	tempDirectory string,
) (*Input, error) {
	if math.IsNaN(wholeWARCThresholdPercent) || math.IsInf(wholeWARCThresholdPercent, 0) ||
		wholeWARCThresholdPercent < 0 || wholeWARCThresholdPercent > 100 {
		return nil, errors.Newf("whole-WARC threshold must be between 0 and 100 percent, got %v", wholeWARCThresholdPercent)
	}
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if plan.Empty() {
		return plan.newInput(ModeEmpty, 0, emptyGetter{}), nil
	}
	objectBytes, err := plan.resolveObjectBytes(ctx, objects, bucket)
	if err != nil {
		return nil, err
	}

	mode := ModeRange
	coveragePercent := float64(plan.SelectedBytes) * 100 / float64(objectBytes)
	if coveragePercent >= wholeWARCThresholdPercent {
		mode = ModeWholeFile
	}
	return plan.serveMode(ctx, objects, bucket, mode, objectBytes, tempDirectory)
}

// OpenAs opens the plan with an EXPLICIT mode (ModeRange or ModeWholeFile), skipping the
// threshold decision — range runners force the lane's strategy. Empty plans still return
// ModeEmpty. Open computes the mode from the threshold then delegates here.
func (plan Plan) OpenAs(
	ctx context.Context,
	objects fetch.ObjectGetter,
	bucket string,
	mode Mode,
	tempDirectory string,
) (*Input, error) {
	if err := plan.validate(); err != nil {
		return nil, err
	}
	if plan.Empty() {
		return plan.newInput(ModeEmpty, 0, emptyGetter{}), nil
	}
	if mode != ModeRange && mode != ModeWholeFile {
		return nil, errors.Newf("open mode must be %q or %q, got %q", ModeRange, ModeWholeFile, mode)
	}
	objectBytes, err := plan.resolveObjectBytes(ctx, objects, bucket)
	if err != nil {
		return nil, err
	}
	return plan.serveMode(ctx, objects, bucket, mode, objectBytes, tempDirectory)
}

// resolveObjectBytes reads and validates the current object size for a non-empty plan. It performs
// exactly one HEAD (ObjectSize) call, so both Open and OpenAs charge a single size request.
func (plan Plan) resolveObjectBytes(ctx context.Context, objects fetch.ObjectGetter, bucket string) (int64, error) {
	if objects == nil {
		return 0, errors.New("object getter is required for a non-empty WARC")
	}
	if bucket == "" {
		return 0, errors.New("object bucket is required for a non-empty WARC")
	}
	objectBytes, err := objects.ObjectSize(ctx, bucket, plan.WARCFilename)
	if err != nil {
		return 0, errors.Wrapf(err, "read WARC object size %s", plan.WARCFilename)
	}
	if objectBytes <= 0 {
		return 0, errors.Newf("WARC object %s has invalid size %d", plan.WARCFilename, objectBytes)
	}
	if err := plan.validateObjectRanges(objectBytes); err != nil {
		return 0, err
	}
	return objectBytes, nil
}

// serveMode builds the Input for a validated non-empty plan whose object size is already known:
// ModeRange keeps the network getter, ModeWholeFile downloads the object once.
func (plan Plan) serveMode(
	ctx context.Context,
	objects fetch.ObjectGetter,
	bucket string,
	mode Mode,
	objectBytes int64,
	tempDirectory string,
) (*Input, error) {
	if mode == ModeRange {
		return plan.newInput(ModeRange, objectBytes, objects), nil
	}
	return plan.downloadWholeWARC(ctx, objects, bucket, objectBytes, tempDirectory)
}

func (plan Plan) validate() error {
	if plan.WARCFilename == "" {
		return errors.New("WARC filename is required")
	}
	var selectedBytes int64
	for pageIndex, item := range plan.Items {
		if item.WarcIndex != plan.WARCIndex || item.WarcFilename != plan.WARCFilename {
			return errors.Newf("selected page %d identifies a different WARC", pageIndex)
		}
		if item.Offset < 0 || item.Length <= 0 || item.Length-1 > math.MaxInt64-item.Offset {
			return errors.Newf(
				"selected page %d has invalid WARC range offset=%d length=%d",
				pageIndex, item.Offset, item.Length,
			)
		}
		if item.Length > math.MaxInt64-selectedBytes {
			return errors.New("selected byte total overflows")
		}
		selectedBytes += item.Length
	}
	if selectedBytes != plan.SelectedBytes {
		return errors.Newf("selected byte total is %d, plan records %d", selectedBytes, plan.SelectedBytes)
	}
	return nil
}

func (plan Plan) validateObjectRanges(objectBytes int64) error {
	if plan.SelectedBytes > objectBytes {
		return errors.Newf(
			"selected bytes %d exceed WARC object size %d for %s",
			plan.SelectedBytes, objectBytes, plan.WARCFilename,
		)
	}
	for pageIndex, item := range plan.Items {
		if item.Offset >= objectBytes || item.Length > objectBytes-item.Offset {
			return errors.Newf(
				"selected page %d range offset=%d length=%d exceeds WARC object size %d",
				pageIndex, item.Offset, item.Length, objectBytes,
			)
		}
	}
	return nil
}

func (plan Plan) downloadWholeWARC(
	ctx context.Context,
	objects fetch.ObjectGetter,
	bucket string,
	objectBytes int64,
	tempDirectory string,
) (*Input, error) {
	if tempDirectory != "" {
		if err := os.MkdirAll(tempDirectory, 0o755); err != nil {
			return nil, errors.Wrap(err, "create whole-WARC temporary directory")
		}
	}
	tempFile, err := os.CreateTemp(tempDirectory, "cc-enrich-*.warc.gz")
	if err != nil {
		return nil, errors.Wrap(err, "create whole-WARC temporary file")
	}
	tempPath := tempFile.Name()
	cleanup := func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}
	if err := objects.DownloadObject(ctx, bucket, plan.WARCFilename, tempFile); err != nil {
		cleanup()
		return nil, errors.Wrapf(err, "download whole WARC object %s", plan.WARCFilename)
	}
	fileInfo, err := tempFile.Stat()
	if err != nil {
		cleanup()
		return nil, errors.Wrap(err, "stat downloaded WARC object")
	}
	if fileInfo.Size() != objectBytes {
		cleanup()
		return nil, errors.Newf(
			"downloaded WARC object %s has size %d, HEAD reported %d",
			plan.WARCFilename, fileInfo.Size(), objectBytes,
		)
	}

	allowedRanges := make(map[objectRange]struct{}, len(plan.Items))
	for _, item := range plan.Items {
		allowedRanges[objectRange{start: item.Offset, end: item.Offset + item.Length - 1}] = struct{}{}
	}
	input := plan.newInput(ModeWholeFile, objectBytes, &localGetter{
		file:          tempFile,
		bucket:        bucket,
		key:           plan.WARCFilename,
		objectBytes:   objectBytes,
		allowedRanges: allowedRanges,
	})
	input.localFile = tempFile
	input.localPath = tempPath
	return input, nil
}

func (plan Plan) newInput(mode Mode, objectBytes int64, getter fetch.RangeGetter) *Input {
	return &Input{
		Items:         append([]model.WorklistItem(nil), plan.Items...),
		Getter:        getter,
		Mode:          mode,
		WARCIndex:     plan.WARCIndex,
		WARCFilename:  plan.WARCFilename,
		SelectedBytes: plan.SelectedBytes,
		ObjectBytes:   objectBytes,
	}
}

func (input *Input) Close() error {
	input.closeOnce.Do(func() {
		if input.localFile != nil {
			if err := input.localFile.Close(); err != nil {
				input.closeErr = errors.Wrap(err, "close downloaded WARC object")
			}
		}
		if input.localPath != "" {
			if err := os.Remove(input.localPath); err != nil && !os.IsNotExist(err) && input.closeErr == nil {
				input.closeErr = errors.Wrap(err, "remove downloaded WARC object")
			}
		}
	})
	return input.closeErr
}

type objectRange struct {
	start int64
	end   int64
}

type localGetter struct {
	file          *os.File
	bucket        string
	key           string
	objectBytes   int64
	allowedRanges map[objectRange]struct{}
}

func (getter *localGetter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bucket != getter.bucket || key != getter.key {
		return nil, errors.Newf("downloaded WARC is %s/%s, requested %s/%s", getter.bucket, getter.key, bucket, key)
	}
	if start < 0 || end < start || end >= getter.objectBytes {
		return nil, errors.Newf("invalid downloaded WARC range %d-%d for object size %d", start, end, getter.objectBytes)
	}
	if _, exists := getter.allowedRanges[objectRange{start: start, end: end}]; !exists {
		return nil, errors.Newf("range %d-%d is not a selected record in downloaded WARC %s", start, end, key)
	}
	length := end - start + 1
	if length > int64(int(^uint(0)>>1)) {
		return nil, errors.Newf("downloaded WARC range length %d exceeds platform capacity", length)
	}
	body := make([]byte, int(length))
	read, err := getter.file.ReadAt(body, start)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.Wrapf(err, "read downloaded WARC range %d-%d", start, end)
	}
	if read != len(body) {
		return nil, errors.Newf("short downloaded WARC range: read %d bytes, want %d", read, len(body))
	}
	return body, nil
}

type emptyGetter struct{}

func (emptyGetter) GetRange(context.Context, string, string, int64, int64) ([]byte, error) {
	return nil, errors.New("empty WARC input has no record ranges")
}
