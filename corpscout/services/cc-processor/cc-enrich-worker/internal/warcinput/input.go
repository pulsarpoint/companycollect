// Package warcinput prepares one WARC-oriented enrichment unit from the static catalog.
package warcinput

import (
	"context"
	"math"
	"path/filepath"

	"github.com/cockroachdb/errors"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/model"
)

type Mode string

const (
	ModeEmpty Mode = "empty"
	ModeRange Mode = "range"
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

// Open validates the selected ranges against the current object size and serves them as network
// range reads. An empty plan returns ModeEmpty and performs no WARC-object I/O; a non-empty plan
// charges exactly one HEAD (ObjectSize) call and keeps the shared range getter.
func (plan Plan) Open(ctx context.Context, objects fetch.ObjectGetter, bucket string) (*Input, error) {
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
	return plan.newInput(ModeRange, objectBytes, objects), nil
}

// resolveObjectBytes reads and validates the current object size for a non-empty plan. It performs
// exactly one HEAD (ObjectSize) call.
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

// Close releases per-input resources. Range and empty inputs hold no owned handles (the range
// getter is a shared transport, never closed per part), so Close is a no-op kept for API symmetry.
func (input *Input) Close() error {
	return nil
}

type emptyGetter struct{}

func (emptyGetter) GetRange(context.Context, string, string, int64, int64) ([]byte, error) {
	return nil, errors.New("empty WARC input has no record ranges")
}
