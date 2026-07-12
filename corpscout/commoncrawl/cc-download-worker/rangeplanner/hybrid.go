package rangeplanner

import (
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
)

type ObjectDecision struct {
	WARCFile               string
	DownloadWholeObject    bool
	ObjectBytes            int64
	SelectedRecords        int64
	SelectedBytes          int64
	SelectedBytePercentage float64
}

type objectSelection struct {
	records int64
	bytes   int64
}

type Selection struct {
	objects map[string]objectSelection
	records int64
	bytes   int64
}

type HybridPlan struct {
	WholeObjectThresholdPercent float64
	Objects                     []ObjectDecision
}

func NewSelection() *Selection {
	return &Selection{objects: make(map[string]objectSelection)}
}

func (selection *Selection) Add(records []Record) error {
	if selection == nil || selection.objects == nil {
		return errors.New("selection is not initialized")
	}
	for _, record := range records {
		if err := validateRecord(record); err != nil {
			return err
		}
		object := selection.objects[record.WARCFile]
		object.records++
		object.bytes += record.Length
		selection.objects[record.WARCFile] = object
		selection.records++
		selection.bytes += record.Length
	}
	return nil
}

func (selection *Selection) WARCFiles() []string {
	filenames := make([]string, 0, len(selection.objects))
	for filename := range selection.objects {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)
	return filenames
}

func (selection *Selection) ExactEstimate() Estimate {
	return Estimate{
		Algorithm:            "exact_records",
		Scope:                "record",
		WARCObjects:          len(selection.objects),
		SelectedRecords:      selection.records,
		SelectedBytes:        selection.bytes,
		SourceRequests:       selection.records,
		SourceBytes:          selection.bytes,
		MaxRecordsPerRequest: 1,
	}
}

func PlanWholeWARCHybrid(selection *Selection, objectSizes map[string]int64, thresholdPercent float64) (HybridPlan, error) {
	if math.IsNaN(thresholdPercent) || math.IsInf(thresholdPercent, 0) || thresholdPercent < 0 || thresholdPercent > 100 {
		return HybridPlan{}, errors.Newf("whole-WARC threshold must be between 0 and 100, got %f", thresholdPercent)
	}
	if selection == nil || selection.objects == nil {
		return HybridPlan{}, errors.New("selection is not initialized")
	}

	filenames := selection.WARCFiles()
	plan := HybridPlan{WholeObjectThresholdPercent: thresholdPercent, Objects: make([]ObjectDecision, 0, len(filenames))}
	for _, filename := range filenames {
		selected := selection.objects[filename]
		objectBytes := objectSizes[filename]
		if objectBytes <= 0 {
			return HybridPlan{}, errors.Newf("WARC object size is missing for %s", filename)
		}
		if selected.bytes > objectBytes {
			return HybridPlan{}, errors.Newf(
				"selected WARC bytes exceed object size for %s: selected=%d object=%d",
				filename,
				selected.bytes,
				objectBytes,
			)
		}
		selectedPercentage := 100 * float64(selected.bytes) / float64(objectBytes)
		plan.Objects = append(plan.Objects, ObjectDecision{
			WARCFile:               filename,
			DownloadWholeObject:    selectedPercentage >= thresholdPercent,
			ObjectBytes:            objectBytes,
			SelectedRecords:        selected.records,
			SelectedBytes:          selected.bytes,
			SelectedBytePercentage: selectedPercentage,
		})
	}
	return plan, nil
}

func (plan HybridPlan) Estimate(scope string) Estimate {
	estimate := Estimate{
		Algorithm:                 fmt.Sprintf("hybrid_whole_warc_%gpct", plan.WholeObjectThresholdPercent),
		Scope:                     scope,
		WARCObjects:               len(plan.Objects),
		WholeWARCThresholdPercent: plan.WholeObjectThresholdPercent,
	}
	for _, object := range plan.Objects {
		estimate.SelectedRecords += object.SelectedRecords
		estimate.SelectedBytes += object.SelectedBytes
		if object.DownloadWholeObject {
			estimate.SourceRequests++
			estimate.SourceBytes += object.ObjectBytes
			estimate.WholeWARCObjects++
			estimate.MaxRecordsPerRequest = max(estimate.MaxRecordsPerRequest, int(object.SelectedRecords))
			if object.SelectedRecords > 1 {
				estimate.MultiRecordRequests++
			}
			continue
		}
		estimate.SourceRequests += object.SelectedRecords
		estimate.SourceBytes += object.SelectedBytes
		estimate.ExactRecordRequests += object.SelectedRecords
		estimate.ExactWARCObjects++
		estimate.MaxRecordsPerRequest = max(estimate.MaxRecordsPerRequest, 1)
	}
	return estimate
}
