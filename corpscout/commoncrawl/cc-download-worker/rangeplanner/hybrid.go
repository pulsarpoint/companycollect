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

type HybridPlan struct {
	WholeObjectThresholdPercent float64
	Objects                     []ObjectDecision
}

func PlanWholeWARCHybrid(records []Record, objectSizes map[string]int64, thresholdPercent float64) (HybridPlan, error) {
	if math.IsNaN(thresholdPercent) || math.IsInf(thresholdPercent, 0) || thresholdPercent < 0 || thresholdPercent > 100 {
		return HybridPlan{}, errors.Newf("whole-WARC threshold must be between 0 and 100, got %f", thresholdPercent)
	}
	selectedRecords := make(map[string]int64)
	selectedBytes := make(map[string]int64)
	for _, record := range records {
		if err := validateRecord(record); err != nil {
			return HybridPlan{}, err
		}
		selectedRecords[record.WARCFile]++
		selectedBytes[record.WARCFile] += record.Length
	}
	filenames := make([]string, 0, len(selectedRecords))
	for filename := range selectedRecords {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)

	plan := HybridPlan{WholeObjectThresholdPercent: thresholdPercent, Objects: make([]ObjectDecision, 0, len(filenames))}
	for _, filename := range filenames {
		objectBytes := objectSizes[filename]
		if objectBytes <= 0 {
			return HybridPlan{}, errors.Newf("WARC object size is missing for %s", filename)
		}
		if selectedBytes[filename] > objectBytes {
			return HybridPlan{}, errors.Newf(
				"selected WARC bytes exceed object size for %s: selected=%d object=%d",
				filename,
				selectedBytes[filename],
				objectBytes,
			)
		}
		selectedPercentage := 100 * float64(selectedBytes[filename]) / float64(objectBytes)
		plan.Objects = append(plan.Objects, ObjectDecision{
			WARCFile:               filename,
			DownloadWholeObject:    selectedPercentage >= thresholdPercent,
			ObjectBytes:            objectBytes,
			SelectedRecords:        selectedRecords[filename],
			SelectedBytes:          selectedBytes[filename],
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
