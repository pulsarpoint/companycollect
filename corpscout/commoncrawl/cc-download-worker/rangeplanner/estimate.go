package rangeplanner

type Estimate struct {
	Algorithm            string
	Scope                string
	Policy               Policy
	WARCObjects          int
	SelectedRecords      int64
	SelectedBytes        int64
	SourceRequests       int64
	SourceBytes          int64
	MultiRecordRequests  int64
	MaxRecordsPerRequest int
}

func ExactEstimate(records []Record) Estimate {
	var selectedBytes int64
	warcObjects := make(map[string]struct{})
	for _, record := range records {
		selectedBytes += record.Length
		warcObjects[record.WARCFile] = struct{}{}
	}
	return Estimate{
		Algorithm:            "exact_records",
		Scope:                "record",
		WARCObjects:          len(warcObjects),
		SelectedRecords:      int64(len(records)),
		SelectedBytes:        selectedBytes,
		SourceRequests:       int64(len(records)),
		SourceBytes:          selectedBytes,
		MaxRecordsPerRequest: 1,
	}
}

func EstimateGroups(scope string, groups [][]Record, policy Policy) (Estimate, error) {
	estimate := Estimate{Algorithm: policy.Name, Scope: scope, Policy: policy}
	warcObjects := make(map[string]struct{})
	for _, records := range groups {
		for _, record := range records {
			estimate.SelectedRecords++
			estimate.SelectedBytes += record.Length
			warcObjects[record.WARCFile] = struct{}{}
		}
		planned, err := Plan(records, policy)
		if err != nil {
			return Estimate{}, err
		}
		for _, sourceRange := range planned {
			estimate.SourceRequests++
			estimate.SourceBytes += sourceRange.Length
			if len(sourceRange.RecordIDs) > 1 {
				estimate.MultiRecordRequests++
			}
			estimate.MaxRecordsPerRequest = max(estimate.MaxRecordsPerRequest, len(sourceRange.RecordIDs))
		}
	}
	estimate.WARCObjects = len(warcObjects)
	return estimate, nil
}
