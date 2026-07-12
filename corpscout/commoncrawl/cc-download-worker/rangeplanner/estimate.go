package rangeplanner

type Estimate struct {
	Algorithm                 string
	Scope                     string
	WARCObjects               int
	SelectedRecords           int64
	SelectedBytes             int64
	SourceRequests            int64
	SourceBytes               int64
	MultiRecordRequests       int64
	MaxRecordsPerRequest      int
	WholeWARCObjects          int
	ExactWARCObjects          int
	ExactRecordRequests       int64
	WholeWARCThresholdPercent float64
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
