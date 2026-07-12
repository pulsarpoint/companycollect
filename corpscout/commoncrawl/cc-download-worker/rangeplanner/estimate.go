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
