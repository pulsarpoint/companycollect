package rangeplanner

import "testing"

func TestPlanWholeWARCHybridUsesSelectedByteCoverage(t *testing.T) {
	records := []Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 600},
		{ID: 1, WARCFile: "b.warc.gz", Offset: 0, Length: 200},
		{ID: 2, WARCFile: "b.warc.gz", Offset: 300, Length: 200},
	}
	plan, err := PlanWholeWARCHybrid(records, map[string]int64{
		"a.warc.gz": 1_000,
		"b.warc.gz": 1_000,
	}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Objects) != 2 || !plan.Objects[0].DownloadWholeObject || plan.Objects[1].DownloadWholeObject {
		t.Fatalf("unexpected decisions %+v", plan.Objects)
	}
	estimate := plan.Estimate("part_block")
	if estimate.SourceRequests != 3 || estimate.SourceBytes != 1_400 || estimate.SelectedBytes != 1_000 {
		t.Fatalf("unexpected estimate %+v", estimate)
	}
	if estimate.WholeWARCObjects != 1 || estimate.ExactWARCObjects != 1 || estimate.ExactRecordRequests != 2 {
		t.Fatalf("unexpected hybrid strategies %+v", estimate)
	}
}

func TestPlanWholeWARCHybridRejectsMissingObjectSize(t *testing.T) {
	_, err := PlanWholeWARCHybrid([]Record{{ID: 0, WARCFile: "missing.warc.gz", Offset: 0, Length: 100}}, nil, 50)
	if err == nil {
		t.Fatal("missing object size unexpectedly succeeded")
	}
}
