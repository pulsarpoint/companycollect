package rangeplanner

import "testing"

func TestPlanWholeWARCHybridUsesSelectedByteCoverage(t *testing.T) {
	records := []Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 600},
		{ID: 1, WARCFile: "b.warc.gz", Offset: 0, Length: 200},
		{ID: 2, WARCFile: "b.warc.gz", Offset: 300, Length: 200},
	}
	selection := NewSelection()
	if err := selection.Add(records); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanWholeWARCHybrid(selection, map[string]int64{
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
	selection := NewSelection()
	if err := selection.Add([]Record{{ID: 0, WARCFile: "missing.warc.gz", Offset: 0, Length: 100}}); err != nil {
		t.Fatal(err)
	}
	_, err := PlanWholeWARCHybrid(selection, nil, 50)
	if err == nil {
		t.Fatal("missing object size unexpectedly succeeded")
	}
}

func TestSelectionAccumulatesRecordsWithoutRetainingThem(t *testing.T) {
	selection := NewSelection()
	if err := selection.Add([]Record{{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 100}}); err != nil {
		t.Fatal(err)
	}
	if err := selection.Add([]Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 100, Length: 200},
		{ID: 1, WARCFile: "b.warc.gz", Offset: 0, Length: 300},
	}); err != nil {
		t.Fatal(err)
	}
	estimate := selection.ExactEstimate()
	if estimate.WARCObjects != 2 || estimate.SelectedRecords != 3 || estimate.SelectedBytes != 600 {
		t.Fatalf("unexpected selection estimate %+v", estimate)
	}
}
