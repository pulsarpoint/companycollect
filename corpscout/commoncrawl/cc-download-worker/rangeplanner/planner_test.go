package rangeplanner

import "testing"

func TestPlanGroupsNearbyRecordsWithoutChangingTheirIDs(t *testing.T) {
	records := []Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 1_000, Length: 100},
		{ID: 1, WARCFile: "a.warc.gz", Offset: 0, Length: 100},
		{ID: 2, WARCFile: "a.warc.gz", Offset: 150, Length: 100},
		{ID: 3, WARCFile: "b.warc.gz", Offset: 0, Length: 100},
	}
	planned, err := Plan(records, Policy{
		Name:        "nearby",
		MaxGapBytes: 100, MaxRangeBytes: 500, MaxJunkPercent: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 3 {
		t.Fatalf("planned ranges=%d, want 3: %+v", len(planned), planned)
	}
	if planned[0].Start != 0 || planned[0].Length != 250 || planned[0].SelectedBytes != 200 {
		t.Fatalf("unexpected merged range %+v", planned[0])
	}
	if len(planned[0].RecordIDs) != 2 || planned[0].RecordIDs[0] != 1 || planned[0].RecordIDs[1] != 2 {
		t.Fatalf("unexpected record mapping %+v", planned[0].RecordIDs)
	}
}

func TestPlanHonorsJunkThreshold(t *testing.T) {
	records := []Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 100},
		{ID: 1, WARCFile: "a.warc.gz", Offset: 200, Length: 100},
	}
	strict, err := Plan(records, Policy{
		Name:        "strict",
		MaxGapBytes: 1_000, MaxRangeBytes: 1_000, MaxJunkPercent: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(strict) != 2 {
		t.Fatalf("strict policy produced %d ranges, want 2", len(strict))
	}
	permissive, err := Plan(records, Policy{
		Name:        "permissive",
		MaxGapBytes: 1_000, MaxRangeBytes: 1_000, MaxJunkPercent: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(permissive) != 1 || permissive[0].Length != 300 || permissive[0].SelectedBytes != 200 {
		t.Fatalf("unexpected permissive plan %+v", permissive)
	}
}

func TestPlanHonorsMaximumRange(t *testing.T) {
	records := []Record{
		{ID: 0, WARCFile: "a.warc.gz", Offset: 0, Length: 100},
		{ID: 1, WARCFile: "a.warc.gz", Offset: 150, Length: 100},
	}
	planned, err := Plan(records, Policy{
		Name:        "bounded",
		MaxGapBytes: 100, MaxRangeBytes: 200, MaxJunkPercent: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned ranges=%d, want 2", len(planned))
	}
}
