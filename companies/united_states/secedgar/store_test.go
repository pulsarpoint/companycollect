package secedgar

import (
	"context"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestStoreCountsRecords(t *testing.T) {
	source := NewSource(Config{})

	result, err := source.Store(context.Background(), []CompanyTickerRecord{
		{CIK10: "0000320193"},
		{CIK10: "0000789019"},
	})
	if err != nil {
		t.Fatalf("Store returned error: %v", err)
	}
	if result.RecordsReceived != 2 || result.RecordsStored != 2 {
		t.Fatalf("Store result = %#v, want received/stored 2", result)
	}
}

func TestStoreHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	source := NewSource(Config{})
	_, err := source.Store(ctx, []CompanyTickerRecord{{CIK10: "0000320193"}})
	if err == nil {
		t.Fatal("Store returned nil error, want context error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindTimeout) {
		t.Fatalf("Store error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindTimeout, err)
	}
}
