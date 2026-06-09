package secedgar

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestDecodeCompanyTickersParsesObjectKeyedJSONDeterministically(t *testing.T) {
	data, err := os.ReadFile("testdata/company_tickers_sample.json")
	if err != nil {
		t.Fatal(err)
	}

	records, err := DecodeCompanyTickers(data)
	if err != nil {
		t.Fatalf("DecodeCompanyTickers error: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(records))
	}

	wantIndexes := []int{0, 2, 10}
	wantCIK10 := []string{"0000320193", "0000789019", "0000001750"}
	wantTickers := []string{"AAPL", "MSFT", "AIR"}
	for i, record := range records {
		if record.SourceKey != SourceKey {
			t.Fatalf("records[%d].SourceKey = %q, want %q", i, record.SourceKey, SourceKey)
		}
		if record.SourceIndex != wantIndexes[i] {
			t.Fatalf("records[%d].SourceIndex = %d, want %d", i, record.SourceIndex, wantIndexes[i])
		}
		if record.CIK10 != wantCIK10[i] {
			t.Fatalf("records[%d].CIK10 = %q, want %q", i, record.CIK10, wantCIK10[i])
		}
		if record.Ticker != wantTickers[i] {
			t.Fatalf("records[%d].Ticker = %q, want %q", i, record.Ticker, wantTickers[i])
		}
		if len(record.RawPayload) == 0 {
			t.Fatalf("records[%d].RawPayload is empty", i)
		}
		sum := sha256.Sum256(record.RawPayload)
		if want := hex.EncodeToString(sum[:]); record.PayloadHash != want {
			t.Fatalf("records[%d].PayloadHash = %q, want %q", i, record.PayloadHash, want)
		}
	}
}

func TestDecodeCompanyTickersRejectsBadShape(t *testing.T) {
	badShape, err := os.ReadFile("testdata/company_tickers_bad_shape.json")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "array", data: badShape},
		{name: "record array", data: []byte(`{"0":[{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}]}`)},
		{name: "leading zero key", data: []byte(`{"1":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},"01":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}}`)},
		{name: "null record", data: []byte(`{"0":null}`)},
		{name: "empty record", data: []byte(`{"0":{}}`)},
		{name: "missing cik", data: []byte(`{"0":{"ticker":"AAPL","title":"Apple Inc."}}`)},
		{name: "zero cik", data: []byte(`{"0":{"cik_str":0,"ticker":"AAPL","title":"Apple Inc."}}`)},
		{name: "empty ticker", data: []byte(`{"0":{"cik_str":320193,"ticker":"","title":"Apple Inc."}}`)},
		{name: "empty title", data: []byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":""}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeCompanyTickers(tt.data); err == nil {
				t.Fatal("DecodeCompanyTickers error = nil, want error")
			}
		})
	}
}
