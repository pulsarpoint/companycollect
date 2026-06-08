package irseobmf

import "testing"

func TestValidateHeaderAcceptsCanonicalHeaderWithBOM(t *testing.T) {
	header := append([]string{}, csvColumns...)
	header[0] = utf8BOM + header[0]
	if err := validateHeader(header); err != nil {
		t.Fatalf("validateHeader returned error: %v", err)
	}
}

func TestValidateHeaderRejectsWrongShape(t *testing.T) {
	if err := validateHeader([]string{"EIN", "NAME"}); err == nil {
		t.Fatal("validateHeader returned nil for short header")
	}
	bad := append([]string{}, csvColumns...)
	bad[5] = "PROVINCE"
	if err := validateHeader(bad); err == nil {
		t.Fatal("validateHeader returned nil for renamed column")
	}
}

func TestRowToRecordNormalizesEINAndTrims(t *testing.T) {
	row := []string{
		"10018605", " SAMPLE CHARITY ", "", "123 MAIN ST", "PORTLAND", "ME", "04101",
		"0000", "03", "3", "1000", "200401", "1", "15", "000000000", "1", "01", "",
		"0", "0", "02", "0", "12", "", "", "", "", "",
	}
	record, err := rowToRecord(row)
	if err != nil {
		t.Fatalf("rowToRecord returned error: %v", err)
	}
	if record.EIN != "010018605" {
		t.Fatalf("EIN = %q, want zero-padded 010018605", record.EIN)
	}
	if record.Name != "SAMPLE CHARITY" {
		t.Fatalf("Name = %q, want trimmed", record.Name)
	}
}

func TestRowToRecordRejectsWrongWidthAndMissingEIN(t *testing.T) {
	if _, err := rowToRecord([]string{"010011694", "NAME"}); err == nil {
		t.Fatal("rowToRecord returned nil for short row")
	}
	row := make([]string, len(csvColumns))
	if _, err := rowToRecord(row); err == nil {
		t.Fatal("rowToRecord returned nil for blank EIN")
	}
}

func TestNormalizeEIN(t *testing.T) {
	cases := map[string]string{
		"10018605":   "010018605",
		"010011694":  "010011694",
		"  123  ":    "000000123",
		"":           "",
		"12-3456789": "12-3456789",
	}
	for in, want := range cases {
		if got := NormalizeEIN(in); got != want {
			t.Fatalf("NormalizeEIN(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseAmount(t *testing.T) {
	if amount, present := parseAmount("65979"); !present || amount != 65979 {
		t.Fatalf("parseAmount(65979) = %d,%v want 65979,true", amount, present)
	}
	if amount, present := parseAmount(""); present || amount != 0 {
		t.Fatalf("parseAmount(empty) = %d,%v want 0,false", amount, present)
	}
	if amount, present := parseAmount("n/a"); present || amount != 0 {
		t.Fatalf("parseAmount(n/a) = %d,%v want 0,false", amount, present)
	}
}

func TestIsActiveExemptStatus(t *testing.T) {
	if !isActiveExemptStatus("01") {
		t.Fatal("status 01 should be active exempt")
	}
	if isActiveExemptStatus("25") {
		t.Fatal("status 25 should not be active exempt")
	}
}
