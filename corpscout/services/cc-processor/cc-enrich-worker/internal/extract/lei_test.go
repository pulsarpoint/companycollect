package extract

import (
	"testing"

	"cc-enrich-worker/internal/model"
)

func TestValidLEI(t *testing.T) {
	valid := []string{
		"HWUPKR0MPOU8FGXBT394", // Apple Inc.
		"7LTWFZYICNSX8D621K86", // Deutsche Bank AG
	}
	for _, l := range valid {
		if !validLEI(l) {
			t.Errorf("expected %s to be a valid LEI", l)
		}
	}
	invalid := []string{
		"HWUPKR0MPOU8FGXBT395", // Apple LEI with last digit changed
		"NOTALEIATALL00000000",
		"5493001KJTIIGC8Y1R1",  // 19 chars
		"hwupkr0mpou8fgxbt394", // lowercase (not how LEIs appear)
	}
	for _, l := range invalid {
		if validLEI(l) {
			t.Errorf("expected %s to be invalid", l)
		}
	}
}

func TestExtractLEIsRequiresTokenBoundaries(t *testing.T) {
	// An LEI embedded in a longer alphanumeric run is not an LEI mention: without boundaries the
	// misaligned 20-char window is tried (and can, ~1/97 of the time, pass the checksum on random
	// order refs / license keys / hex hashes), while the real LEI at offset 1 is never seen.
	body := []byte(`<html><body>
		ref REFAHWUPKR0MPOU8FGXBT394 and order 7LTWFZYICNSX8D621K86PLUS9
		real one: HWUPKR0MPOU8FGXBT394, done.
	</body></html>`)
	ids := ExtractLEIs(body)
	if len(ids) != 1 {
		t.Fatalf("ExtractLEIs = %+v, want exactly the one standalone LEI", ids)
	}
	if ids[0].Value != "HWUPKR0MPOU8FGXBT394" || !ids[0].Valid || ids[0].Source != "text" {
		t.Fatalf("ExtractLEIs = %+v, want valid text LEI HWUPKR0MPOU8FGXBT394", ids[0])
	}
}

func TestExtractLEIs(t *testing.T) {
	body := []byte(`<html><head>
		<script type="application/ld+json">{"@type":"Organization","name":"Apple","leiCode":"HWUPKR0MPOU8FGXBT394"}</script>
		</head><body><footer>Deutsche Bank AG · LEI 7LTWFZYICNSX8D621K86 · some hash ABCD1234ABCD1234ABCD</footer></body></html>`)
	ids := ExtractLEIs(body)
	got := map[string]model.Identifier{}
	for _, id := range ids {
		got[id.Value] = id
	}
	if a, ok := got["HWUPKR0MPOU8FGXBT394"]; !ok || a.Source != "jsonld" || !a.Valid {
		t.Fatalf("apple from jsonld missing/wrong: %+v", got)
	}
	if d, ok := got["7LTWFZYICNSX8D621K86"]; !ok || d.Source != "text" || !d.Valid {
		t.Fatalf("deutsche bank from text missing/wrong: %+v", got)
	}
	if _, ok := got["ABCD1234ABCD1234ABCD"]; ok {
		t.Fatal("a random 20-char token should not pass the checksum")
	}
}
