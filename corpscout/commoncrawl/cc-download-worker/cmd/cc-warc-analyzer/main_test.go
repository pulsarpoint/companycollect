package main

import "testing"

func TestParseThresholds(t *testing.T) {
	thresholds, err := parseThresholds("10, 25,75")
	if err != nil {
		t.Fatal(err)
	}
	if len(thresholds) != 3 || thresholds[0] != 10 || thresholds[1] != 25 || thresholds[2] != 75 {
		t.Fatalf("unexpected thresholds %v", thresholds)
	}
	for _, value := range []string{"", "-1", "101", "ten", "NaN", "+Inf"} {
		if _, err := parseThresholds(value); err == nil {
			t.Fatalf("thresholds %q unexpectedly succeeded", value)
		}
	}
}
