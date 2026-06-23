package main

import "testing"

func TestClassifyConfidentByMargin(t *testing.T) {
	ref := &Reference{Codes: []string{"62.01", "47.11"}, Labels: []string{"Prog", "Retail"},
		Divisions: []string{"62", "47"}, M: [][]float32{norm([]float32{1, 0, 0}), norm([]float32{0, 1, 0})}}
	protos := &Prototypes{}
	r := Classify(norm([]float32{1, 0, 0}), ref, protos)
	if r.NaceCode != "62.01" || !r.NaceConfident {
		t.Fatalf("want 62.01 confident, got %+v", r)
	}
}

func TestPageTypeDetected(t *testing.T) {
	ref := &Reference{Codes: []string{"62.01"}, Labels: []string{"x"}, Divisions: []string{"62"},
		M: [][]float32{norm([]float32{1, 0, 0, 0})}}
	protos := &Prototypes{Labels: []string{"parked"}, P: [][]float32{norm([]float32{0, 0, 1, 0})}}
	r := Classify(norm([]float32{0, 0, 1, 0}), ref, protos)
	if r.PageType != "parked" || r.NaceConfident {
		t.Fatalf("want parked, not confident; got %+v", r)
	}
}

func TestDivision(t *testing.T) {
	for code, want := range map[string]string{"62.01": "62", "47": "47", "A": "A", "": ""} {
		if got := division(code); got != want {
			t.Errorf("division(%q)=%q want %q", code, got, want)
		}
	}
}
