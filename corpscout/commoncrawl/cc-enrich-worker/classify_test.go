package main

import (
	"testing"

	"cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/vec"
)

func TestClassifyConfidentByMargin(t *testing.T) {
	ref := &model.Reference{Codes: []string{"62.01", "47.11"}, Labels: []string{"Prog", "Retail"},
		Divisions: []string{"62", "47"}, M: [][]float32{vec.Norm([]float32{1, 0, 0}), vec.Norm([]float32{0, 1, 0})}}
	protos := &model.Prototypes{}
	r := Classify(vec.Norm([]float32{1, 0, 0}), ref, protos)
	if r.NaceCode != "62.01" || !r.NaceConfident {
		t.Fatalf("want 62.01 confident, got %+v", r)
	}
}

func TestPageTypeDetected(t *testing.T) {
	ref := &model.Reference{Codes: []string{"62.01"}, Labels: []string{"x"}, Divisions: []string{"62"},
		M: [][]float32{vec.Norm([]float32{1, 0, 0, 0})}}
	protos := &model.Prototypes{Labels: []string{"parked"}, P: [][]float32{vec.Norm([]float32{0, 0, 1, 0})}}
	r := Classify(vec.Norm([]float32{0, 0, 1, 0}), ref, protos)
	if r.PageType != "parked" || r.NaceConfident {
		t.Fatalf("want parked, not confident; got %+v", r)
	}
}

func TestConfidenceScore(t *testing.T) {
	// dominant top score (clearly above the bulk) -> high confidence
	dominant := []float32{0.9, 0.3, 0.29, 0.28, 0.27, 0.26, 0.25, 0.24, 0.23, 0.22, 0.21}
	// mushy: top barely above a cluster -> low confidence
	mushy := []float32{0.41, 0.40, 0.39, 0.39, 0.38, 0.38, 0.37, 0.37, 0.36, 0.36, 0.35}
	hi := confidenceScore(dominant)
	lo := confidenceScore(mushy)
	if hi <= lo {
		t.Fatalf("dominant confidence (%.3f) should exceed mushy (%.3f)", hi, lo)
	}
	if hi < 0 || hi > 1 || lo < 0 || lo > 1 {
		t.Fatalf("confidence out of [0,1]: hi=%v lo=%v", hi, lo)
	}
	if confidenceScore(nil) != 0 {
		t.Fatal("empty -> 0")
	}
}

func TestDivision(t *testing.T) {
	for code, want := range map[string]string{"62.01": "62", "47": "47", "A": "A", "": ""} {
		if got := division(code); got != want {
			t.Errorf("division(%q)=%q want %q", code, got, want)
		}
	}
}
