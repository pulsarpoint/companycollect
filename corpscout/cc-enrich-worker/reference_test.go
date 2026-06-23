package main

import "testing"

func TestBuildReferenceNormalizesAndOrders(t *testing.T) {
	ref := BuildReference([]refRow{
		{Code: "62.01", Label: "Prog", Division: "62", Embedding: []float32{3, 0, 0}},
		{Code: "47.11", Label: "Retail", Division: "47", Embedding: []float32{0, 5, 0}},
	})
	if len(ref.M) != 2 || ref.Codes[0] != "62.01" || ref.Divisions[1] != "47" {
		t.Fatalf("co-ordering wrong: %+v", ref)
	}
	if d := dot(ref.M[0], ref.M[0]); d < 0.99 || d > 1.01 {
		t.Fatalf("row 0 not normalized: %v", d)
	}
}

func TestBuildPrototypesNormalizes(t *testing.T) {
	p := BuildPrototypes([]protoRow{{PageType: "parked", Embedding: []float32{0, 4, 0}}})
	if len(p.P) != 1 || p.Labels[0] != "parked" {
		t.Fatalf("labels wrong: %+v", p)
	}
	if d := dot(p.P[0], p.P[0]); d < 0.99 || d > 1.01 {
		t.Fatalf("not normalized: %v", d)
	}
}
