package classify

import "testing"

type fakeEmbedder struct{ vec []float32 }

func (f fakeEmbedder) Embed(texts []string, instr string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = f.vec
	}
	return out, nil
}

func TestVerifyReference(t *testing.T) {
	emb8 := fakeEmbedder{vec: make([]float32, 8)} // endpoint produces dim-8 vectors
	good := ReferenceMeta{Model: "qwen3-8b", Dim: 8, Count: 1024, Expected: 1024}

	if err := VerifyReference(good, "qwen3-8b", emb8); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	// incomplete coverage
	if err := VerifyReference(ReferenceMeta{Model: "qwen3-8b", Dim: 8, Count: 1000, Expected: 1024}, "qwen3-8b", emb8); err == nil {
		t.Fatal("expected coverage mismatch error")
	}
	// model mismatch vs configured
	if err := VerifyReference(good, "qwen3-4b", emb8); err == nil {
		t.Fatal("expected model mismatch error")
	}
	// dim mismatch: reference says 4, endpoint produces 8
	if err := VerifyReference(ReferenceMeta{Model: "qwen3-8b", Dim: 4, Count: 1024, Expected: 1024}, "qwen3-8b", emb8); err == nil {
		t.Fatal("expected dim mismatch error")
	}
	// empty configured model -> skip the name check, still verify coverage + dim
	if err := VerifyReference(good, "", emb8); err != nil {
		t.Fatalf("expected pass with empty model, got %v", err)
	}
}
