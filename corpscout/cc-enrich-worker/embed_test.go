package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbedBatchesAndNormalizes(t *testing.T) {
	var reqCount int
	var sawPrefix bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		var body struct {
			Input []string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		for _, in := range body.Input {
			if strings.HasPrefix(in, "Instruct: Classify\nQuery: ") {
				sawPrefix = true
			}
		}
		out := struct {
			Data []struct {
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}{}
		for range body.Input {
			out.Data = append(out.Data, struct {
				Embedding []float32 `json:"embedding"`
			}{[]float32{3, 4}})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := NewEmbedClient(srv.URL, "m", 2) // batch size 2
	vecs, err := c.Embed([]string{"a", "b", "c"}, "Classify")
	if err != nil {
		t.Fatal(err)
	}
	if reqCount != 2 {
		t.Fatalf("want 2 batched requests, got %d", reqCount)
	}
	if len(vecs) != 3 {
		t.Fatalf("want 3 vectors, got %d", len(vecs))
	}
	if !sawPrefix {
		t.Fatal("query instruction prefix not applied")
	}
	if d := dot(vecs[0], vecs[0]); d < 0.99 || d > 1.01 {
		t.Fatalf("not normalized: %v", d)
	}
}
