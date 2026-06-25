package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEmbedBatchesAndNormalizes(t *testing.T) {
	var reqCount int64
	var sawPrefix atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&reqCount, 1)
		var body struct {
			Input []string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		for _, in := range body.Input {
			if strings.HasPrefix(in, "Instruct: Classify\nQuery: ") {
				sawPrefix.Store(true)
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
	c := NewEmbedClient(srv.URL, "m", 2, 1) // batch 2, sequential
	vecs, err := c.Embed([]string{"a", "b", "c"}, "Classify")
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&reqCount) != 2 {
		t.Fatalf("want 2 batched requests, got %d", reqCount)
	}
	if len(vecs) != 3 {
		t.Fatalf("want 3 vectors, got %d", len(vecs))
	}
	if !sawPrefix.Load() {
		t.Fatal("query instruction prefix not applied")
	}
	if d := dot(vecs[0], vecs[0]); d < 0.99 || d > 1.01 {
		t.Fatalf("not normalized: %v", d)
	}
}

// TestEmbedConcurrentPreservesOrder fans 4 single-text requests across 4 concurrent
// workers; the server returns a one-hot vector at the index encoded in each input,
// so vecs[i] must be one-hot at i regardless of completion order.
func TestEmbedConcurrentPreservesOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		out := struct {
			Data []struct {
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}{}
		for _, in := range body.Input {
			idx := int(in[len(in)-1] - '0')
			v := make([]float32, 4)
			v[idx] = 1
			out.Data = append(out.Data, struct {
				Embedding []float32 `json:"embedding"`
			}{v})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := NewEmbedClient(srv.URL, "m", 1, 4) // batch 1, 4 in flight
	vecs, err := c.Embed([]string{"q0", "q1", "q2", "q3"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(vecs) != 4 {
		t.Fatalf("want 4 vectors, got %d", len(vecs))
	}
	for i := 0; i < 4; i++ {
		if vecs[i][i] < 0.99 {
			t.Fatalf("order broken at %d: %v", i, vecs[i])
		}
	}
}
