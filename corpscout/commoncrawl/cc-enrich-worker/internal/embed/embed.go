package embed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
)

const (
	embedMaxChars  = 2000 // COMMONCRAWL_EMBED_MAX_CHARS default
	MaxChars       = 2000 // exported max chars for text truncation (COMMONCRAWL_EMBED_MAX_CHARS)
	embedBatch     = 16   // texts/request; small so requests co-batch under the engine token budget
	DefaultBatch   = 16   // default batch size for embeddings
)

type EmbedClient struct {
	url, model  string
	batch       int
	concurrency int
	http        *http.Client
}

// NewEmbedClient builds a client that POSTs to <baseURL>/embeddings. batch is the
// number of texts per request; concurrency is the number of requests kept in flight
// (the embedding GPU needs many concurrent requests to saturate — a single stream
// leaves it idle). concurrency<=1 is sequential.
func NewEmbedClient(baseURL, model string, batch, concurrency int) *EmbedClient {
	if batch <= 0 {
		batch = embedBatch
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	return &EmbedClient{
		url: baseURL + "/embeddings", model: model, batch: batch, concurrency: concurrency,
		http: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			MaxConnsPerHost:     concurrency,
		}},
	}
}

type embedChunk struct {
	start int
	texts []string
}

// Embed returns L2-normalized vectors for texts, batched and fanned out across up to
// concurrency in-flight requests. Output order matches input order: each chunk writes
// its own non-overlapping range of the result slice.
func (c *EmbedClient) Embed(texts []string, instruction string) ([][]float32, error) {
	out := make([][]float32, len(texts))

	var chunks []embedChunk
	for i := 0; i < len(texts); i += c.batch {
		end := i + c.batch
		if end > len(texts) {
			end = len(texts)
		}
		ct := make([]string, 0, end-i)
		for _, t := range texts[i:end] {
			if len(t) > embedMaxChars {
				t = t[:embedMaxChars]
			}
			if instruction != "" {
				t = "Instruct: " + instruction + "\nQuery: " + t
			}
			ct = append(ct, t)
		}
		chunks = append(chunks, embedChunk{start: i, texts: ct})
	}

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, ch := range chunks {
		wg.Add(1)
		go func(ch embedChunk) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			vecs, err := c.embedOne(ch.texts)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			for j, v := range vecs { // distinct indices per chunk -> no shared write
				out[ch.start+j] = v
			}
		}(ch)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (c *EmbedClient) embedOne(chunk []string) ([][]float32, error) {
	body, _ := json.Marshal(map[string]any{"model": c.model, "input": chunk})
	resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var parsed struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if len(parsed.Data) != len(chunk) {
		return nil, fmt.Errorf("embed: got %d vectors for %d inputs", len(parsed.Data), len(chunk))
	}
	out := make([][]float32, len(parsed.Data))
	for i, d := range parsed.Data {
		out[i] = norm(d.Embedding)
	}
	return out, nil
}

func sqrt32(x float32) float32 { return float32(math.Sqrt(float64(x))) }

// Dot returns the dot product of vectors a and b.
func Dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// Norm L2-normalizes a vector.
func norm(v []float32) []float32 {
	s := float32(1.0) / (sqrt32(Dot(v, v)) + 1e-9)
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * s
	}
	return out
}
