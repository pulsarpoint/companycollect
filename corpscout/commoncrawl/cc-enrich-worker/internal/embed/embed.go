package embed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"cc-enrich-worker/internal/vec"
)

const (
	MaxChars     = 2000 // default chars/text cap; override with COMMONCRAWL_EMBED_MAX_CHARS
	DefaultBatch = 16   // texts/request; small so requests co-batch under the engine token budget
	maxRetries   = 4    // embed request retries on transient errors (EOF/conn/5xx)
)

type EmbedClient struct {
	url, model  string
	batch       int
	concurrency int
	maxChars    int
	http        *http.Client
}

// NewEmbedClient builds a client that POSTs to <baseURL>/embeddings. batch is the
// number of texts per request; concurrency is the number of requests kept in flight
// (the embedding GPU needs many concurrent requests to saturate — a single stream
// leaves it idle). concurrency<=1 is sequential. maxChars<=0 => MaxChars default.
func NewEmbedClient(baseURL, model string, batch, concurrency, maxChars int) *EmbedClient {
	if batch <= 0 {
		batch = DefaultBatch
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if maxChars <= 0 {
		maxChars = MaxChars
	}
	return &EmbedClient{
		url: baseURL + "/embeddings", model: model, batch: batch, concurrency: concurrency, maxChars: maxChars,
		http: &http.Client{Timeout: 120 * time.Second, Transport: &http.Transport{
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
			if len(t) > c.maxChars {
				t = t[:c.maxChars]
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
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 750 * time.Millisecond) // backoff on a flaky/overloaded server
		}
		out, err := c.tryEmbed(body, len(chunk))
		if err == nil {
			return out, nil
		}
		lastErr = err // EOF / connection reset / 5xx (e.g. vLLM restarting after OOM) -> retry
	}
	return nil, fmt.Errorf("embed after %d attempts: %w", maxRetries, lastErr)
}

func (c *EmbedClient) tryEmbed(body []byte, n int) ([][]float32, error) {
	resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("embed: server status %d", resp.StatusCode)
	}
	var parsed struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	if len(parsed.Data) != n {
		return nil, fmt.Errorf("embed: got %d vectors for %d inputs", len(parsed.Data), n)
	}
	out := make([][]float32, len(parsed.Data))
	for i, d := range parsed.Data {
		out[i] = vec.Norm(d.Embedding)
	}
	return out, nil
}
