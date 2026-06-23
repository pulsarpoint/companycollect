package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type EmbedClient struct {
	url, model string
	batch      int
	http       *http.Client
}

func NewEmbedClient(baseURL, model string, batch int) *EmbedClient {
	if batch <= 0 {
		batch = embedBatch
	}
	return &EmbedClient{url: baseURL + "/embeddings", model: model, batch: batch, http: &http.Client{}}
}

// Embed returns L2-normalized vectors for texts, chunked by batch. When instruction
// is non-empty each text is wrapped as `Instruct: <instr>\nQuery: <text>` (the Qwen3
// asymmetric query convention). Texts are truncated to embedMaxChars first.
func (c *EmbedClient) Embed(texts []string, instruction string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for i := 0; i < len(texts); i += c.batch {
		end := i + c.batch
		if end > len(texts) {
			end = len(texts)
		}
		chunk := make([]string, 0, end-i)
		for _, t := range texts[i:end] {
			if len(t) > embedMaxChars {
				t = t[:embedMaxChars]
			}
			if instruction != "" {
				t = "Instruct: " + instruction + "\nQuery: " + t
			}
			chunk = append(chunk, t)
		}
		body, _ := json.Marshal(map[string]any{"model": c.model, "input": chunk})
		resp, err := c.http.Post(c.url, "application/json", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		var parsed struct {
			Data []struct {
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if len(parsed.Data) != len(chunk) {
			return nil, fmt.Errorf("embed: got %d vectors for %d inputs", len(parsed.Data), len(chunk))
		}
		for _, d := range parsed.Data {
			out = append(out, norm(d.Embedding))
		}
	}
	return out, nil
}
