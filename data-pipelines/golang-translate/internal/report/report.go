package report

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"
)

type Report struct {
	RunID               string    `json:"run_id"`
	Status              string    `json:"status"`
	Scenario            string    `json:"scenario"`
	Description         string    `json:"description,omitempty"`
	Strategy            string    `json:"strategy"`
	BaseURL             string    `json:"base_url"`
	Model               string    `json:"model"`
	InputPath           string    `json:"input_path"`
	SourceLang          string    `json:"source_lang"`
	TargetLang          string    `json:"target_lang"`
	ItemsPlanned        int       `json:"items_planned"`
	BatchSize           int       `json:"batch_size"`
	Parallel            int       `json:"parallel"`
	RequestsPlanned     int       `json:"requests_planned"`
	RequestsSucceeded   int       `json:"requests_succeeded"`
	RequestsFailed      int       `json:"requests_failed"`
	TermsSent           int       `json:"terms_sent"`
	TermsSucceeded      int       `json:"terms_succeeded"`
	TermsMissing        int       `json:"terms_missing"`
	PromptTokens        int       `json:"prompt_tokens,omitempty"`
	OutputTokens        int       `json:"output_tokens,omitempty"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	ElapsedMS           int64     `json:"elapsed_ms"`
	RequestsPerSecond   float64   `json:"requests_per_second"`
	TermsPerSecond      float64   `json:"terms_per_second"`
	MinRequestLatencyMS int64     `json:"min_request_latency_ms"`
	P50RequestLatencyMS int64     `json:"p50_request_latency_ms"`
	P95RequestLatencyMS int64     `json:"p95_request_latency_ms"`
	MaxRequestLatencyMS int64     `json:"max_request_latency_ms"`
	FailureReason       string    `json:"failure_reason,omitempty"`
}

type Responses struct {
	RunID     string           `json:"run_id"`
	Scenario  string           `json:"scenario"`
	Responses []ResponseDetail `json:"responses"`
}

type ResponseDetail struct {
	BatchID      string            `json:"batch_id"`
	Status       string            `json:"status"`
	ItemCount    int               `json:"item_count"`
	LatencyMS    int64             `json:"latency_ms"`
	MissingCount int               `json:"missing_count"`
	PromptTokens int               `json:"prompt_tokens,omitempty"`
	OutputTokens int               `json:"output_tokens,omitempty"`
	Error        string            `json:"error,omitempty"`
	RawContent   string            `json:"raw_content,omitempty"`
	Translations map[string]string `json:"translations,omitempty"`
}

func (r *Report) Finish(latencies []time.Duration) {
	r.FinishedAt = time.Now()
	r.ElapsedMS = r.FinishedAt.Sub(r.StartedAt).Milliseconds()
	r.Status = "PASS"
	if r.FailureReason != "" || r.RequestsFailed > 0 || r.TermsMissing > 0 || r.RequestsSucceeded != r.RequestsPlanned {
		r.Status = "FAIL"
	}
	seconds := r.FinishedAt.Sub(r.StartedAt).Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}
	r.RequestsPerSecond = float64(r.RequestsSucceeded) / seconds
	r.TermsPerSecond = float64(r.TermsSucceeded) / seconds
	r.setLatencies(latencies)
}

func (r Report) Print(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Go LLM Translation Benchmark Report")
	_, _ = fmt.Fprintf(w, "Run ID: %s\n", r.RunID)
	_, _ = fmt.Fprintf(w, "Scenario: %s\n", r.Scenario)
	if r.Description != "" {
		_, _ = fmt.Fprintf(w, "Description: %s\n", r.Description)
	}
	_, _ = fmt.Fprintf(w, "Strategy: %s\n", r.Strategy)
	_, _ = fmt.Fprintf(w, "Base URL: %s\n", r.BaseURL)
	_, _ = fmt.Fprintf(w, "Model: %s\n", r.Model)
	_, _ = fmt.Fprintf(w, "Items planned: %d\n", r.ItemsPlanned)
	_, _ = fmt.Fprintf(w, "Batch size: %d\n", r.BatchSize)
	_, _ = fmt.Fprintf(w, "Parallel: %d\n", r.Parallel)
	_, _ = fmt.Fprintf(w, "Requests planned: %d\n", r.RequestsPlanned)
	_, _ = fmt.Fprintf(w, "Requests succeeded: %d\n", r.RequestsSucceeded)
	_, _ = fmt.Fprintf(w, "Requests failed: %d\n", r.RequestsFailed)
	_, _ = fmt.Fprintf(w, "Terms sent: %d\n", r.TermsSent)
	_, _ = fmt.Fprintf(w, "Terms succeeded: %d\n", r.TermsSucceeded)
	_, _ = fmt.Fprintf(w, "Terms missing: %d\n", r.TermsMissing)
	_, _ = fmt.Fprintf(w, "Started at: %s\n", r.StartedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "Finished at: %s\n", r.FinishedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "Elapsed: %s\n", time.Duration(r.ElapsedMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "Requests/sec: %.2f\n", r.RequestsPerSecond)
	_, _ = fmt.Fprintf(w, "Terms/sec: %.2f\n", r.TermsPerSecond)
	_, _ = fmt.Fprintf(w, "Min request latency: %s\n", time.Duration(r.MinRequestLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "P50 request latency: %s\n", time.Duration(r.P50RequestLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "P95 request latency: %s\n", time.Duration(r.P95RequestLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "Max request latency: %s\n", time.Duration(r.MaxRequestLatencyMS)*time.Millisecond)
	if r.FailureReason != "" {
		_, _ = fmt.Fprintf(w, "Failure reason: %s\n", r.FailureReason)
	}
	_, _ = fmt.Fprintf(w, "Status: %s\n", r.Status)
}

func WriteJSON(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

func (r *Report) setLatencies(latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	r.MinRequestLatencyMS = latencies[0].Milliseconds()
	r.P50RequestLatencyMS = percentile(latencies, 0.50)
	r.P95RequestLatencyMS = percentile(latencies, 0.95)
	r.MaxRequestLatencyMS = latencies[len(latencies)-1].Milliseconds()
}

func percentile(values []time.Duration, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(values))*p)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index].Milliseconds()
}
