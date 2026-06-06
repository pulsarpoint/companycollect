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

type InvalidBatch struct {
	JobID   string `json:"job_id"`
	BatchID string `json:"batch_id"`
	Error   string `json:"error"`
}

type Report struct {
	RunID             string         `json:"run_id"`
	Status            string         `json:"status"`
	NATSURL           string         `json:"nats_url"`
	InputQueue        string         `json:"input_queue"`
	OutputQueue       string         `json:"output_queue"`
	InputStream       string         `json:"input_stream"`
	OutputStream      string         `json:"output_stream"`
	BatchesPlanned    int            `json:"batches_planned"`
	BatchesSent       int            `json:"batches_sent"`
	BatchesReceived   int            `json:"batches_received"`
	TermsSent         int            `json:"terms_sent"`
	TermsSucceeded    int            `json:"terms_succeeded"`
	TermsFailed       int            `json:"terms_failed"`
	StartedAt         time.Time      `json:"started_at"`
	FinishedAt        time.Time      `json:"finished_at"`
	ElapsedMS         int64          `json:"elapsed_ms"`
	BatchesPerSecond  float64        `json:"batches_per_second"`
	TermsPerSecond    float64        `json:"terms_per_second"`
	MinBatchLatencyMS int64          `json:"min_batch_latency_ms"`
	P50BatchLatencyMS int64          `json:"p50_batch_latency_ms"`
	P95BatchLatencyMS int64          `json:"p95_batch_latency_ms"`
	MaxBatchLatencyMS int64          `json:"max_batch_latency_ms"`
	MissingBatches    []string       `json:"missing_batches"`
	InvalidBatches    []InvalidBatch `json:"invalid_batches"`
	LastInputDepth    uint64         `json:"last_input_depth"`
	FailureReason     string         `json:"failure_reason,omitempty"`
}

func New(runID string, natsURL string, inputQueue string, outputQueue string, inputStream string, outputStream string, batches int, startedAt time.Time) *Report {
	return &Report{
		RunID:          runID,
		Status:         "RUNNING",
		NATSURL:        natsURL,
		InputQueue:     inputQueue,
		OutputQueue:    outputQueue,
		InputStream:    inputStream,
		OutputStream:   outputStream,
		BatchesPlanned: batches,
		StartedAt:      startedAt,
	}
}

func (r *Report) Finish(latencies []time.Duration) {
	r.FinishedAt = time.Now()
	r.ElapsedMS = r.FinishedAt.Sub(r.StartedAt).Milliseconds()
	r.Status = "PASS"
	if r.FailureReason != "" || len(r.MissingBatches) > 0 || len(r.InvalidBatches) > 0 || r.BatchesReceived != r.BatchesPlanned || r.TermsFailed > 0 {
		r.Status = "FAIL"
	}
	seconds := r.FinishedAt.Sub(r.StartedAt).Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}
	r.BatchesPerSecond = float64(r.BatchesReceived) / seconds
	r.TermsPerSecond = float64(r.TermsSucceeded) / seconds
	r.setLatencies(latencies)
}

func (r *Report) Print(w io.Writer) {
	_, _ = fmt.Fprintf(w, "Translation E2E Report\n")
	_, _ = fmt.Fprintf(w, "Run ID: %s\n", r.RunID)
	_, _ = fmt.Fprintf(w, "NATS URL: %s\n", r.NATSURL)
	_, _ = fmt.Fprintf(w, "Input queue: %s\n", r.InputQueue)
	_, _ = fmt.Fprintf(w, "Output queue: %s\n", r.OutputQueue)
	_, _ = fmt.Fprintf(w, "Batches planned: %d\n", r.BatchesPlanned)
	_, _ = fmt.Fprintf(w, "Batches sent: %d\n", r.BatchesSent)
	_, _ = fmt.Fprintf(w, "Batches received: %d\n", r.BatchesReceived)
	_, _ = fmt.Fprintf(w, "Terms sent: %d\n", r.TermsSent)
	_, _ = fmt.Fprintf(w, "Terms succeeded: %d\n", r.TermsSucceeded)
	_, _ = fmt.Fprintf(w, "Terms failed: %d\n", r.TermsFailed)
	_, _ = fmt.Fprintf(w, "Started at: %s\n", r.StartedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "Finished at: %s\n", r.FinishedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "Elapsed: %s\n", time.Duration(r.ElapsedMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "Batches/sec: %.2f\n", r.BatchesPerSecond)
	_, _ = fmt.Fprintf(w, "Terms/sec: %.2f\n", r.TermsPerSecond)
	_, _ = fmt.Fprintf(w, "Min batch latency: %s\n", time.Duration(r.MinBatchLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "P50 batch latency: %s\n", time.Duration(r.P50BatchLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "P95 batch latency: %s\n", time.Duration(r.P95BatchLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "Max batch latency: %s\n", time.Duration(r.MaxBatchLatencyMS)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "Missing batches: %v\n", r.MissingBatches)
	_, _ = fmt.Fprintf(w, "Invalid batches: %v\n", r.InvalidBatches)
	if r.FailureReason != "" {
		_, _ = fmt.Fprintf(w, "Failure reason: %s\n", r.FailureReason)
	}
	_, _ = fmt.Fprintf(w, "Last input depth: %d\n", r.LastInputDepth)
	_, _ = fmt.Fprintf(w, "Status: %s\n", r.Status)
}

func (r *Report) WriteJSON(path string) error {
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report json: %w", err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		return fmt.Errorf("write report json: %w", err)
	}
	return nil
}

func (r *Report) setLatencies(latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	r.MinBatchLatencyMS = latencies[0].Milliseconds()
	r.P50BatchLatencyMS = percentile(latencies, 0.50).Milliseconds()
	r.P95BatchLatencyMS = percentile(latencies, 0.95).Milliseconds()
	r.MaxBatchLatencyMS = latencies[len(latencies)-1].Milliseconds()
}

func percentile(values []time.Duration, p float64) time.Duration {
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
	return values[index]
}
