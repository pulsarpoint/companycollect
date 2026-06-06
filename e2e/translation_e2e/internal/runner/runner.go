package runner

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/config"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/fixtures"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/jetstream"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/report"
	"github.com/pulsarpoint/companycollect/e2e/translation_e2e/internal/validate"
)

const resultFetchWait = 250 * time.Millisecond

func Run(ctx context.Context, cfg config.Config) (*report.Report, error) {
	startedAt := time.Now()
	runID := fmt.Sprintf("translation-e2e-%s", startedAt.UTC().Format("20060102-150405.000000000"))
	jobs := fixtures.BuildJobs(cfg)
	rep := report.New(
		runID,
		cfg.NATSURL,
		cfg.InputQueue,
		cfg.OutputQueue,
		cfg.InputStream,
		cfg.OutputStream,
		len(jobs),
		startedAt,
	)
	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	harness, err := jetstream.New(runCtx, cfg)
	if err != nil {
		rep.FailureReason = err.Error()
		rep.Finish(nil)
		return rep, err
	}
	defer harness.Close()

	expectedByJobID := make(map[string]fixtures.ExpectedJob, len(jobs))
	for _, job := range jobs {
		expectedByJobID[job.Job.JobID] = job
	}
	sentAt := make(map[string]time.Time, len(jobs))
	receivedAt := make(map[string]time.Time, len(jobs))
	latencies := make([]time.Duration, 0, len(jobs))
	nextPublishCheck := time.Time{}
	nextJob := 0

	for len(receivedAt) < len(jobs) {
		if err := runCtx.Err(); err != nil {
			rep.FailureReason = err.Error()
			rep.MissingBatches = missingJobIDs(jobs, receivedAt)
			rep.Finish(latencies)
			return rep, errors.Wrap(err, "translation e2e timed out")
		}
		now := time.Now()
		if nextJob < len(jobs) && !now.Before(nextPublishCheck) {
			depth, err := harness.InputDepth(runCtx)
			if err != nil {
				rep.FailureReason = err.Error()
				rep.Finish(latencies)
				return rep, err
			}
			rep.LastInputDepth = depth
			if depth <= cfg.MaxInputMessages {
				job := jobs[nextJob].Job
				if err := harness.PublishJob(runCtx, job); err != nil {
					rep.FailureReason = err.Error()
					rep.Finish(latencies)
					return rep, err
				}
				sentAt[job.JobID] = time.Now()
				rep.BatchesSent++
				rep.TermsSent += len(job.Terms)
				nextJob++
			}
			nextPublishCheck = now.Add(time.Second)
		}

		messages, err := harness.FetchResults(runCtx, 1, resultFetchWait)
		if err != nil {
			rep.FailureReason = err.Error()
			rep.Finish(latencies)
			return rep, err
		}
		for _, message := range messages {
			result := message.Result
			expected, ok := expectedByJobID[result.JobID]
			if !ok {
				invalid := report.InvalidBatch{JobID: result.JobID, BatchID: result.BatchID, Error: "unknown job_id"}
				rep.InvalidBatches = append(rep.InvalidBatches, invalid)
				rep.FailureReason = invalid.Error
				rep.Finish(latencies)
				return rep, fmt.Errorf("received result for unknown job_id %q", result.JobID)
			}
			if _, duplicate := receivedAt[result.JobID]; duplicate {
				invalid := report.InvalidBatch{JobID: result.JobID, BatchID: result.BatchID, Error: "duplicate result"}
				rep.InvalidBatches = append(rep.InvalidBatches, invalid)
				rep.FailureReason = invalid.Error
				rep.Finish(latencies)
				return rep, fmt.Errorf("received duplicate result for job_id %q", result.JobID)
			}
			if err := validate.Result(cfg, expected, result); err != nil {
				invalid := report.InvalidBatch{JobID: result.JobID, BatchID: result.BatchID, Error: err.Error()}
				rep.InvalidBatches = append(rep.InvalidBatches, invalid)
				rep.FailureReason = err.Error()
				rep.Finish(latencies)
				return rep, err
			}
			if err := message.Ack(runCtx); err != nil {
				rep.FailureReason = err.Error()
				rep.Finish(latencies)
				return rep, err
			}
			now := time.Now()
			receivedAt[result.JobID] = now
			rep.BatchesReceived++
			rep.TermsSucceeded += len(result.Results)
			rep.TermsFailed += len(result.Failures)
			if sent := sentAt[result.JobID]; !sent.IsZero() {
				latencies = append(latencies, now.Sub(sent))
			}
		}
	}
	rep.MissingBatches = missingJobIDs(jobs, receivedAt)
	rep.Finish(latencies)
	return rep, nil
}

func missingJobIDs(jobs []fixtures.ExpectedJob, receivedAt map[string]time.Time) []string {
	missing := make([]string, 0)
	for _, job := range jobs {
		if _, ok := receivedAt[job.Job.JobID]; !ok {
			missing = append(missing, job.Job.JobID)
		}
	}
	sort.Strings(missing)
	return missing
}
