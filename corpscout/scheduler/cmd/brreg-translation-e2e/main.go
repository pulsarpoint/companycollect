package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	APIURL            string
	DatabaseURL       string
	Provider          string
	Model             string
	PromptVersion     string
	Timeout           time.Duration
	PollInterval      time.Duration
	LeaseSeconds      int
	MaxServiceRetries int
	KeepRecord        bool
	EnvFile           string
}

type startWorkflowResponse struct {
	Status        string `json:"status"`
	Workflow      string `json:"workflow"`
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
}

type translationState struct {
	RawRecordID        string          `json:"raw_record_id"`
	OrganizationNumber string          `json:"organization_number"`
	WorkflowID         string          `json:"workflow_id"`
	WorkflowRunID      string          `json:"workflow_run_id"`
	WorkflowStatus     string          `json:"workflow_status"`
	WorkflowCompleted  int32           `json:"workflow_records_completed"`
	WorkflowFailed     int32           `json:"workflow_records_failed"`
	WorkflowError      string          `json:"workflow_error,omitempty"`
	TaskStatus         string          `json:"task_status,omitempty"`
	TaskError          string          `json:"task_error,omitempty"`
	TranslationStatus  string          `json:"translation_status,omitempty"`
	TranslationModel   string          `json:"translation_model,omitempty"`
	TranslationError   string          `json:"translation_error,omitempty"`
	TranslatedPayload  json.RawMessage `json:"translated_payload,omitempty"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "brreg translation e2e failed: %+v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseConfig()
	if cfg.EnvFile != "" {
		loadEnvFile(cfg.EnvFile)
		cfg = cfg.withEnvDefaults()
	}
	if cfg.APIURL == "" {
		return errors.New("api URL is required")
	}
	if cfg.DatabaseURL == "" {
		return errors.New("database URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout+30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return errors.Wrap(err, "connect database")
	}
	defer pool.Close()

	testRecord, err := insertTestRecord(ctx, pool)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if cfg.KeepRecord || !success {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `
DELETE FROM brreg_workflow.raw_records
WHERE id = $1::uuid
  AND metadata->>'e2e_test' = 'brreg_translation_real_llm'`,
			testRecord.ID,
		)
	}()

	workflow, err := startWorkflow(ctx, cfg, testRecord.ID)
	if err != nil {
		return err
	}

	state, err := waitForTranslation(ctx, pool, cfg, testRecord, workflow)
	if err != nil {
		return err
	}
	if err := validateTranslatedPayload(state.TranslatedPayload); err != nil {
		return err
	}

	success = true
	printJSON(map[string]any{
		"status":              "succeeded",
		"provider":            cfg.Provider,
		"model":               cfg.Model,
		"workflow_id":         workflow.WorkflowID,
		"workflow_run_id":     workflow.WorkflowRunID,
		"raw_record_id":       testRecord.ID,
		"organization_number": testRecord.OrganizationNumber,
		"translation_status":  state.TranslationStatus,
		"translation_model":   state.TranslationModel,
		"record_kept":         cfg.KeepRecord,
	})
	return nil
}

func parseConfig() config {
	defaultEnvFile := filepath.Join("..", ".env")
	if _, err := os.Stat(defaultEnvFile); err != nil {
		defaultEnvFile = ""
	}
	cfg := config{}
	flag.StringVar(&cfg.EnvFile, "env-file", defaultEnvFile, "optional .env file to load before reading defaults")
	flag.StringVar(&cfg.APIURL, "api-url", "", "Corpscout API base URL, for example http://localhost:8094/api/v1")
	flag.StringVar(&cfg.DatabaseURL, "database-url", "", "Postgres DSN for Corpscout")
	flag.StringVar(&cfg.Provider, "provider", "", "LLM provider slug configured in Corpscout")
	flag.StringVar(&cfg.Model, "model", "", "optional model override")
	flag.StringVar(&cfg.PromptVersion, "prompt-version", "v1", "translation prompt version")
	flag.DurationVar(&cfg.Timeout, "timeout", 5*time.Minute, "overall polling timeout")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 2*time.Second, "database polling interval")
	flag.IntVar(&cfg.LeaseSeconds, "lease-seconds", 300, "workflow lease/activity timeout seconds")
	flag.IntVar(&cfg.MaxServiceRetries, "max-service-retries", 2, "translation service retries inside one batch")
	flag.BoolVar(&cfg.KeepRecord, "keep-record", false, "keep the synthetic raw record after a successful run")
	flag.Parse()
	return cfg.withEnvDefaults()
}

func (cfg config) withEnvDefaults() config {
	if cfg.APIURL == "" {
		cfg.APIURL = envOr("CORPSCOUT_API_URL", "http://localhost:8094/api/v1")
	}
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = envOr("DATABASE_URL", "")
	}
	if cfg.Provider == "" {
		cfg.Provider = envOr("BRREG_TRANSLATION_E2E_PROVIDER", envOr("BRREG_TRANSLATION_PROVIDER", "deepseek-v4-flash"))
	}
	if cfg.Model == "" {
		cfg.Model = envOr("BRREG_TRANSLATION_E2E_MODEL", envOr("BRREG_TRANSLATION_MODEL", ""))
	}
	return cfg
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		key, value, _ := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

type testRecord struct {
	ID                 uuid.UUID
	OrganizationNumber string
}

func insertTestRecord(ctx context.Context, pool *pgxpool.Pool) (testRecord, error) {
	recordID := uuid.New()
	orgNumber := fmt.Sprintf("9%08d", time.Now().UnixNano()%100000000)
	payload := map[string]any{
		"organisasjonsnummer": orgNumber,
		"navn":                "CORPSCOUT E2E TEST AS",
		"organisasjonsform": map[string]any{
			"kode":        "AS",
			"beskrivelse": "Aksjeselskap",
		},
		"aktivitet": []string{
			"Drive utvikling av programvare og teknologitjenester.",
		},
		"vedtektsfestetFormaal": []string{
			"Utvikle, selge og drifte programvarelosninger.",
		},
		"forretningsadresse": map[string]any{
			"adresse":    []string{"Testveien 1"},
			"postnummer": "0150",
			"poststed":   "OSLO",
			"landkode":   "NO",
		},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return testRecord{}, errors.Wrap(err, "marshal e2e payload")
	}
	sum := sha256.Sum256(payloadJSON)
	metadata := []byte(`{"e2e_test":"brreg_translation_real_llm"}`)
	_, err = pool.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id,
  source_native_id,
  organization_number,
  organization_name,
  registration_status,
  country_iso2,
  raw_payload,
  payload_hash,
  metadata
) VALUES (
  $1::uuid,
  $2::text,
  $2::text,
  'CORPSCOUT E2E TEST AS',
  'active',
  'NO',
  $3::jsonb,
  $4::text,
  $5::jsonb
)`,
		recordID,
		orgNumber,
		string(payloadJSON),
		hex.EncodeToString(sum[:]),
		string(metadata),
	)
	if err != nil {
		return testRecord{}, errors.Wrap(err, "insert e2e raw record")
	}
	return testRecord{ID: recordID, OrganizationNumber: orgNumber}, nil
}

func startWorkflow(ctx context.Context, cfg config, rawRecordID uuid.UUID) (startWorkflowResponse, error) {
	body := map[string]any{
		"ids":                 []string{rawRecordID.String()},
		"limit":               1,
		"batch_size":          1,
		"max_attempts":        1,
		"max_parallel_tasks":  1,
		"lease_seconds":       cfg.LeaseSeconds,
		"provider":            cfg.Provider,
		"prompt_version":      cfg.PromptVersion,
		"source_lang":         "no",
		"target_lang":         "en",
		"max_service_retries": cfg.MaxServiceRetries,
		"trigger":             "e2e-real-llm",
	}
	if cfg.Model != "" {
		body["model"] = cfg.Model
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return startWorkflowResponse{}, errors.Wrap(err, "marshal workflow request")
	}
	endpoint := strings.TrimRight(cfg.APIURL, "/") + "/workflows/brreg/translation"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return startWorkflowResponse{}, errors.Wrap(err, "create workflow request")
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return startWorkflowResponse{}, errors.Wrap(err, "start brreg translation workflow")
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return startWorkflowResponse{}, errors.Newf("start workflow returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded startWorkflowResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return startWorkflowResponse{}, errors.Wrap(err, "decode workflow start response")
	}
	if decoded.WorkflowID == "" {
		return startWorkflowResponse{}, errors.New("workflow start response did not include workflow_id")
	}
	return decoded, nil
}

func waitForTranslation(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg config,
	record testRecord,
	workflow startWorkflowResponse,
) (translationState, error) {
	deadline := time.Now().Add(cfg.Timeout)
	var last translationState
	for time.Now().Before(deadline) {
		state, err := fetchTranslationState(ctx, pool, record, workflow)
		if err != nil {
			return translationState{}, err
		}
		last = state
		if state.TranslationStatus == "succeeded" {
			return state, nil
		}
		if state.TranslationStatus == "failed" || state.TaskStatus == "failed_terminal" || state.WorkflowStatus == "failed" {
			printJSON(state)
			return translationState{}, errors.New("translation workflow finished with failure")
		}
		time.Sleep(cfg.PollInterval)
	}
	printJSON(last)
	return translationState{}, errors.New("timed out waiting for translation result")
}

func fetchTranslationState(
	ctx context.Context,
	pool *pgxpool.Pool,
	record testRecord,
	workflow startWorkflowResponse,
) (translationState, error) {
	state := translationState{
		RawRecordID:        record.ID.String(),
		OrganizationNumber: record.OrganizationNumber,
		WorkflowID:         workflow.WorkflowID,
		WorkflowRunID:      workflow.WorkflowRunID,
	}
	err := pool.QueryRow(ctx, `
WITH latest_translation AS (
  SELECT status, model, error, translated_payload
  FROM brreg_workflow.translation_results
  WHERE raw_record_id = $1::uuid
  ORDER BY created_at DESC
  LIMIT 1
),
task_state AS (
  SELECT status, last_error
  FROM brreg_workflow.raw_record_task_states
  WHERE raw_record_id = $1::uuid
    AND task_type = 'translate'
),
workflow_run AS (
  SELECT status, records_completed, records_failed, error
  FROM brreg_workflow.workflow_runs
  WHERE orchestrator_run_id = $2::text
)
SELECT
  COALESCE((SELECT status FROM latest_translation), ''),
  COALESCE((SELECT model FROM latest_translation), ''),
  COALESCE((SELECT error FROM latest_translation), ''),
  COALESCE((SELECT translated_payload::text FROM latest_translation), ''),
  COALESCE((SELECT status FROM task_state), ''),
  COALESCE((SELECT last_error FROM task_state), ''),
  COALESCE((SELECT status FROM workflow_run), ''),
  COALESCE((SELECT records_completed FROM workflow_run), 0),
  COALESCE((SELECT records_failed FROM workflow_run), 0),
  COALESCE((SELECT error FROM workflow_run), '')`,
		record.ID,
		workflow.WorkflowID,
	).Scan(
		&state.TranslationStatus,
		&state.TranslationModel,
		&state.TranslationError,
		&state.TranslatedPayload,
		&state.TaskStatus,
		&state.TaskError,
		&state.WorkflowStatus,
		&state.WorkflowCompleted,
		&state.WorkflowFailed,
		&state.WorkflowError,
	)
	if err != nil {
		return translationState{}, errors.Wrap(err, "fetch translation state")
	}
	return state, nil
}

func validateTranslatedPayload(payload json.RawMessage) error {
	if len(payload) == 0 {
		return errors.New("translation result did not include translated_payload")
	}
	var decoded struct {
		SchemaVersion string `json:"schema_version"`
		Terms         []struct {
			OriginalText   string `json:"original_text"`
			TranslatedText string `json:"translated_text"`
		} `json:"terms"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return errors.Wrap(err, "decode translated payload")
	}
	if decoded.SchemaVersion == "" {
		return errors.New("translated payload is missing schema_version")
	}
	if len(decoded.Terms) == 0 {
		return errors.New("translated payload has no terms")
	}
	for _, term := range decoded.Terms {
		if strings.TrimSpace(term.OriginalText) == "" || strings.TrimSpace(term.TranslatedText) == "" {
			return errors.New("translated payload contains an empty term")
		}
	}
	return nil
}

func envOr(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func printJSON(value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Println(strconv.Quote(fmt.Sprint(value)))
		return
	}
	fmt.Println(string(data))
}
