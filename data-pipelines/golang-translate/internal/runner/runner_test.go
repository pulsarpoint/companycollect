package runner

import (
	"context"
	"testing"

	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/config"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/fixture"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/llm"
)

type fakeClient struct{}

func (fakeClient) Translate(_ context.Context, request llm.Request) (llm.Result, error) {
	result := make(map[string]string, len(request.Items))
	for _, item := range request.Items {
		result[item.ID] = item.Text + " translated"
	}
	return llm.Result{Translations: result, RawContent: `{"translations":[]}`}, nil
}

func TestRunSingleStrategySendsOneLongRequest(t *testing.T) {
	cfg := config.Config{
		InputPath:      "input.json",
		BaseURL:        "http://llm",
		Model:          "qwen3:6b",
		Strategy:       config.StrategySingle,
		Items:          3,
		BatchSize:      1,
		Parallel:       4,
		Timeout:        1_000_000_000,
		RequestTimeout: 1_000_000_000,
		Scenario:       "single",
	}
	input := fixture.Input{
		SourceLang: "et",
		TargetLang: "en",
		Items:      []fixture.Item{{ID: "one", Text: "one"}, {ID: "two", Text: "two"}, {ID: "three", Text: "three"}},
	}

	rep, _, err := Run(context.Background(), cfg, input, fakeClient{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.RequestsPlanned != 1 || rep.TermsSucceeded != 3 || rep.Parallel != 1 {
		t.Fatalf("unexpected report: %#v", rep)
	}
}
