package workflow

import temporalworkflow "go.temporal.io/sdk/workflow"

const (
	TranslateBrregRawInputsTaskQueue    = "brreg-translation"
	TranslateBrregRawInputsWorkflowName = "TranslateBrregRawInputs"
)

type TranslateBrregRawInputsInput struct {
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`
}

type TranslateBrregRawInputsResult struct {
	Status string `json:"status"`
}

func TranslateBrregRawInputs(_ temporalworkflow.Context, _ TranslateBrregRawInputsInput) (TranslateBrregRawInputsResult, error) {
	return TranslateBrregRawInputsResult{Status: "empty"}, nil
}
