package workflowschedules

import (
	"encoding/json"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

type Definition struct {
	Key                string
	WorkflowName       string
	TaskQueue          string
	Domain             string
	Purpose            string
	DefaultDisplayName string
	DefaultDescription string
	DecodeActionInput  func(json.RawMessage) (any, error)
}

func Definitions() []Definition {
	return []Definition{
		naceTaxonomySyncDefinition(),
		fxRateSyncDefinition(),
	}
}

func DefinitionByKey(key string) (Definition, bool) {
	key = strings.TrimSpace(key)
	for _, def := range Definitions() {
		if def.Key == key {
			return def, true
		}
	}
	return Definition{}, false
}

func naceTaxonomySyncDefinition() Definition {
	return Definition{
		Key:                "nace_taxonomy_sync",
		WorkflowName:       naceworkflow.SyncWorkflowName,
		TaskQueue:          naceworkflow.SyncTaskQueue,
		Domain:             "taxonomy",
		Purpose:            "nace_taxonomy_sync",
		DefaultDisplayName: "NACE taxonomy sync",
		DefaultDescription: "Downloads and imports the configured NACE taxonomy source when the source file changes.",
		DecodeActionInput:  decodeNACETaxonomySyncInput,
	}
}

func decodeNACETaxonomySyncInput(raw json.RawMessage) (any, error) {
	var input naceworkflow.SyncNACETaxonomyInput
	if len(raw) > 0 && strings.TrimSpace(string(raw)) != "null" {
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, errors.New("invalid nace taxonomy sync action input")
		}
	}
	input.Revision = strings.TrimSpace(input.Revision)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Trigger == "" {
		input.Trigger = "schedule"
	}
	if input.Revision == "" {
		input.Revision = nacetaxonomy.DefaultRevision
	}
	if input.SourceURL == "" {
		return nil, errors.New("nace source url is required")
	}
	if input.Trigger != "schedule" && input.Trigger != "manual" {
		return nil, errors.New("nace taxonomy sync trigger must be schedule or manual")
	}
	return input, nil
}

func fxRateSyncDefinition() Definition {
	return Definition{
		Key:                "fx_rate_sync",
		WorkflowName:       fx.SyncWorkflowName,
		TaskQueue:          fx.SyncTaskQueue,
		Domain:             "reference_data",
		Purpose:            "fx_rate_sync",
		DefaultDisplayName: "FX rate sync",
		DefaultDescription: "Downloads and imports the configured ECB exchange rate feed when the source file changes.",
		DecodeActionInput:  decodeFXRateSyncInput,
	}
}

func decodeFXRateSyncInput(raw json.RawMessage) (any, error) {
	var input fx.SyncExchangeRatesInput
	if len(raw) > 0 && strings.TrimSpace(string(raw)) != "null" {
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, errors.New("invalid fx rate sync action input")
		}
	}
	input.Provider = strings.ToLower(strings.TrimSpace(input.Provider))
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Provider == "" {
		input.Provider = fx.DefaultProvider
	}
	if input.SourceURL == "" {
		return nil, errors.New("fx source url is required")
	}
	if input.Trigger == "" {
		input.Trigger = "schedule"
	}
	if input.Provider != fx.DefaultProvider {
		return nil, errors.New("fx provider must be ecb")
	}
	if input.Trigger != "schedule" && input.Trigger != "manual" {
		return nil, errors.New("fx rate sync trigger must be schedule or manual")
	}
	return input, nil
}
