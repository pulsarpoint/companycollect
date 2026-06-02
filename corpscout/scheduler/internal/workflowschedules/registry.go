package workflowschedules

import (
	"encoding/json"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
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
	return []Definition{naceTaxonomySyncDefinition()}
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
		WorkflowName:       nacetaxonomy.SyncWorkflowName,
		TaskQueue:          nacetaxonomy.SyncTaskQueue,
		Domain:             "taxonomy",
		Purpose:            "nace_taxonomy_sync",
		DefaultDisplayName: "NACE taxonomy sync",
		DefaultDescription: "Downloads and imports the configured NACE taxonomy source when the source file changes.",
		DecodeActionInput:  decodeNACETaxonomySyncInput,
	}
}

func decodeNACETaxonomySyncInput(raw json.RawMessage) (any, error) {
	var input nacetaxonomy.SyncNACETaxonomyInput
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
