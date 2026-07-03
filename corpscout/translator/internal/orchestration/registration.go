package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

var (
	ErrSourceRuntimeRequired = errors.New("source runtime is required")
	ErrSourceNameRequired    = errors.New("source name is required")
)

// SourceRuntime is the per-source activity implementation; *engine.Runtime
// satisfies it.
type SourceRuntime interface {
	LoadNewInput(ctx context.Context) (engine.LoadResult, error)
	ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error)
	UploadOutput(ctx context.Context) (engine.UploadResult, error)
}

type sourceRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

// RegisterSource registers the shared translation workflow and one source's
// activities. Each source runs on its own task queue with its own worker, so
// every source's worker registers both the workflow and its activities.
func RegisterSource(registry sourceRegistry, source string, runtime SourceRuntime) error {
	if source == "" {
		return ErrSourceNameRequired
	}
	if runtime == nil {
		return ErrSourceRuntimeRequired
	}

	registry.RegisterWorkflow(engine.TranslationWorkflow)

	registry.RegisterActivityWithOptions(
		runtime.LoadNewInput,
		activity.RegisterOptions{Name: engine.ActivityLoadNewInput(source)},
	)
	registry.RegisterActivityWithOptions(
		runtime.ProcessOneBatch,
		activity.RegisterOptions{Name: engine.ActivityProcessOneBatch(source)},
	)
	registry.RegisterActivityWithOptions(
		runtime.UploadOutput,
		activity.RegisterOptions{Name: engine.ActivityUploadOutput(source)},
	)
	return nil
}
