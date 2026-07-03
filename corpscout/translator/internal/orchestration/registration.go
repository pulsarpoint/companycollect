package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

var ErrProcessRuntimeRequired = errors.New("process runtime is required")

// ProcessRuntime is the activity implementation; *engine.Runtime satisfies it.
type ProcessRuntime interface {
	ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error)
	FlushOutput(ctx context.Context) (engine.UploadResult, error)
}

type processRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

// RegisterProcess registers the translation workflow and its activities on
// the translator's single Temporal worker.
func RegisterProcess(registry processRegistry, runtime ProcessRuntime) error {
	if runtime == nil {
		return ErrProcessRuntimeRequired
	}
	registry.RegisterWorkflow(engine.TranslationWorkflow)
	registry.RegisterActivityWithOptions(
		runtime.ProcessOneBatch,
		activity.RegisterOptions{Name: engine.ActivityProcessOneBatch},
	)
	registry.RegisterActivityWithOptions(
		runtime.FlushOutput,
		activity.RegisterOptions{Name: engine.ActivityFlushOutput},
	)
	return nil
}
