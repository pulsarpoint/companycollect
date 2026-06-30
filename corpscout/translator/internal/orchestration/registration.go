package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"go.temporal.io/sdk/activity"
)

var ErrBRREGRuntimeRequired = errors.New("brreg runtime is required")

type BRREGRuntime interface {
	LoadNewInput(ctx context.Context) (brreg.InitResult, error)
	ProcessOneBatch(ctx context.Context, input brreg.ProcessInput) (brreg.ProcessResult, error)
	UploadOutput(ctx context.Context) (brreg.UploadResult, error)
}

type norwayBRREGRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

func RegisterNorwayBRREG(registry norwayBRREGRegistry, runtime BRREGRuntime) error {
	if runtime == nil {
		return ErrBRREGRuntimeRequired
	}

	registry.RegisterWorkflow(brreg.NorwayBRREGWorkflow)

	registry.RegisterActivityWithOptions(
		runtime.LoadNewInput,
		activity.RegisterOptions{Name: brreg.ActivityLoadNewInput},
	)
	registry.RegisterActivityWithOptions(
		runtime.ProcessOneBatch,
		activity.RegisterOptions{Name: brreg.ActivityProcessOneBatch},
	)
	registry.RegisterActivityWithOptions(
		runtime.UploadOutput,
		activity.RegisterOptions{Name: brreg.ActivityUploadOutput},
	)
	return nil
}
