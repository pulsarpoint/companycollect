package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
)

var ErrBRREGRuntimeRequired = errors.New("brreg runtime is required")

type BRREGRuntime interface {
	LoadNewInput(ctx context.Context) (brreg.InitResult, error)
	ProcessOneBatch(ctx context.Context, input brreg.ProcessInput) (brreg.ProcessResult, error)
	UploadOutput(ctx context.Context) (brreg.UploadResult, error)
}

type BRREGActivities struct {
	Runtime BRREGRuntime
}

func (a BRREGActivities) LoadNewInput(ctx context.Context) (brreg.InitResult, error) {
	if a.Runtime == nil {
		return brreg.InitResult{}, ErrBRREGRuntimeRequired
	}
	return a.Runtime.LoadNewInput(ctx)
}

func (a BRREGActivities) ProcessOneBatch(ctx context.Context, input brreg.ProcessInput) (brreg.ProcessResult, error) {
	if a.Runtime == nil {
		return brreg.ProcessResult{}, ErrBRREGRuntimeRequired
	}
	return a.Runtime.ProcessOneBatch(ctx, input)
}

func (a BRREGActivities) UploadOutput(ctx context.Context) (brreg.UploadResult, error) {
	if a.Runtime == nil {
		return brreg.UploadResult{}, ErrBRREGRuntimeRequired
	}
	return a.Runtime.UploadOutput(ctx)
}
