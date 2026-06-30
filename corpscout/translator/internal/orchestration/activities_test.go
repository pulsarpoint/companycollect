package orchestration

import (
	"context"
	"errors"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
)

func TestBRREGActivitiesCallRuntime(t *testing.T) {
	runtime := &fakeBRREGRuntime{
		loadResult:    brreg.InitResult{RowsInserted: 11},
		processResult: brreg.ProcessResult{TranslatedCount: 3},
		uploadResult:  brreg.UploadResult{RowsSeen: 3, RowsInserted: 3},
	}
	activities := BRREGActivities{Runtime: runtime}
	ctx := context.Background()

	loadResult, err := activities.LoadNewInput(ctx)
	if err != nil {
		t.Fatalf("load activity failed: %v", err)
	}
	if loadResult.RowsInserted != 11 || runtime.loadCalls != 1 {
		t.Fatalf("load activity did not call runtime correctly: result=%#v calls=%d", loadResult, runtime.loadCalls)
	}

	processInput := brreg.ProcessInput{BatchSize: 9, TimeoutSeconds: 30}
	processResult, err := activities.ProcessOneBatch(ctx, processInput)
	if err != nil {
		t.Fatalf("process activity failed: %v", err)
	}
	if processResult.TranslatedCount != 3 || runtime.processInput != processInput {
		t.Fatalf("process activity did not call runtime correctly: result=%#v input=%#v", processResult, runtime.processInput)
	}

	uploadResult, err := activities.UploadOutput(ctx)
	if err != nil {
		t.Fatalf("upload activity failed: %v", err)
	}
	if uploadResult.RowsInserted != 3 || runtime.uploadCalls != 1 {
		t.Fatalf("upload activity did not call runtime correctly: result=%#v calls=%d", uploadResult, runtime.uploadCalls)
	}
}

func TestBRREGActivitiesRequireRuntime(t *testing.T) {
	activities := BRREGActivities{}

	if _, err := activities.LoadNewInput(context.Background()); !errors.Is(err, ErrBRREGRuntimeRequired) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}

type fakeBRREGRuntime struct {
	loadCalls    int
	processInput brreg.ProcessInput
	uploadCalls  int

	loadResult    brreg.InitResult
	processResult brreg.ProcessResult
	uploadResult  brreg.UploadResult
}

func (f *fakeBRREGRuntime) LoadNewInput(ctx context.Context) (brreg.InitResult, error) {
	f.loadCalls++
	return f.loadResult, nil
}

func (f *fakeBRREGRuntime) ProcessOneBatch(ctx context.Context, input brreg.ProcessInput) (brreg.ProcessResult, error) {
	f.processInput = input
	return f.processResult, nil
}

func (f *fakeBRREGRuntime) UploadOutput(ctx context.Context) (brreg.UploadResult, error) {
	f.uploadCalls++
	return f.uploadResult, nil
}
