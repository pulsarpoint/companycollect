package orchestration

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

func TestRegisterSourceRegistersWorkflowAndPerSourceActivities(t *testing.T) {
	registry := &fakeSourceRegistry{}

	if err := RegisterSource(registry, "norway_brreg", &fakeSourceRuntime{}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	if len(registry.workflows) != 1 {
		t.Fatalf("expected one workflow registration, got %d", len(registry.workflows))
	}
	if !strings.HasSuffix(functionName(registry.workflows[0]), ".TranslationWorkflow") {
		t.Fatalf("unexpected workflow registration: %s", functionName(registry.workflows[0]))
	}

	activityNames := make([]string, 0, len(registry.activities))
	for _, registered := range registry.activities {
		activityNames = append(activityNames, registered.options.Name)
	}

	expected := []string{
		"norway_brreg.LoadNewInput",
		"norway_brreg.ProcessOneBatch",
		"norway_brreg.UploadOutput",
	}
	if !reflect.DeepEqual(activityNames, expected) {
		t.Fatalf("unexpected activity registrations: got %#v want %#v", activityNames, expected)
	}
}

func TestRegisterSourceRequiresRuntime(t *testing.T) {
	if err := RegisterSource(&fakeSourceRegistry{}, "norway_brreg", nil); !errors.Is(err, ErrSourceRuntimeRequired) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}

func TestRegisterSourceRequiresSourceName(t *testing.T) {
	if err := RegisterSource(&fakeSourceRegistry{}, "", &fakeSourceRuntime{}); !errors.Is(err, ErrSourceNameRequired) {
		t.Fatalf("expected missing source name error, got %v", err)
	}
}

type fakeSourceRegistry struct {
	workflows  []interface{}
	activities []registeredActivity
}

type registeredActivity struct {
	activity interface{}
	options  activity.RegisterOptions
}

func (f *fakeSourceRegistry) RegisterWorkflow(workflow interface{}) {
	f.workflows = append(f.workflows, workflow)
}

func (f *fakeSourceRegistry) RegisterActivityWithOptions(
	activityFunc interface{},
	options activity.RegisterOptions,
) {
	f.activities = append(f.activities, registeredActivity{
		activity: activityFunc,
		options:  options,
	})
}

func functionName(value interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(value).Pointer()).Name()
}

type fakeSourceRuntime struct{}

func (f *fakeSourceRuntime) LoadNewInput(ctx context.Context) (engine.LoadResult, error) {
	return engine.LoadResult{}, nil
}

func (f *fakeSourceRuntime) ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error) {
	return engine.ProcessResult{}, nil
}

func (f *fakeSourceRuntime) UploadOutput(ctx context.Context) (engine.UploadResult, error) {
	return engine.UploadResult{}, nil
}
