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

func TestRegisterProcessRegistersWorkflowAndActivities(t *testing.T) {
	registry := &fakeProcessRegistry{}

	if err := RegisterProcess(registry, &fakeProcessRuntime{}); err != nil {
		t.Fatalf("register process: %v", err)
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
		"translator.ProcessOneBatch",
		"translator.FlushOutput",
	}
	if !reflect.DeepEqual(activityNames, expected) {
		t.Fatalf("unexpected activity registrations: got %#v want %#v", activityNames, expected)
	}
}

func TestRegisterProcessRequiresRuntime(t *testing.T) {
	if err := RegisterProcess(&fakeProcessRegistry{}, nil); !errors.Is(err, ErrProcessRuntimeRequired) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}

type fakeProcessRegistry struct {
	workflows  []interface{}
	activities []registeredActivity
}

type registeredActivity struct {
	activity interface{}
	options  activity.RegisterOptions
}

func (f *fakeProcessRegistry) RegisterWorkflow(workflow interface{}) {
	f.workflows = append(f.workflows, workflow)
}

func (f *fakeProcessRegistry) RegisterActivityWithOptions(
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

type fakeProcessRuntime struct{}

func (f *fakeProcessRuntime) ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error) {
	return engine.ProcessResult{}, nil
}

func (f *fakeProcessRuntime) FlushOutput(ctx context.Context) (engine.UploadResult, error) {
	return engine.UploadResult{}, nil
}
