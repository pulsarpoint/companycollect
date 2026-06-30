package orchestration

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"go.temporal.io/sdk/activity"
)

func TestRegisterNorwayBRREGRegistersWorkflowAndActivities(t *testing.T) {
	registry := &fakeBRREGRegistry{}

	if err := RegisterNorwayBRREG(registry, &fakeBRREGRuntime{}); err != nil {
		t.Fatalf("register norway brreg: %v", err)
	}

	if len(registry.workflows) != 1 {
		t.Fatalf("expected one workflow registration, got %d", len(registry.workflows))
	}
	if !strings.HasSuffix(functionName(registry.workflows[0]), ".NorwayBRREGWorkflow") {
		t.Fatalf("unexpected workflow registration: %s", functionName(registry.workflows[0]))
	}

	activityNames := make([]string, 0, len(registry.activities))
	for _, registered := range registry.activities {
		activityNames = append(activityNames, registered.options.Name)
	}

	expected := []string{
		brreg.ActivityLoadNewInput,
		brreg.ActivityProcessOneBatch,
		brreg.ActivityUploadOutput,
	}
	if !reflect.DeepEqual(activityNames, expected) {
		t.Fatalf("unexpected activity registrations: got %#v want %#v", activityNames, expected)
	}
}

func TestRegisterNorwayBRREGRequiresRuntime(t *testing.T) {
	if err := RegisterNorwayBRREG(&fakeBRREGRegistry{}, nil); !errors.Is(err, ErrBRREGRuntimeRequired) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}

type fakeBRREGRegistry struct {
	workflows  []interface{}
	activities []registeredActivity
}

type registeredActivity struct {
	activity interface{}
	options  activity.RegisterOptions
}

func (f *fakeBRREGRegistry) RegisterWorkflow(workflow interface{}) {
	f.workflows = append(f.workflows, workflow)
}

func (f *fakeBRREGRegistry) RegisterActivityWithOptions(
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

type fakeBRREGRuntime struct{}

func (f *fakeBRREGRuntime) LoadNewInput(ctx context.Context) (brreg.InitResult, error) {
	return brreg.InitResult{}, nil
}

func (f *fakeBRREGRuntime) ProcessOneBatch(ctx context.Context, input brreg.ProcessInput) (brreg.ProcessResult, error) {
	return brreg.ProcessResult{}, nil
}

func (f *fakeBRREGRuntime) UploadOutput(ctx context.Context) (brreg.UploadResult, error) {
	return brreg.UploadResult{}, nil
}
