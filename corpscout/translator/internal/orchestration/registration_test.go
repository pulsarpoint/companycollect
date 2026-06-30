package orchestration

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
)

func TestRegisterNorwayBRREGRegistersWorkflowAndActivities(t *testing.T) {
	registry := &fakeBRREGRegistry{}

	RegisterNorwayBRREG(registry, &fakeBRREGRuntime{})

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
		ActivityLoadNewInput,
		ActivityProcessOneBatch,
		ActivityUploadOutput,
	}
	if !reflect.DeepEqual(activityNames, expected) {
		t.Fatalf("unexpected activity registrations: got %#v want %#v", activityNames, expected)
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
