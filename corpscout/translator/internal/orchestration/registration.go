package orchestration

import "go.temporal.io/sdk/activity"

type norwayBRREGRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

func RegisterNorwayBRREG(registry norwayBRREGRegistry, runtime BRREGRuntime) {
	registry.RegisterWorkflow(NorwayBRREGWorkflow)

	activities := BRREGActivities{Runtime: runtime}
	registry.RegisterActivityWithOptions(
		activities.LoadNewInput,
		activity.RegisterOptions{Name: ActivityLoadNewInput},
	)
	registry.RegisterActivityWithOptions(
		activities.ProcessOneBatch,
		activity.RegisterOptions{Name: ActivityProcessOneBatch},
	)
	registry.RegisterActivityWithOptions(
		activities.UploadOutput,
		activity.RegisterOptions{Name: ActivityUploadOutput},
	)
}
