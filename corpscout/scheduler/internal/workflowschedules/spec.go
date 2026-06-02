package workflowschedules

import (
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/robfig/cron"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const DefaultTimezone = "Europe/Belgrade"

type ScheduleSpecInput struct {
	CronExpression       string `json:"cron_expression"`
	Timezone             string `json:"timezone"`
	OverlapPolicy        string `json:"overlap_policy"`
	CatchupWindowSeconds int    `json:"catchup_window_seconds"`
}

func BuildScheduleSpec(input ScheduleSpecInput) (client.ScheduleSpec, error) {
	cronExpression := strings.TrimSpace(input.CronExpression)
	if cronExpression == "" {
		return client.ScheduleSpec{}, errors.New("cron expression is required")
	}
	if fields := strings.Fields(cronExpression); len(fields) != 5 {
		return client.ScheduleSpec{}, errors.New("cron expression must contain 5 fields")
	}
	if _, err := cron.ParseStandard(cronExpression); err != nil {
		return client.ScheduleSpec{}, errors.New("cron expression is invalid")
	}

	timezone := strings.TrimSpace(input.Timezone)
	if timezone == "" {
		timezone = DefaultTimezone
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return client.ScheduleSpec{}, errors.New("timezone is invalid")
	}

	if input.CatchupWindowSeconds < 0 {
		return client.ScheduleSpec{}, errors.New("catchup window seconds cannot be negative")
	}

	return client.ScheduleSpec{
		CronExpressions: []string{cronExpression},
		TimeZoneName:    timezone,
	}, nil
}

func OverlapPolicy(value string) (enumspb.ScheduleOverlapPolicy, error) {
	switch strings.TrimSpace(value) {
	case "", "skip":
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, nil
	case "buffer_one":
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, nil
	case "allow_all":
		return enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, nil
	case "cancel_other":
		return enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, nil
	case "terminate_other":
		return enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER, nil
	default:
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, errors.New("unsupported overlap policy")
	}
}
