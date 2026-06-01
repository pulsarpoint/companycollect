package httpapi

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/robfig/cron"
)

func validScheduleKind(kind string) bool {
	switch kind {
	case "manual", "interval", "cron", "event":
		return true
	default:
		return false
	}
}

func parsePositiveDuration(expr string) (time.Duration, error) {
	duration, err := time.ParseDuration(expr)
	if err != nil {
		return 0, errors.Wrap(err, "parse schedule expression")
	}
	if duration <= 0 {
		return 0, errors.Newf("schedule expression must be positive")
	}
	return duration, nil
}

func parseCronSchedule(expr string) (cron.Schedule, error) {
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, errors.Wrap(err, "parse cron schedule expression")
	}
	return schedule, nil
}
