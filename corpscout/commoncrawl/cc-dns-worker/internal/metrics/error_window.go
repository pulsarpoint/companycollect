package metrics

import "time"

type errorSample struct {
	at      time.Time
	domains int64
	errors  int64
}

// ErrorWindow tracks aggregate domain outcomes over a fixed recent interval. It is owned by the
// single reporting goroutine, so it needs neither locks nor one allocation per resolved domain.
type ErrorWindow struct {
	duration time.Duration
	samples  []errorSample
	domains  int64
	errors   int64
}

func NewErrorWindow(duration time.Duration) *ErrorWindow {
	return &ErrorWindow{duration: duration}
}

// Add records one reporting interval and evicts intervals older than the configured duration.
func (window *ErrorWindow) Add(at time.Time, domains, errors int64) {
	if domains < 0 {
		domains = 0
	}
	if errors < 0 {
		errors = 0
	}
	sample := errorSample{at: at, domains: domains, errors: errors}
	window.samples = append(window.samples, sample)
	window.domains += domains
	window.errors += errors

	cutoff := at.Add(-window.duration)
	firstCurrent := 0
	for firstCurrent < len(window.samples) && window.samples[firstCurrent].at.Before(cutoff) {
		window.domains -= window.samples[firstCurrent].domains
		window.errors -= window.samples[firstCurrent].errors
		firstCurrent++
	}
	if firstCurrent > 0 {
		window.samples = window.samples[firstCurrent:]
	}
}

func (window *ErrorWindow) Percent() float64 {
	return pct(window.errors, window.domains)
}
