package metrics

import "time"

type errorSample struct {
	at       time.Time
	attempts int64
	errors   int64
}

// ErrorWindow tracks aggregate query attempts over a fixed recent interval. It is owned by the
// single reporting goroutine, so it needs neither locks nor one allocation per DNS query.
type ErrorWindow struct {
	duration time.Duration
	samples  []errorSample
	attempts int64
	errors   int64
}

func NewErrorWindow(duration time.Duration) *ErrorWindow {
	return &ErrorWindow{duration: duration}
}

// Add records one reporting interval and evicts intervals older than the configured duration.
func (window *ErrorWindow) Add(at time.Time, attempts, errors int64) {
	if attempts < 0 {
		attempts = 0
	}
	if errors < 0 {
		errors = 0
	}
	sample := errorSample{at: at, attempts: attempts, errors: errors}
	window.samples = append(window.samples, sample)
	window.attempts += attempts
	window.errors += errors

	cutoff := at.Add(-window.duration)
	firstCurrent := 0
	for firstCurrent < len(window.samples) && !window.samples[firstCurrent].at.After(cutoff) {
		window.attempts -= window.samples[firstCurrent].attempts
		window.errors -= window.samples[firstCurrent].errors
		firstCurrent++
	}
	if firstCurrent > 0 {
		window.samples = window.samples[firstCurrent:]
	}
}

func (window *ErrorWindow) Percent() float64 {
	return pct(window.errors, window.attempts)
}
