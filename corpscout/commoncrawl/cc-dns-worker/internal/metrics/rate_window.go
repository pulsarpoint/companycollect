package metrics

import "time"

type rateSample struct {
	at      time.Time
	count   int64
	elapsed time.Duration
}

// RateWindow reports a count-per-second rate across the retained reporting intervals.
type RateWindow struct {
	duration time.Duration
	samples  []rateSample
	count    int64
	elapsed  time.Duration
}

func NewRateWindow(duration time.Duration) *RateWindow {
	return &RateWindow{duration: duration}
}

func (window *RateWindow) Add(at time.Time, count int64, elapsed time.Duration) {
	if count < 0 {
		count = 0
	}
	if elapsed < 0 {
		elapsed = 0
	}
	sample := rateSample{at: at, count: count, elapsed: elapsed}
	window.samples = append(window.samples, sample)
	window.count += count
	window.elapsed += elapsed

	cutoff := at.Add(-window.duration)
	firstCurrent := 0
	for firstCurrent < len(window.samples) && !window.samples[firstCurrent].at.After(cutoff) {
		window.count -= window.samples[firstCurrent].count
		window.elapsed -= window.samples[firstCurrent].elapsed
		firstCurrent++
	}
	if firstCurrent > 0 {
		window.samples = window.samples[firstCurrent:]
	}
}

func (window *RateWindow) PerSecond() float64 {
	seconds := window.elapsed.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(window.count) / seconds
}
