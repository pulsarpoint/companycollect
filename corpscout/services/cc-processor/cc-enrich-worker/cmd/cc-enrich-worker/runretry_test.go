package main

import (
	"testing"
	"time"
)

func TestPartBackoffSchedule(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 1 * time.Minute},
		{2, 2 * time.Minute},
		{3, 4 * time.Minute},
		{4, 8 * time.Minute},
		{5, 16 * time.Minute},
		{6, 30 * time.Minute},
		{7, 30 * time.Minute},
		{99, 30 * time.Minute},
	}
	for _, c := range cases {
		if got := partBackoff(c.failures); got != c.want {
			t.Errorf("partBackoff(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestPartQueueOrdersByEligibleAtThenFIFO(t *testing.T) {
	q := &partQueue{}
	now := time.Now()
	q.add(pendingPart{part: 10}) // never attempted: zero eligibleAt, eligible immediately
	q.add(pendingPart{part: 11}) // same eligibleAt as 10 -> FIFO by insertion
	q.add(pendingPart{part: 12, attempts: 1, eligibleAt: now.Add(time.Hour)})
	q.add(pendingPart{part: 13, attempts: 1, eligibleAt: now.Add(time.Minute)})

	var got []uint32
	for q.Len() > 0 {
		got = append(got, q.next().part)
	}
	want := []uint32{10, 11, 13, 12}
	if !reflectEqualU32(got, want) {
		t.Errorf("pop order = %v, want %v", got, want)
	}
}
