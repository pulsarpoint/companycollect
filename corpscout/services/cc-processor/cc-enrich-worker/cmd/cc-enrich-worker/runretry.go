package main

import (
	"container/heap"
	"time"
)

// Retry policy for failed parts (spec 2026-07-14-part-retry-backoff-design.md): a failed part is
// requeued with exponential backoff instead of failing the run, and counts as Failed only after
// maxPartAttempts attempts (~1.5 h of waits — sized to the observed S3-coldness recovery horizon).
const maxPartAttempts = 8

// partBackoffBase is a variable, not a constant, so unit tests shrink minutes to microseconds.
var partBackoffBase = time.Minute

// partBackoffSteps are multiples of partBackoffBase applied after the n-th failed attempt.
var partBackoffSteps = []time.Duration{1, 2, 4, 8, 16, 30, 30}

// partBackoff returns how long a part waits after its n-th failed attempt (1-based).
func partBackoff(failedAttempts int) time.Duration {
	if failedAttempts < 1 {
		failedAttempts = 1
	}
	step := failedAttempts - 1
	if step >= len(partBackoffSteps) {
		step = len(partBackoffSteps) - 1
	}
	return partBackoffSteps[step] * partBackoffBase
}

// pendingPart is one schedulable part attempt. seq breaks eligibleAt ties FIFO so initial parts
// dispatch in their original order and a requeue never overtakes a fresh part scheduled at the
// same instant. The zero eligibleAt of a never-attempted part sorts before every retry time.
type pendingPart struct {
	part       uint32
	attempts   int       // failed attempts so far
	eligibleAt time.Time // zero for never-attempted parts: eligible immediately
	seq        int       // set by partQueue.add
}

// partQueue is a min-heap of pendingPart by (eligibleAt, seq). It is not safe for concurrent use;
// only the pool dispatcher touches it.
type partQueue struct {
	items []pendingPart
	seq   int
}

func (q *partQueue) Len() int { return len(q.items) }

func (q *partQueue) Less(i, j int) bool {
	if !q.items[i].eligibleAt.Equal(q.items[j].eligibleAt) {
		return q.items[i].eligibleAt.Before(q.items[j].eligibleAt)
	}
	return q.items[i].seq < q.items[j].seq
}

func (q *partQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

func (q *partQueue) Push(x any) { q.items = append(q.items, x.(pendingPart)) }

func (q *partQueue) Pop() any {
	old := q.items
	n := len(old)
	item := old[n-1]
	q.items = old[:n-1]
	return item
}

// add enqueues p with the next FIFO sequence number.
func (q *partQueue) add(p pendingPart) {
	p.seq = q.seq
	q.seq++
	heap.Push(q, p)
}

// peek returns the earliest-eligible part without removing it. Callers must check Len() > 0.
func (q *partQueue) peek() pendingPart { return q.items[0] }

// next removes and returns the earliest-eligible part. Callers must check Len() > 0.
func (q *partQueue) next() pendingPart { return heap.Pop(q).(pendingPart) }
