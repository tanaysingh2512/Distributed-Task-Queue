// Package retry computes when a failed task should become eligible for
// redelivery. It implements full-jitter exponential backoff (as described
// in the AWS Architecture Blog's "Exponential Backoff And Jitter" post)
// to avoid thundering-herd retries against a struggling downstream.
package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy configures backoff bounds for a queue. Zero-value Policy is not
// usable; construct via NewPolicy.
type Policy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

func NewPolicy(base, max time.Duration, maxAttempts int) Policy {
	return Policy{BaseDelay: base, MaxDelay: max, MaxAttempts: maxAttempts}
}

// Default mirrors common production defaults: 500ms base, 2 minute cap,
// 8 attempts before dead-lettering.
func Default() Policy {
	return NewPolicy(500*time.Millisecond, 2*time.Minute, 8)
}

// Next returns the delay to wait before the task identified by `attempt`
// (1-indexed, i.e. the attempt number that just failed) becomes eligible
// for redelivery again. Uses full jitter: a uniform random delay in
// [0, cappedExponential] rather than the exponential value itself, which
// empirically reduces contention better than equal- or decorrelated-jitter
// variants for this workload shape.
func (p Policy) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := float64(p.BaseDelay) * math.Pow(2, float64(attempt-1))
	capped := math.Min(exp, float64(p.MaxDelay))
	if capped <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(capped) + 1))
}

// ShouldDeadLetter reports whether a task that has just failed its
// `attempt`-th attempt has exhausted its retry budget.
func (p Policy) ShouldDeadLetter(attempt int) bool {
	return attempt >= p.MaxAttempts
}
