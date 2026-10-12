package retry

import (
	"testing"
	"time"
)

func TestNextIsBoundedByMaxDelay(t *testing.T) {
	p := NewPolicy(500*time.Millisecond, 2*time.Minute, 8)

	for attempt := 1; attempt <= 20; attempt++ {
		d := p.Next(attempt)
		if d < 0 {
			t.Fatalf("attempt %d: negative delay %v", attempt, d)
		}
		if d > p.MaxDelay {
			t.Fatalf("attempt %d: delay %v exceeds MaxDelay %v", attempt, d, p.MaxDelay)
		}
	}
}

func TestNextGrowsWithAttemptOnAverage(t *testing.T) {
	p := NewPolicy(100*time.Millisecond, 10*time.Second, 10)

	avg := func(attempt int, samples int) time.Duration {
		var total time.Duration
		for i := 0; i < samples; i++ {
			total += p.Next(attempt)
		}
		return total / time.Duration(samples)
	}

	early := avg(1, 500)
	later := avg(5, 500)

	if later <= early {
		t.Fatalf("expected average backoff to grow with attempt number: attempt1=%v attempt5=%v", early, later)
	}
}

func TestShouldDeadLetter(t *testing.T) {
	p := NewPolicy(100*time.Millisecond, 10*time.Second, 3)

	cases := []struct {
		attempt int
		want    bool
	}{
		{1, false},
		{2, false},
		{3, true},
		{4, true},
	}

	for _, c := range cases {
		if got := p.ShouldDeadLetter(c.attempt); got != c.want {
			t.Errorf("ShouldDeadLetter(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}
