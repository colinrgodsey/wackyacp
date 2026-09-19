package agy

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The drain loop must keep polling while data keeps appearing, retry through
// transient poll errors, stop on the first clean-and-quiet read, abort when
// delivery becomes impossible, and respect the attempt budget.
func TestDrainFinal_StopsOnCleanQuiet(t *testing.T) {
	calls := 0
	poll := func(context.Context) (int, error, error) {
		calls++
		if calls == 1 {
			return 3, nil, nil // tail rows arrived
		}
		return 0, nil, nil // quiet
	}
	b := &Bridge{logf: func(string, ...any) {}}
	b.drainFinal(context.Background(), poll)
	if calls != 2 {
		t.Errorf("expected 2 polls (data, then quiet), got %d", calls)
	}
}

func TestDrainFinal_RetriesThroughPollErrors(t *testing.T) {
	calls := 0
	poll := func(context.Context) (int, error, error) {
		calls++
		switch calls {
		case 1, 2:
			return 0, errors.New("database is locked"), nil
		default:
			return 0, nil, nil // clean read after the lock cleared
		}
	}
	b := &Bridge{logf: func(string, ...any) {}}
	b.drainFinal(context.Background(), poll)
	if calls != 3 {
		t.Errorf("expected 3 polls (2 errors, then quiet), got %d", calls)
	}
}

func TestDrainFinal_EmitErrorAbortsImmediately(t *testing.T) {
	calls := 0
	poll := func(context.Context) (int, error, error) {
		calls++
		return 1, nil, errors.New("stream closed")
	}
	b := &Bridge{logf: func(string, ...any) {}}
	b.drainFinal(context.Background(), poll)
	if calls != 1 {
		t.Errorf("expected drain to stop after emit error, got %d calls", calls)
	}
}

func TestDrainFinal_AttemptBudget(t *testing.T) {
	calls := 0
	poll := func(context.Context) (int, error, error) {
		calls++
		return 1, nil, nil // perpetually produces data - budget must stop it
	}
	b := &Bridge{logf: func(string, ...any) {}}
	start := time.Now()
	b.drainFinal(context.Background(), poll)
	if calls != finalDrainAttempts {
		t.Errorf("expected %d polls, got %d", finalDrainAttempts, calls)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("drain ran %v; backoff should total ~%v", elapsed, finalDrainBackoff*(finalDrainAttempts-1))
	}
}

func TestDrainFinal_RespectsContextCancel(t *testing.T) {
	calls := 0
	poll := func(context.Context) (int, error, error) {
		calls++
		return 1, nil, nil // always data
	}
	b := &Bridge{logf: func(string, ...any) {}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	b.drainFinal(ctx, poll)
	if calls >= finalDrainAttempts {
		t.Errorf("expected context cancel to cut the drain short, got %d calls", calls)
	}
}
