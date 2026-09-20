package agy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type stubPollerOutcome struct {
	noText        int
	noName        int
	hadUpdates    bool
	schemaMissing bool
}

func (s *stubPollerOutcome) AdvisoryCounts() (int, int) { return s.noText, s.noName }
func (s *stubPollerOutcome) HadUpdates() bool           { return s.hadUpdates }
func (s *stubPollerOutcome) SchemaMissing() bool        { return s.schemaMissing }

func TestStartPoller_TicksAndStops(t *testing.T) {
	var count atomic.Int32
	poll := func(ctx context.Context) (int, error, error) {
		count.Add(1)
		return 1, nil, nil
	}
	b := &Bridge{
		cfg:  Config{PollInterval: 10 * time.Millisecond},
		logf: func(string, ...any) {},
	}
	stop, done := b.startPoller(context.Background(), poll)

	// Wait until at least 2 ticks have occurred.
	deadline := time.Now().Add(500 * time.Millisecond)
	for count.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if count.Load() < 2 {
		t.Fatalf("expected at least 2 ticks, got %d", count.Load())
	}

	stop()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("done channel did not close after stop")
	}

	// Verify idempotency of stop
	stop()

	// Verify no more polls happen after done
	current := count.Load()
	time.Sleep(30 * time.Millisecond)
	if after := count.Load(); after > current+1 {
		t.Errorf("poll count continued increasing after stop: was %d, now %d", current, after)
	}
}

func TestStartPoller_StopsOnContextCancel(t *testing.T) {
	var count atomic.Int32
	poll := func(ctx context.Context) (int, error, error) {
		count.Add(1)
		return 0, nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bridge{
		cfg:  Config{PollInterval: 10 * time.Millisecond},
		logf: func(string, ...any) {},
	}
	_, done := b.startPoller(ctx, poll)

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("done channel did not close after context cancel")
	}
}

func TestStartPoller_DropsMidTurnErrors(t *testing.T) {
	var count atomic.Int32
	poll := func(ctx context.Context) (int, error, error) {
		c := count.Add(1)
		if c%2 == 1 {
			return 0, errors.New("transient poll lock"), nil
		}
		return 0, nil, errors.New("emit error")
	}
	b := &Bridge{
		cfg:  Config{PollInterval: 10 * time.Millisecond},
		logf: func(string, ...any) {},
	}
	stop, done := b.startPoller(context.Background(), poll)

	deadline := time.Now().Add(500 * time.Millisecond)
	for count.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	<-done

	if count.Load() < 4 {
		t.Errorf("expected poller to keep ticking through errors, got %d ticks", count.Load())
	}
}

func TestDrainTurnFinal_AppliesDeadlineBound(t *testing.T) {
	var seenDeadline time.Time
	var hasDeadline bool
	poll := func(ctx context.Context) (int, error, error) {
		dl, ok := ctx.Deadline()
		hasDeadline = ok
		seenDeadline = dl
		return 0, nil, nil // clean quiet
	}

	b := &Bridge{logf: func(string, ...any) {}}
	before := time.Now()
	b.drainTurnFinal(context.Background(), poll)

	if !hasDeadline {
		t.Fatal("expected drainTurnFinal context to have a deadline")
	}
	expectedMin := before.Add(finalPollTimeout - 500*time.Millisecond)
	expectedMax := time.Now().Add(finalPollTimeout + 500*time.Millisecond)
	if seenDeadline.Before(expectedMin) || seenDeadline.After(expectedMax) {
		t.Errorf("expected deadline around %v..%v, got %v", expectedMin, expectedMax, seenDeadline)
	}
}

func TestDrainTurnFinal_ExitsOnDeadlineExpiration(t *testing.T) {
	b := &Bridge{logf: func(string, ...any) {}}
	// Pass an already expired context to verify immediate cutoff by the bound.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond) // ensure deadline passed

	calls := 0
	poll := func(ctx context.Context) (int, error, error) {
		calls++
		return 1, nil, nil
	}
	b.drainTurnFinal(ctx, poll)
	if calls != 0 {
		t.Errorf("expected 0 calls when context deadline is already expired, got %d", calls)
	}
}

func TestDrainTurnFinal_NormalDrainStopsOnCleanQuiet(t *testing.T) {
	b := &Bridge{logf: func(string, ...any) {}}
	calls := 0
	poll := func(ctx context.Context) (int, error, error) {
		calls++
		if calls == 1 {
			return 2, nil, nil // some tail rows arrived
		}
		return 0, nil, nil // quiet
	}
	b.drainTurnFinal(nil, poll) // nil context defaults to background
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestMapOutcome(t *testing.T) {
	canceledTurn := &activeTurn{}
	canceledTurn.cancelled.Store(true)

	uncanceledTurn := &activeTurn{}

	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name           string
		ctx            context.Context
		turn           *activeTurn
		poller         turnPollerOutcome
		outcome        turnOutcome
		waitErr        error
		stderrTail     string
		deliverErr     error
		wantStopReason string
		wantErrSub     string
		wantErrorCode  int
	}{
		{
			name:       "deliver error takes precedence",
			turn:       canceledTurn,
			deliverErr: errors.New("client connection reset"),
			wantErrSub: "delivering session/update: client connection reset",
		},
		{
			name:           "turn was cancelled",
			turn:           canceledTurn,
			wantStopReason: StopReasonCancelled,
		},
		{
			name:       "context was cancelled without turn.WasCancelled",
			ctx:        ctxCanceled,
			turn:       uncanceledTurn,
			wantErrSub: "turn aborted: context canceled",
		},
		{
			name:          "waitErr with stderr tail reports agent failure",
			turn:          uncanceledTurn,
			waitErr:       errors.New("exit status 1"),
			stderrTail:    "model quota exceeded",
			wantErrSub:    "agy failed: model quota exceeded",
			wantErrorCode: CodeServerFailure,
		},
		{
			name:          "waitErr without stderr tail uses waitErr",
			turn:          uncanceledTurn,
			waitErr:       errors.New("exit status 2"),
			wantErrSub:    "agy failed: exit status 2",
			wantErrorCode: CodeServerFailure,
		},
		{
			name:           "clean exit with updates",
			turn:           uncanceledTurn,
			poller:         &stubPollerOutcome{hadUpdates: true},
			wantStopReason: StopReasonEndTurn,
		},
		{
			name:           "clean exit with advisories",
			turn:           uncanceledTurn,
			poller:         &stubPollerOutcome{noText: 2, noName: 1, hadUpdates: true},
			wantStopReason: StopReasonEndTurn,
		},
		{
			name:           "clean exit without updates and no swallowed error",
			turn:           uncanceledTurn,
			poller:         &stubPollerOutcome{hadUpdates: false},
			wantStopReason: StopReasonEndTurn,
		},
		{
			name:           "nil poller and nil turn edge case",
			wantStopReason: StopReasonEndTurn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bridge{
				cfg:  Config{LogDir: t.TempDir(), ConversationsDir: t.TempDir()},
				logf: func(string, ...any) {},
			}
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			gotOutcome, err := b.mapOutcome(
				ctx,
				tt.turn,
				tt.poller,
				tt.outcome,
				tt.waitErr,
				tt.stderrTail,
				tt.deliverErr,
				logSnapshot{},
				time.Now(),
			)

			if tt.wantErrSub != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrSub)
				}
				if !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrSub)
				}
				if tt.wantErrorCode != 0 {
					var te *turnError
					if !errors.As(err, &te) {
						t.Fatalf("expected *turnError, got %T: %v", err, err)
					}
					if te.Code != tt.wantErrorCode {
						t.Errorf("turnError code = %d, want %d", te.Code, tt.wantErrorCode)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotOutcome.StopReason != tt.wantStopReason {
				t.Errorf("StopReason = %q, want %q", gotOutcome.StopReason, tt.wantStopReason)
			}
		})
	}
}

func TestMapOutcome_LogsAdvisories(t *testing.T) {
	tests := []struct {
		name    string
		noText  int
		noName  int
		wantLog string
	}{
		{
			name:    "both advisories",
			noText:  3,
			noName:  2,
			wantLog: "turn advisories: 3 step(s) had no extractable text, 2 tool-shaped step(s) lacked names",
		},
		{
			name:    "noText only",
			noText:  4,
			noName:  0,
			wantLog: "turn advisories: 4 step(s) had no extractable text (agy field 20.1 missing)",
		},
		{
			name:    "noName only",
			noText:  0,
			noName:  5,
			wantLog: "turn advisories: 5 tool-shaped step(s) lacked names (agy field 5.4 missing)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logged []string
			b := &Bridge{
				logf: func(format string, args ...any) {
					logged = append(logged, fmt.Sprintf(format, args...))
				},
			}
			poller := &stubPollerOutcome{noText: tt.noText, noName: tt.noName, hadUpdates: true}
			_, err := b.mapOutcome(context.Background(), nil, poller, turnOutcome{}, nil, "", nil, logSnapshot{}, time.Now())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			found := false
			for _, line := range logged {
				if line == tt.wantLog {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected log %q, got logged: %v", tt.wantLog, logged)
			}
		})
	}
}

func TestMapOutcome_LogsMissingConversationAndSchema(t *testing.T) {
	var logged []string
	b := &Bridge{
		cfg: Config{ConversationsDir: "/test/convs"},
		logf: func(format string, args ...any) {
			logged = append(logged, fmt.Sprintf(format, args...))
		},
	}

	// ConversationID empty
	poller := &stubPollerOutcome{hadUpdates: false}
	outcome := turnOutcome{ConversationID: ""}
	_, err := b.mapOutcome(context.Background(), nil, poller, outcome, nil, "", nil, logSnapshot{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundConv := false
	for _, l := range logged {
		if strings.Contains(l, "agy exited without creating a conversation in /test/convs") {
			foundConv = true
		}
	}
	if !foundConv {
		t.Errorf("expected missing conversation log, got %v", logged)
	}

	// Schema missing
	logged = nil
	poller = &stubPollerOutcome{hadUpdates: false, schemaMissing: true}
	outcome = turnOutcome{ConversationID: "conv-123"}
	_, err = b.mapOutcome(context.Background(), nil, poller, outcome, nil, "", nil, logSnapshot{}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundSchema := false
	for _, l := range logged {
		if strings.Contains(l, "conversation conv-123 never gained a steps table") {
			foundSchema = true
		}
	}
	if !foundSchema {
		t.Errorf("expected schema missing log, got %v", logged)
	}
}
