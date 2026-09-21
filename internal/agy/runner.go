package agy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// defaultPrintTimeout bounds how long agy may take on one prompt. agy parses the
// value itself, so it is carried as a string in its own CLI's notation.
const defaultPrintTimeout = "20m"

const (
	maxStderrTailBytes = 8 * 1024
	defaultPoll        = 100 * time.Millisecond
	finalPollTimeout   = 5 * time.Second
	finalDrainAttempts = 5
	finalDrainBackoff  = 150 * time.Millisecond
	killGrace          = 5 * time.Second
)

// Stop reasons surfaced in a session/prompt result.
const (
	StopReasonEndTurn   = "end_turn"
	StopReasonCancelled = "cancelled"
)

// JSON-RPC error codes used by this bridge. agy-side failures use the
// implementation-defined -32000 range, matching the reference bridge.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
	CodeServerFailure  = -32000
)

// turnError carries a JSON-RPC error code alongside the underlying failure so
// the prompt handler can report agy problems with the right code.
type turnError struct {
	Code int
	Err  error
}

func (e *turnError) Error() string { return e.Err.Error() }
func (e *turnError) Unwrap() error { return e.Err }

// activeTurn is an in-flight prompt that session/cancel can interrupt.
type activeTurn struct {
	sessionID string
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

// WasCancelled reports whether the client asked for this turn to be cancelled,
// which is what distinguishes an interrupted turn from a failed one.
func (t *activeTurn) WasCancelled() bool { return t.cancelled.Load() }

// turnOutcome is what one prompt turn produced.
type turnOutcome struct {
	ConversationID string
	LastStepIdx    int64
	StopReason     string
	UpdatesEmitted int
}

// agentProcess is the running agy child a turn waits on.
type agentProcess interface {
	// Wait blocks until the child exits. A child killed by cancellation returns
	// an error; callers must consult the turn's cancellation state to tell
	// cancellation apart from failure.
	Wait() error
	// StderrTail returns the tail of the child's stderr for error messages.
	StderrTail() string
}

// agentStarter spawns an agy child bound to ctx. Replacing it in tests lets the
// turn loop be exercised without a real agent; the process-stub integration
// tests cover the real starter.
type agentStarter func(ctx context.Context, argv []string) (agentProcess, error)

// processAgent is the real agentProcess backed by exec.Cmd.
type processAgent struct {
	cmd  *exec.Cmd
	tail *tailBuffer
}

func (a *processAgent) Wait() error {
	if err := a.cmd.Wait(); err != nil {
		return fmt.Errorf("agy process: %w", err)
	}
	return nil
}

func (a *processAgent) StderrTail() string { return a.tail.String() }

// newProcessStarter spawns agy in its own session so a cancelled turn can take
// the whole process group with it, escalating to SIGKILL after killGrace.
func newProcessStarter(cfg Config) agentStarter {
	return func(ctx context.Context, argv []string) (agentProcess, error) {
		if len(argv) == 0 {
			return nil, errors.New("cannot start agy with empty argv")
		}
		tail := newTailBuffer(maxStderrTailBytes)
		cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
		cmd.Dir = cfg.WorkingDir
		cmd.Stdin = nil
		cmd.Stdout = io.Discard
		echo := cfg.Stderr
		if echo == nil {
			echo = io.Discard
		}
		cmd.Stderr = io.MultiWriter(echo, tail)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Cancel = func() error { return signalProcessGroup(cmd, syscall.SIGTERM) }
		cmd.WaitDelay = killGrace

		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("starting %s: %w", argv[0], err)
		}
		return &processAgent{cmd: cmd, tail: tail}, nil
	}
}

func signalProcessGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}
	if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil {
		return syscall.Kill(-pgid, sig)
	}
	return cmd.Process.Signal(sig)
}

// tailBuffer keeps the last N bytes written to it, so a chatty agent cannot grow
// the bridge's memory without bound while still leaving context for errors.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(max int) *tailBuffer {
	if max <= 0 {
		max = maxStderrTailBytes
	}
	return &tailBuffer{max: max}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.max:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// agentArgs builds the agy command line for one turn. argv[0] must be the
// agy executable: the runner hands argv straight to exec.CommandContext, which
// treats the first element as the binary to run.
func (b *Bridge) agentArgs(sess StoredSession, promptText string) []string {
	args := []string{b.cfg.AgyBin, "--add-dir", b.cfg.WorkingDir}
	args = append(args, withPrintTimeout(b.cfg.ExtraArgs, b.cfg.PrintTimeout)...)
	args = withPermissionMode(args, b.cfg.PermissionMode)
	if sess.ConversationID != "" {
		args = append(args, "--conversation", sess.ConversationID)
	}
	if model := b.models.CanonicalID(sess.ModelID); model != "" {
		args = append(args, "--model", model)
	}
	return append(args, "-p", promptText)
}

// skipPermissionsFlag is agy's own auto-approve switch, and the only way to get
// approve posture: headless agy answers tool permission requests itself, denying
// them, so the bridge can never see one to forward.
const skipPermissionsFlag = "--dangerously-skip-permissions"

// withPermissionMode appends agy's auto-approve flag in approve posture, unless the
// operator already passed it through --extra-args. Deny posture appends nothing, so
// agy keeps its own default posture.
func withPermissionMode(args []string, mode string) []string {
	if mode != PermissionApprove {
		return args
	}
	for _, arg := range args {
		if arg == skipPermissionsFlag || strings.HasPrefix(arg, skipPermissionsFlag+"=") {
			return args
		}
	}
	out := make([]string, 0, len(args)+1)
	out = append(out, args...)
	return append(out, skipPermissionsFlag)
}

// withPrintTimeout appends the default print timeout unless the caller already
// passed one. A flag that merely shares the prefix must not count as one.
func withPrintTimeout(extra []string, timeout string) []string {
	if timeout == "" {
		timeout = defaultPrintTimeout
	}
	for _, arg := range extra {
		if arg == "--print-timeout" || strings.HasPrefix(arg, "--print-timeout=") {
			return extra
		}
	}
	out := make([]string, 0, len(extra)+2)
	out = append(out, extra...)
	return append(out, "--print-timeout", timeout)
}

// runTurn executes one agy prompt turn: spawn the child, stream updates as the
// conversation database grows, then classify the exit so a swallowed backend
// failure is reported instead of an empty success.
func (b *Bridge) runTurn(ctx context.Context, turn *activeTurn, promptText string, sess StoredSession, emit func(Update) error) (turnOutcome, error) {
	outcome := turnOutcome{ConversationID: sess.ConversationID, LastStepIdx: sess.LastStepIdx}

	// Snapshot before spawning: both the conversation listing (to identify the
	// conversation this turn creates) and the agent log sizes (to scope the
	// swallowed-error scan to bytes appended during this turn).
	var conversationSnapshot map[string]bool
	if sess.ConversationID == "" {
		snapshot, err := b.transcript.Snapshot()
		if err != nil {
			b.logf("%v", err)
		}
		conversationSnapshot = snapshot
	}
	logPre, err := snapshotLogs(b.cfg.LogDir)
	if err != nil {
		b.logf("%v", err)
	}
	spawned := time.Now()

	childCtx, cancelChild := context.WithCancel(ctx)
	defer cancelChild()

	argv := b.agentArgs(sess, promptText)
	if childCtx.Err() != nil {
		// session/cancel landed between turn registration and spawn: exec.CommandContext only
		// applies Cancel after the child is forked, so spawning anyway would fork agy just to
		// SIGTERM it. Skip the fork and report the cancellation the turn already carries.
		outcome.StopReason = StopReasonCancelled
		return outcome, nil
	}
	b.logf("spawning %s", strings.Join(argvForLog(argv), " "))
	proc, err := b.starter(childCtx, argv)
	if err != nil {
		return outcome, &turnError{Code: CodeServerFailure, Err: fmt.Errorf("failed to run agy: %w", err)}
	}

	poller := NewTurnPoller(b.transcript, sess.ConversationID, sess.LastStepIdx, conversationSnapshot, b.cfg.ShowNarration, b.logf)
	if b.fileLogf != nil {
		poller.SetAdvisoryLog(b.fileLogf)
	}

	var (
		emitMu  sync.Mutex
		emitErr error
	)
	pollOnce := func(pollCtx context.Context) (emitted int, pollErr error, emitErr error) {
		// Poll logs its own advisory failures (transient lock, schema drift), so
		// only the emitted updates matter here.
		updates, err := poller.Poll(pollCtx)
		if err != nil {
			return 0, err, nil
		}
		for _, update := range updates {
			if err := emit(update); err != nil {
				emitMu.Lock()
				if emitErr == nil {
					emitErr = err
				}
				defer emitMu.Unlock()
				cancelChild()
				return emitted, nil, emitErr
			}
			outcome.UpdatesEmitted++
			emitted++
		}
		return emitted, nil, nil
	}

	// 1. Poll: stream updates while the child runs.
	stopPoller, pollerDone := b.startPoller(childCtx, pollOnce)

	// 2. Wait: wait for the child process to exit, then tear down the poller ticker.
	waitErr := proc.Wait()
	stopPoller()
	<-pollerDone

	// 3. Drain: drain the tail of the turn until quiet or budget exhausted.
	b.drainTurnFinal(context.Background(), pollOnce)

	outcome.ConversationID = poller.ConversationID()
	outcome.LastStepIdx = poller.LastIdx()

	// 4. Deliver: check for delivery errors.
	emitMu.Lock()
	deliverErr := emitErr
	emitMu.Unlock()

	// 5. Map: evaluate outcome and return.
	return b.mapOutcome(ctx, turn, poller, outcome, waitErr, proc.StderrTail(), deliverErr, logPre, spawned)
}

// startPoller launches a goroutine that periodically polls for updates while the
// agent process runs. It returns a stop function to terminate polling and a done
// channel that closes when the polling goroutine has exited.
func (b *Bridge) startPoller(ctx context.Context, pollOnce func(context.Context) (int, error, error)) (func(), <-chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(b.pollInterval())
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Mid-turn poll/emit errors are intentionally dropped here: the
				// next tick retries the same data, and the post-exit drainFinal
				// loop is the authoritative retry for anything still pending.
				_, _, _ = pollOnce(ctx)
			}
		}
	}()
	var stopOnce sync.Once
	stopFunc := func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}
	return stopFunc, done
}

// drainTurnFinal polls after the agy process has exited until a fully clean read
// produces no new data, bounded by finalDrainAttempts and finalPollTimeout.
// A fresh context is used deliberately when called by runTurn: the turn
// context may be cancelled, and this read is bounded by the read-only open
// plus busy_timeout.
func (b *Bridge) drainTurnFinal(ctx context.Context, pollOnce func(context.Context) (int, error, error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	finalCtx, finalCancel := context.WithTimeout(ctx, finalPollTimeout)
	defer finalCancel()
	b.drainFinal(finalCtx, pollOnce)
}

// turnPollerOutcome abstracts the subset of TurnPoller methods needed by mapOutcome.
type turnPollerOutcome interface {
	AdvisoryCounts() (noText int, noName int)
	HadUpdates() bool
	SchemaMissing() bool
}

// mapOutcome evaluates the turn's outcome after the agent process and final drain have
// completed, translating cancellation, process failure, delivery errors, or clean completion
// into a turnOutcome and error.
func (b *Bridge) mapOutcome(
	ctx context.Context,
	turn *activeTurn,
	poller turnPollerOutcome,
	outcome turnOutcome,
	waitErr error,
	stderrTail string,
	deliverErr error,
	logPre logSnapshot,
	spawned time.Time,
) (turnOutcome, error) {
	if deliverErr != nil {
		return outcome, fmt.Errorf("delivering session/update: %w", deliverErr)
	}

	switch {
	case turn != nil && turn.WasCancelled():
		outcome.StopReason = StopReasonCancelled
	case ctx != nil && ctx.Err() != nil:
		return outcome, fmt.Errorf("turn aborted: %w", ctx.Err())
	case waitErr != nil:
		return outcome, &turnError{Code: CodeServerFailure, Err: agentFailureError(waitErr, stderrTail)}
	default:
		logf := b.logf
		if logf == nil {
			logf = func(string, ...any) {}
		}
		hadUpdates := false
		if poller != nil {
			hadUpdates = poller.HadUpdates()
			noText, noName := poller.AdvisoryCounts()
			if noText > 0 || noName > 0 {
				switch {
				case noText > 0 && noName > 0:
					logf("turn advisories: %d step(s) had no extractable text, %d tool-shaped step(s) lacked names", noText, noName)
				case noText > 0:
					logf("turn advisories: %d step(s) had no extractable text (agy field 20.1 missing)", noText)
				case noName > 0:
					logf("turn advisories: %d tool-shaped step(s) lacked names (agy field 5.4 missing)", noName)
				}
			}

			// A clean exit that produced nothing is almost always agy hiding a backend
			// failure; only a log signature can tell that from a genuinely empty answer.
			if !hadUpdates {
				switch {
				case outcome.ConversationID == "":
					logf("agy exited without creating a conversation in %s: this turn had no output to stream", b.cfg.ConversationsDir)
				case poller.SchemaMissing():
					logf("conversation %s never gained a steps table: agy changed its schema, so nothing could be streamed", outcome.ConversationID)
				}
			}
		} else {
			// poller is only nil in synthetic unit test harnesses; in production runTurn it is
			// always populated. When nil, update detection falls back to outcome.UpdatesEmitted.
			hadUpdates = outcome.UpdatesEmitted > 0
		}

		if !hadUpdates {
			if msg, ok := detectSwallowedError(b.cfg.LogDir, logPre, spawned); ok {
				return outcome, &turnError{Code: CodeInternalError, Err: errors.New(msg)}
			}
		}
		outcome.StopReason = StopReasonEndTurn
	}
	return outcome, nil
}

// argvForLog renders the command line without the prompt text, which belongs in a
// log file no more than the user's conversation does.
func argvForLog(argv []string) []string {
	for i, arg := range argv {
		if arg == "-p" && i+1 < len(argv) {
			return append(append([]string{}, argv[:i+1]...), "<prompt>")
		}
	}
	return argv
}

// agentFailureError renders the message for a failed agy exit, preferring the
// agent's own stderr over the bare exit status.
func agentFailureError(waitErr error, stderrTail string) error {
	if tail := strings.TrimSpace(stderrTail); tail != "" {
		return fmt.Errorf("agy failed: %s", tail)
	}
	if waitErr != nil {
		return fmt.Errorf("agy failed: %w", waitErr)
	}
	return errors.New("agy failed without a message")
}

// drainFinal polls after the agy process has exited until a fully clean read
// produces no new data (or the attempts/deadline budget is spent). pollOnce
// reports three ways: emitted count, a poll-side error (retryable - the data
// is still on disk), and an emit-side error (delivery is impossible, so
// draining further is pointless). Without this loop a single failed read
// would silently defer the turn's tail to the next turn's stream.
func (b *Bridge) drainFinal(ctx context.Context, pollOnce func(context.Context) (int, error, error)) {
	for attempt := 1; attempt <= finalDrainAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		emitted, pollErr, emitErr := pollOnce(ctx)
		if emitErr != nil {
			return
		}
		if pollErr != nil {
			b.logf("final drain poll %d/%d failed: %v", attempt, finalDrainAttempts, pollErr)
			if !sleepFinalDrain(ctx) {
				return
			}
			continue
		}
		if emitted == 0 {
			return // clean and quiet: nothing further to drain
		}
		// Data moved on this pass; check once more for anything committed
		// between reads before declaring the tail complete.
		if !sleepFinalDrain(ctx) {
			return
		}
	}
	b.logf("final drain budget spent after %d attempts; tail (if any) will surface next turn", finalDrainAttempts)
}

func sleepFinalDrain(ctx context.Context) bool {
	t := time.NewTimer(finalDrainBackoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
