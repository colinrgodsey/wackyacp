package serve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/colinrgodsey/wackypub/pkg/stdio"
)

// reapGrace bounds how long a spawned stdio-serve child is given to exit after
// its stdin closes, before the reap goroutine force-kills it. A healthy
// stdio-serve exits on stdin EOF well inside this window; the grace only
// catches a wedged child so a failed dial cannot leak a process.
const reapGrace = 5 * time.Second

// Dialer is the gRPC dialer that makes the wackypub stdio-serve child
// reconnectable: every dial attempt (initial connect, transport drop, backoff
// cycle) spawns a fresh wackypub stdio-serve child, so a child death degrades
// to a transport disconnect that gRPC heals itself, instead of a dead conn
// the server is stuck on. The pattern is wackydiscord's ProcessDialer
// (wackydiscord PR #23); the serve mode and the bot share the same
// spawn-per-dial lifecycle.
//
// The child is tied to the serve's lifecycle context, NOT to the dial context:
// grpc-go's createTransport cancels the connect context the moment the dial
// succeeds, so a child spawned under the dial context would die immediately
// after every successful dial. If the dial context expires mid-spawn, the
// child just started is reaped before the error is returned, so failed dials
// leave no orphans.
type Dialer struct {
	mu         sync.Mutex
	bin        string
	wsDir      string
	lifecycle  context.Context
	activeCmd  *exec.Cmd
	activeConn *stdio.Conn
	lastCmd    *exec.Cmd // most recent spawn attempt (live or reaped); diagnostics and tests
	closed     bool
	lastErr    error
}

// NewDialer returns a dialer that spawns bin stdio-serve with CWD wsDir. The
// child is owned until Close or replaced by a later dial; the lifecycle
// context only refuses NEW spawns. What ends a running child is the pipe EOF,
// not context cancellation, by design.
func NewDialer(lifecycle context.Context, bin, wsDir string) *Dialer {
	if bin == "" {
		bin = "wackypub"
	}
	return &Dialer{bin: bin, wsDir: wsDir, lifecycle: lifecycle}
}

// Dial implements the grpc.WithContextDialer contract. It is serialized:
// pick_first dials one subchannel at a time, but the resolver-reset path can
// re-enter, and two concurrent children would leave one holding a live stdin
// pipe forever.
func (d *Dialer) Dial(ctx context.Context, target string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, errors.New("dialer is closed")
	}
	if d.lifecycle != nil && d.lifecycle.Err() != nil {
		return nil, d.lifecycle.Err()
	}

	// Tear down the previous child if its transport is still open. gRPC normally
	// closes its transport before re-dialing, but the reset path can dial again
	// while the old conn is live; that edge must not leak a child.
	if d.activeConn != nil {
		_ = d.activeConn.Close()
		d.activeConn = nil
		d.activeCmd = nil
	}

	cmd := exec.Command(d.bin, "stdio-serve")
	cmd.Dir = d.wsDir
	conn, err := stdio.DialCommand(ctx, cmd, reapGrace)
	if err != nil {
		d.lastCmd = cmd
		d.lastErr = err
		return nil, fmt.Errorf("spawning %s stdio-serve in %s: %w", d.bin, d.wsDir, err)
	}
	if err := ctx.Err(); err != nil {
		// The dial deadline expired mid-spawn. The child is deliberately NOT
		// tied to ctx (see type doc), so reap it explicitly: no orphan on a
		// failed dial.
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		d.lastCmd = cmd
		d.lastErr = err
		return nil, err
	}
	d.activeCmd = cmd
	d.activeConn = conn
	d.lastCmd = cmd
	d.lastErr = nil
	return conn, nil
}

// childAlive reports whether the active child still exists in the process
// table, via kill(pid, 0): ESRCH means the kernel has reaped it. A missing
// active cmd (no completed dial) counts as alive: callers should only act on
// a confirmed dead child. cmd.ProcessState is deliberately NOT read here: it
// is written by cmd.Wait in the reap goroutine, and reading it from this one
// would race.
func (d *Dialer) childAlive() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	cmd := d.activeCmd
	if cmd == nil || cmd.Process == nil {
		return true
	}
	return syscall.Kill(cmd.Process.Pid, 0) != syscall.ESRCH
}

// Close refuses new spawns and tears down the live child. Idempotent.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var err error
	if d.activeConn != nil {
		err = d.activeConn.Close()
	}
	d.activeConn = nil
	d.activeCmd = nil
	d.lastCmd = nil
	return err
}
