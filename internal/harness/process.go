package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ErrHarnessNotFound indicates the harness command executable does not exist on PATH.
var ErrHarnessNotFound = errors.New("harness command not found")

// Config specifies the configuration for launching a harness subprocess.
type Config struct {
	Command     string
	Args        []string
	AgentFolder string
	Stderr      io.Writer
	WaitDelay   time.Duration
}

// Process wraps the running harness subprocess.
type Process struct {
	cmd       *exec.Cmd
	ctx       context.Context
	cancel    context.CancelFunc
	waitDelay time.Duration
	Stdin     io.WriteCloser
	Stdout    io.ReadCloser
	closeOnce sync.Once
	closeErr  error
}

// Resolve looks up the harness command on PATH and fails fast if not found.
func Resolve(command string) (string, error) {
	if command == "" {
		return "", fmt.Errorf("%w: command cannot be empty", ErrHarnessNotFound)
	}
	binPath, err := exec.LookPath(command)
	if err != nil {
		return "", fmt.Errorf("%w: %q: %w", ErrHarnessNotFound, command, err)
	}
	return binPath, nil
}

// Start launches the harness subprocess with process-group isolation and cancellation escalation.
func Start(ctx context.Context, cfg Config) (*Process, error) {
	binPath, err := Resolve(cfg.Command)
	if err != nil {
		return nil, err
	}

	procCtx, procCancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(procCtx, binPath, cfg.Args...)
	if cfg.AgentFolder != "" {
		cmd.Dir = cfg.AgentFolder
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Go 1.20+ process-group termination: SIGTERM -> WaitDelay -> SIGKILL
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		pgid, err := syscall.Getpgid(cmd.Process.Pid)
		if err == nil {
			return syscall.Kill(-pgid, syscall.SIGTERM)
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}

	waitDelay := cfg.WaitDelay
	if waitDelay <= 0 {
		waitDelay = 5 * time.Second
	}
	cmd.WaitDelay = waitDelay

	if cfg.Stderr != nil {
		cmd.Stderr = cfg.Stderr
	} else {
		cmd.Stderr = os.Stderr
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		procCancel()
		return nil, fmt.Errorf("creating harness stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		procCancel()
		_ = stdin.Close()
		return nil, fmt.Errorf("creating harness stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		procCancel()
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("starting harness process %s: %w", cfg.Command, err)
	}

	return &Process{
		cmd:       cmd,
		ctx:       procCtx,
		cancel:    procCancel,
		waitDelay: waitDelay,
		Stdin:     stdin,
		Stdout:    stdout,
	}, nil
}

// Close closes the stdio pipes and waits for the process to exit.
// It unconditionally escalates termination (SIGTERM to -pgid, then SIGKILL after WaitDelay)
// if the harness does not exit on stdin EOF within a brief grace period.
func (p *Process) Close() error {
	p.closeOnce.Do(func() {
		var stdinErr, stdoutErr error
		if p.Stdin != nil {
			stdinErr = p.Stdin.Close()
		}
		if p.Stdout != nil {
			stdoutErr = p.Stdout.Close()
		}

		waitDone := make(chan error, 1)
		go func() {
			waitDone <- p.cmd.Wait()
		}()

		waitDelay := p.waitDelay
		if waitDelay <= 0 {
			waitDelay = 5 * time.Second
		}

		var waitErr error
		if p.ctx.Err() != nil {
			// Context already canceled: trigger termination immediately
			if p.cmd.Cancel != nil {
				_ = p.cmd.Cancel()
			}
			if p.cancel != nil {
				p.cancel()
			}
			select {
			case waitErr = <-waitDone:
			case <-time.After(waitDelay):
				if p.cmd.Process != nil {
					pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
					if err == nil {
						_ = syscall.Kill(-pgid, syscall.SIGKILL)
					} else {
						_ = p.cmd.Process.Kill()
					}
				}
				waitErr = <-waitDone
			}
		} else {
			// Normal EOF path: allow brief grace period (50ms) for cooperative exit on EOF
			select {
			case waitErr = <-waitDone:
				// Harness exited cleanly on EOF
			case <-time.After(50 * time.Millisecond):
				// Harness ignored stdin EOF: escalate to SIGTERM(-pgid)
				if p.cmd.Cancel != nil {
					_ = p.cmd.Cancel()
				}
				if p.cancel != nil {
					p.cancel()
				}
				select {
				case waitErr = <-waitDone:
				case <-time.After(waitDelay):
					// WaitDelay expired: escalate to SIGKILL(-pgid)
					if p.cmd.Process != nil {
						pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
						if err == nil {
							_ = syscall.Kill(-pgid, syscall.SIGKILL)
						} else {
							_ = p.cmd.Process.Kill()
						}
					}
					waitErr = <-waitDone
				}
			}
		}

		p.closeErr = errors.Join(stdinErr, stdoutErr, waitErr)
	})
	return p.closeErr
}

// Wait waits for the command to exit.
func (p *Process) Wait() error {
	return p.cmd.Wait()
}

// Pid returns the process ID of the harness subprocess.
func (p *Process) Pid() int {
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}
