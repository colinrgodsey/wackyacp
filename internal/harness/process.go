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
}

// Process wraps the running harness subprocess.
type Process struct {
	cmd       *exec.Cmd
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

	cmd := exec.CommandContext(ctx, binPath, cfg.Args...)
	if cfg.AgentFolder != "" {
		cmd.Dir = cfg.AgentFolder
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Go 1.20+ process-group termination: SIGTERM -> 5s WaitDelay -> SIGKILL
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
	cmd.WaitDelay = 5 * time.Second

	if cfg.Stderr != nil {
		cmd.Stderr = cfg.Stderr
	} else {
		cmd.Stderr = os.Stderr
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("creating harness stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("creating harness stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("starting harness process %s: %w", cfg.Command, err)
	}

	return &Process{
		cmd:    cmd,
		Stdin:  stdin,
		Stdout: stdout,
	}, nil
}

// Close closes the stdio pipes and waits for the process to exit.
func (p *Process) Close() error {
	p.closeOnce.Do(func() {
		var stdinErr, stdoutErr error
		if p.Stdin != nil {
			stdinErr = p.Stdin.Close()
		}
		if p.Stdout != nil {
			stdoutErr = p.Stdout.Close()
		}
		waitErr := p.cmd.Wait()
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
