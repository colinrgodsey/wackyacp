package harness

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestResolve(t *testing.T) {
	// Existing binary
	path, err := Resolve("sh")
	if err != nil {
		t.Fatalf("expected sh to resolve, got: %v", err)
	}
	if path == "" {
		t.Fatalf("expected non-empty path")
	}

	// Missing binary
	_, err = Resolve("nonexistent-command-12345-xyz")
	if err == nil {
		t.Fatalf("expected error for nonexistent binary, got nil")
	}
	if !errors.Is(err, ErrHarnessNotFound) {
		t.Fatalf("expected ErrHarnessNotFound, got: %v", err)
	}
}

func TestHarnessLifecycle(t *testing.T) {
	ctx := context.Background()
	var stderr bytes.Buffer

	proc, err := Start(ctx, Config{
		Command: "cat",
		Stderr:  &stderr,
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer proc.Close()

	if proc.Pid() == 0 {
		t.Fatalf("expected non-zero pid")
	}

	// Write to stdin and read from stdout
	msg := "hello harness\n"
	if _, err := io.WriteString(proc.Stdin, msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(proc.Stdout, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf) != msg {
		t.Fatalf("expected %q, got %q", msg, string(buf))
	}

	if err := proc.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestHarnessCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	proc, err := Start(ctx, Config{
		Command: "sleep",
		Args:    []string{"10"},
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Cancel context to trigger process-group termination
	time.Sleep(50 * time.Millisecond)
	cancel()

	err = proc.Wait()
	if err == nil {
		t.Fatalf("expected process to terminate with error on cancel")
	}
}

func TestHarnessIgnoreEOF_CloseEscalatesKill(t *testing.T) {
	ctx := context.Background()

	// Child process reads stdin then sleeps rather than exiting
	proc, err := Start(ctx, Config{
		Command:   "sh",
		Args:      []string{"-c", "cat > /dev/null; sleep 100"},
		WaitDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- proc.Close()
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed > 3*time.Second {
			t.Errorf("expected Close to terminate uncooperative child within WaitDelay bounds, took: %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("proc.Close() hung indefinitely on child ignoring stdin EOF")
	}
}
