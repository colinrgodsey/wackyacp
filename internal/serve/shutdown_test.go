package serve

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colinrgodsey/wackyacp/internal/acp"
)

// shutdownHarness runs Serve over in-memory pipes and reports when it returns,
// which startServer cannot observe because it discards Serve's result.
type shutdownHarness struct {
	in   *io.PipeWriter
	out  *bufio.Scanner
	done chan error
}

func startShutdownServer(t *testing.T) (*shutdownHarness, context.CancelFunc) {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, &fakeWackypub{}, nil)
	srv := NewServer(inR, outW, b, folder)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
	})
	return &shutdownHarness{in: inW, out: bufio.NewScanner(outR), done: done}, cancel
}

func (h *shutdownHarness) request(t *testing.T, id int, method string, params any) {
	t.Helper()
	frame, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if _, err := h.in.Write(append(frame, '\n')); err != nil {
		t.Fatalf("write request: %v", err)
	}
	for h.out.Scan() {
		var f map[string]json.RawMessage
		if err := json.Unmarshal(h.out.Bytes(), &f); err != nil {
			t.Fatalf("malformed frame: %v", err)
		}
		if _, hasID := f["id"]; hasID {
			return
		}
	}
	t.Fatalf("server closed its output before answering %s: %v", method, h.out.Err())
}

func waitServeExit(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: Serve did not return within 2s", what)
		return nil
	}
}

func TestServeExitsOnInputEOF(t *testing.T) {
	h, _ := startShutdownServer(t)
	h.request(t, 1, acp.MethodInitialize, map[string]any{"protocolVersion": 1})
	if err := h.in.Close(); err != nil {
		t.Fatalf("close input: %v", err)
	}
	if err := waitServeExit(t, h.done, "input EOF"); err != nil {
		t.Errorf("a clean EOF must exit without error, got %v", err)
	}
}

// The SIGTERM shape: the context is canceled while the transport is still open,
// so only closing the reader can wake a blocked scan.
func TestServeExitsOnContextCancelWithInputOpen(t *testing.T) {
	h, cancel := startShutdownServer(t)
	h.request(t, 1, acp.MethodInitialize, map[string]any{"protocolVersion": 1})
	cancel()
	if err := waitServeExit(t, h.done, "ctx cancel"); err != nil {
		t.Errorf("cancel must exit without error, got %v", err)
	}
}

// The wake-up close must not be reported as a transport fault when nothing asked
// for shutdown: a closed reader with a live context is still an error.
func TestServeReportsClosedReaderWhenContextLive(t *testing.T) {
	inR, inW := io.Pipe()
	_, outW := io.Pipe()
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, &fakeWackypub{}, nil)
	srv := NewServer(inR, outW, b, folder)

	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()
	if err := inR.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected the read failure to surface while the context is live")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the reader was closed")
	}
	_ = inW.Close()
}

// The production shape: a real pipe fd whose write end stays open, so no EOF can
// arrive and the scanner is parked in read(2). An in-memory io.Pipe cannot stand in
// for this, because Pipe.Close wakes its own reader while close(2) does not wake a
// blocked read(2) on a real fd.
func TestServeExitsOnCancelWithRealPipeFD(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	_, outW := io.Pipe()
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, &fakeWackypub{}, nil)
	srv := NewServer(inR, outW, b, folder)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
		_ = inR.Close()
	})

	// Nothing is ever written, so only ctx can end Serve.
	cancel()
	if err := waitServeExit(t, done, "ctx cancel over a real pipe fd"); err != nil {
		t.Errorf("cancel must exit without error over a real fd, got %v", err)
	}
}

// Real fd and real EOF: closing the write end must end Serve cleanly, not as a
// transport fault, after the request it carried has been answered.
func TestServeExitsOnRealPipeEOFAfterAnswering(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	outR, outW := io.Pipe()
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, &fakeWackypub{}, nil)
	srv := NewServer(inR, outW, b, folder)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
		_ = inR.Close()
	})

	// The answer is read on its own goroutine so a server that never answers fails
	// this test instead of hanging the package.
	answered := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(outR)
		for scanner.Scan() {
			var f map[string]json.RawMessage
			if err := json.Unmarshal(scanner.Bytes(), &f); err != nil {
				answered <- false
				return
			}
			if _, hasID := f["id"]; hasID {
				answered <- true
				return
			}
		}
		answered <- false
	}()

	frame, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": acp.MethodInitialize,
		"params": map[string]any{"protocolVersion": 1}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := inW.Write(append(frame, '\n')); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case ok := <-answered:
		if !ok {
			t.Fatal("server did not answer initialize over the real fd")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not answer initialize over the real fd within 2s")
	}

	if err := inW.Close(); err != nil {
		t.Fatalf("close write end: %v", err)
	}
	if err := waitServeExit(t, done, "EOF over a real pipe fd"); err != nil {
		t.Errorf("EOF over a real fd must exit without error, got %v", err)
	}
}

// unwokeReader models the production fd honestly: it reports itself as an io.Closer
// the way os.Stdin does, but Close cannot unblock the pending read, which is exactly
// the property of a real blocking fd that made closing the reader on ctx.Done useless
// (bugs/wackyacp/serve-stdin-eof-shutdown-hang). Anything that makes Serve's return
// depend on the read waking fails here.
type unwokeReader struct {
	release chan struct{}
	closed  atomic.Bool
}

func (r *unwokeReader) Read([]byte) (int, error) {
	<-r.release
	return 0, io.EOF
}

func (r *unwokeReader) Close() error {
	r.closed.Store(true)
	return nil
}

func TestServeDoesNotDependOnTheReadWaking(t *testing.T) {
	in := &unwokeReader{release: make(chan struct{})}
	_, outW := io.Pipe()
	folder := t.TempDir()
	b := NewBackend("agent1", filepath.Dir(folder), folder, &fakeWackypub{}, nil)
	srv := NewServer(in, outW, b, folder)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		close(in.release)
	})

	cancel()
	if err := waitServeExit(t, done, "cancel with an un-wakeable read"); err != nil {
		t.Errorf("cancel must exit without error even though the read cannot wake, got %v", err)
	}
	// Closing a caller-owned stdin from inside Serve was a side effect: anything else
	// reading the same fd would see a spurious EOF.
	if in.closed.Load() {
		t.Error("Serve must not close a reader it does not own")
	}
}
