package agy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Wire types for newline-delimited JSON-RPC 2.0. One message per line, flushed
// as it is written; stdout carries protocol messages only.
type (
	request struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id,omitempty"`
		Method  string          `json:"method,omitempty"`
		Params  json.RawMessage `json:"params,omitempty"`
	}

	response struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result,omitempty"`
		Error   *wireError      `json:"error,omitempty"`
	}

	notification struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}

	wireError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
)

// lineWriter serializes JSON messages onto one stream, so notifications emitted
// from a polling goroutine cannot interleave with a response mid-object.
type lineWriter struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func newLineWriter(w io.Writer) *lineWriter {
	return &lineWriter{w: bufio.NewWriter(w)}
}

// WriteJSON writes v as a single compact JSON line and flushes it.
func (l *lineWriter) WriteJSON(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encoding protocol message: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.w.Write(payload); err != nil {
		return fmt.Errorf("writing protocol message: %w", err)
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return fmt.Errorf("writing protocol message delimiter: %w", err)
	}
	if err := l.w.Flush(); err != nil {
		return fmt.Errorf("flushing protocol message: %w", err)
	}
	return nil
}

// newPrefixLogger returns a logger that tags every line, so bridge diagnostics
// stay attributable when they share a terminal with the agent's own output.
func newPrefixLogger(w io.Writer, prefix string) func(format string, args ...any) {
	if w == nil {
		w = io.Discard
	}
	var mu sync.Mutex
	return func(format string, args ...any) {
		message := fmt.Sprintf(format, args...)
		mu.Lock()
		defer mu.Unlock()
		for _, line := range strings.Split(strings.TrimRight(message, "\n"), "\n") {
			fmt.Fprintf(w, "%s%s\n", prefix, line)
		}
	}
}
