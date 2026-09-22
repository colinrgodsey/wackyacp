package agy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type logRecorder struct {
	lines []string
}

func (l *logRecorder) logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logRecorder) count(substring string) int {
	hits := 0
	for _, line := range l.lines {
		if strings.Contains(line, substring) {
			hits++
		}
	}
	return hits
}

func aggregate(updates []Update) string {
	var b strings.Builder
	for _, u := range updates {
		if u.Content != nil {
			b.WriteString(u.Content.Text)
		}
	}
	return b.String()
}

func TestPollerBindsConversationOnSecondPoll(t *testing.T) {
	dir := t.TempDir()
	tr := NewTranscript(dir)

	before, err := tr.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	rec := &logRecorder{}
	poller := NewTurnPoller(tr, "", -1, before, false, rec.logf)

	// The child creates its conversation file a moment after it starts, so a poll
	// before that is a quiet no-update rather than a logged failure.
	updates, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll before the conversation existed: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("updates = %v, want none", updates)
	}
	if poller.ConversationID() != "" {
		t.Fatal("poller bound a conversation that does not exist")
	}
	if got := rec.count("conversation"); got != 0 {
		t.Fatalf("logged %d line(s) while merely waiting for the conversation: %v", got, rec.lines)
	}

	newConversationDB(t, dir, "fresh", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("hi")}})
	updates, err = poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll after creation: %v", err)
	}
	if poller.ConversationID() != "fresh" {
		t.Fatalf("bound %q, want fresh", poller.ConversationID())
	}
	if aggregate(updates) != "hi" {
		t.Fatalf("updates = %q, want hi", aggregate(updates))
	}
	if poller.LastIdx() != 1 {
		t.Fatalf("LastIdx = %d, want 1", poller.LastIdx())
	}
	if !poller.HadUpdates() {
		t.Fatal("HadUpdates false after emitting text")
	}
}

// TestPollerRefusesToGuessBetweenConversations covers the case where a diagnostic
// really is warranted: two conversations appeared during one turn, so the poller
// cannot attribute any output, and it must say so once rather than every tick.
func TestPollerRefusesToGuessBetweenConversations(t *testing.T) {
	dir := t.TempDir()
	tr := NewTranscript(dir)

	before, err := tr.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	newConversationDB(t, dir, "one", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("a")}})
	newConversationDB(t, dir, "two", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("b")}})

	rec := &logRecorder{}
	poller := NewTurnPoller(tr, "", -1, before, false, rec.logf)

	for attempt := 0; attempt < 2; attempt++ {
		updates, err := poller.Poll(context.Background())
		if !errors.Is(err, ErrAmbiguousConversation) {
			t.Fatalf("Poll %d err = %v, want ErrAmbiguousConversation", attempt, err)
		}
		if len(updates) != 0 {
			t.Fatalf("Poll %d streamed %v without a bound conversation", attempt, updates)
		}
	}
	if poller.ConversationID() != "" {
		t.Fatalf("bound %q despite the ambiguity", poller.ConversationID())
	}
	if got := rec.count("cannot bind"); got != 1 {
		t.Fatalf("logged %d bind diagnostics, want exactly 1: %v", got, rec.lines)
	}
}

func TestPollerStreamsTextDeltaPerRow(t *testing.T) {
	dir := t.TempDir()
	path := newConversationDB(t, dir, "conv", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("Hello")}})
	tr := NewTranscript(dir)

	poller := NewTurnPoller(tr, "conv", -1, nil, false, nil)
	first, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if aggregate(first) != "Hello" {
		t.Fatalf("first poll = %q", aggregate(first))
	}

	updateStepPayload(t, path, 1, textPayload("Hello world"))
	second, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if aggregate(second) != " world" {
		t.Fatalf("second poll = %q, want the delta only", aggregate(second))
	}

	third, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third poll = %q, want nothing new", aggregate(third))
	}
}

func TestPollerAdvancesAcrossRows(t *testing.T) {
	dir := t.TempDir()
	path := newConversationDB(t, dir, "conv", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("one ")}})
	tr := NewTranscript(dir)
	poller := NewTurnPoller(tr, "conv", -1, nil, false, nil)

	if _, err := poller.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	insertStep(t, path, row{idx: 2, stepType: stepTypeText, payload: textPayload("two ")})
	insertStep(t, path, row{idx: 3, stepType: 5, payload: toolPayload("read_file", `{"path":"/p"}`)})

	updates, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if aggregate(updates) != "two " {
		t.Fatalf("text = %q, want %q", aggregate(updates), "two ")
	}
	if len(updates) != 3 {
		t.Fatalf("got %d updates, want one chunk plus a tool pair", len(updates))
	}
	if updates[1].SessionUpdate != updateToolCall || updates[2].SessionUpdate != updateToolDone {
		t.Fatalf("tool updates out of order: %+v", updates[1:])
	}
	if updates[1].ToolCallID != "agy-3-5" || updates[2].ToolCallID != "agy-3-5" {
		t.Fatalf("tool ids = %q / %q, want matching ids", updates[1].ToolCallID, updates[2].ToolCallID)
	}
	if updates[1].Title != "read_file: /p" || updates[2].Status != "completed" {
		t.Fatalf("tool pair = %+v, %+v", updates[1], updates[2])
	}
	if updates[1].ToolName != "read_file" || updates[2].ToolName != "read_file" {
		t.Fatalf("tool names = %q / %q, want read_file", updates[1].ToolName, updates[2].ToolName)
	}
	if string(updates[1].RawInput) != `{"path":"/p"}` {
		t.Fatalf("rawInput = %s", updates[1].RawInput)
	}
}

func TestPollerEmitsToolCallOnce(t *testing.T) {
	dir := t.TempDir()
	path := newConversationDB(t, dir, "conv", []row{{idx: 1, stepType: 5, payload: toolPayload("shell", `{"command":"ls"}`)}})
	tr := NewTranscript(dir)
	poller := NewTurnPoller(tr, "conv", -1, nil, false, nil)

	first, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("first poll = %d updates, want the pair", len(first))
	}
	updateStepPayload(t, path, 1, toolPayload("shell", `{"command":"ls -la"}`))
	second, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("tool call re-emitted: %+v", second)
	}
}

func TestPollerNarration(t *testing.T) {
	tests := []struct {
		name          string
		showNarration bool
		text          string
		want          string
	}{
		{name: "suppressed", text: "I will read the file", want: ""},
		{name: "shown when enabled", showNarration: true, text: "I will read the file", want: "I will read the file"},
		{name: "ordinary text kept", text: "The file contains three functions.", want: "The file contains three functions."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			newConversationDB(t, dir, "conv", []row{{idx: 1, stepType: stepTypeText, payload: textPayload(tc.text)}})
			poller := NewTurnPoller(NewTranscript(dir), "conv", -1, nil, tc.showNarration, nil)
			updates, err := poller.Poll(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := aggregate(updates); got != tc.want {
				t.Fatalf("aggregate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPollerNarrationThatStopsBeingNarration(t *testing.T) {
	dir := t.TempDir()
	path := newConversationDB(t, dir, "conv", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("I will")}})
	poller := NewTurnPoller(NewTranscript(dir), "conv", -1, nil, false, nil)

	if updates, err := poller.Poll(context.Background()); err != nil || len(updates) != 0 {
		t.Fatalf("poll = %v, %v; want narration suppressed so far", updates, err)
	}

	updateStepPayload(t, path, 1, textPayload("I will\nand here is the answer"))
	updates, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := aggregate(updates); got != "\nand here is the answer" {
		t.Fatalf("aggregate = %q, want only the non-narration suffix", got)
	}
}

func TestPollerHonoursBaseIndex(t *testing.T) {
	dir := t.TempDir()
	newConversationDB(t, dir, "conv", []row{
		{idx: 1, stepType: stepTypeText, payload: textPayload("previous turn")},
		{idx: 2, stepType: stepTypeText, payload: textPayload("this turn")},
	})
	poller := NewTurnPoller(NewTranscript(dir), "conv", 1, nil, false, nil)

	updates, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := aggregate(updates); got != "this turn" {
		t.Fatalf("aggregate = %q, want only rows above the base index", got)
	}
}

// TestPollerWaitsForTheStepsTable covers the startup race that agy really does
// create: the conversation file appears before its steps table. That is not a
// schema change, so it must be quiet and recover on a later poll.
func TestPollerWaitsForTheStepsTable(t *testing.T) {
	dir := t.TempDir()
	writeEmptyDB(t, dir+"/conv.db")

	rec := &logRecorder{}
	poller := NewTurnPoller(NewTranscript(dir), "conv", -1, nil, false, rec.logf)
	for i := 0; i < 3; i++ {
		updates, err := poller.Poll(context.Background())
		if err != nil {
			t.Fatalf("poll %d err = %v, want the missing table treated as not ready", i, err)
		}
		if len(updates) != 0 {
			t.Fatalf("poll %d streamed %v from an empty database", i, updates)
		}
	}
	if !poller.SchemaMissing() {
		t.Fatal("SchemaMissing = false while the steps table is absent")
	}
	if got := rec.count("steps table"); got != 0 {
		t.Fatalf("logged %d lines about the missing table, want silence while it may still appear", got)
	}

	writeConversationDB(t, dir+"/conv.db", []row{{idx: 1, stepType: stepTypeText, payload: textPayload("hi")}})
	updates, err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll after the table appeared: %v", err)
	}
	if got := aggregate(updates); got != "hi" {
		t.Fatalf("aggregate = %q, want hi", got)
	}
	if poller.SchemaMissing() {
		t.Fatal("SchemaMissing = true after a successful read")
	}
}

func TestPollerUnreadableTextRowWarnsOnce(t *testing.T) {
	dir := t.TempDir()
	newConversationDB(t, dir, "conv", []row{{idx: 4, stepType: stepTypeText, payload: []byte{0xff, 0xff}}})

	rec := &logRecorder{}
	poller := NewTurnPoller(NewTranscript(dir), "conv", -1, nil, false, rec.logf)
	for i := 0; i < 3; i++ {
		updates, err := poller.Poll(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(updates) != 0 {
			t.Fatalf("poll %d emitted %+v for an undecodable row", i, updates)
		}
	}
	if got := rec.count("no extractable text"); got != 1 {
		t.Fatalf("warning repeated %d times, want once", got)
	}
	if poller.HadUpdates() {
		t.Fatal("HadUpdates true without any update emitted")
	}
}

func TestPollerToolWithoutNameWarnsOnce(t *testing.T) {
	dir := t.TempDir()
	payload := pbBytes(fieldToolWrapper, pbBytes(fieldToolCall, pbString(fieldToolArgs, "{}")))
	newConversationDB(t, dir, "conv", []row{{idx: 9, stepType: 5, payload: payload}})

	rec := &logRecorder{}
	poller := NewTurnPoller(NewTranscript(dir), "conv", -1, nil, false, rec.logf)
	for i := 0; i < 2; i++ {
		updates, err := poller.Poll(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(updates) != 0 {
			t.Fatalf("poll %d emitted %+v for an anonymous tool call", i, updates)
		}
	}
	if got := rec.count("no name"); got != 1 {
		t.Fatalf("warning repeated %d times, want once", got)
	}
}

func TestPollerCancelledContextStopsBinding(t *testing.T) {
	dir := t.TempDir()
	before, err := NewTranscript(dir).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	poller := NewTurnPoller(NewTranscript(dir), "", -1, before, false, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := poller.Poll(ctx); err == nil {
		t.Fatal("Poll bound work on a cancelled context")
	}
}

func TestNextDeltaSnapsToRuneStart(t *testing.T) {
	text := "aé日"
	tests := []struct {
		name    string
		emitted int
		want    string
	}{
		{name: "nothing emitted", emitted: 0, want: text},
		{name: "rune boundary", emitted: 1, want: "é日"},
		{name: "mid rune start snaps forward", emitted: 2, want: "日"},
		{name: "mid three byte rune", emitted: 4, want: ""},
		{name: "fully emitted", emitted: len(text), want: ""},
		{name: "beyond end", emitted: len(text) + 5, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			delta, next := nextDelta(text, tc.emitted)
			if delta != tc.want {
				t.Fatalf("nextDelta(%d) = %q, want %q", tc.emitted, delta, tc.want)
			}
			if next != len(text) {
				t.Fatalf("nextDelta returned watermark %d, want %d", next, len(text))
			}
			if !utf8Valid(delta) {
				t.Fatalf("delta %q is not valid UTF-8", delta)
			}
		})
	}
}

func utf8Valid(s string) bool {
	return strings.ToValidUTF8(s, "") == s
}
