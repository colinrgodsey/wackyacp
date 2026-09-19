package agy

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"
)

// TurnPoller translates agy step rows into ACP updates for a single prompt turn.
//
// agy exposes no push channel, so progress is discovered by re-reading the
// conversation database: a text row grows in place while the model streams, and
// a tool row appears fully formed once the call is made. The poller therefore
// tracks, per row, how much of that row has already been sent and emits only the
// suffix. It is driven externally (one Poll call per tick, plus a final one)
// rather than owning a timer, which keeps it testable without wall-clock waits.
type TurnPoller struct {
	transcript    *Transcript
	showNarration bool
	logf          func(format string, args ...any)
	advisoryLogf  func(format string, args ...any)

	// snapshot is the conversations directory listing taken before spawning, or
	// nil when this turn continues an already bound conversation.
	snapshot map[string]bool

	conversationID string
	baseIdx        int64
	lastIdx        int64

	emittedText    map[int64]int
	emittedTools   map[int64]bool
	warnedNoText   map[int64]bool
	warnedNoName   map[int64]bool
	hadUpdates     bool
	schemaMissing  bool
	bindWarned     bool
	bindWarnedOnce bool
}

// NewTurnPoller builds a poller for one turn. Pass the conversation id already
// bound to the session (empty for a first turn) together with the pre-spawn
// directory snapshot to bind, and the session's last observed step index as the
// base so earlier turns are never re-emitted.
func NewTurnPoller(transcript *Transcript, conversationID string, baseIdx int64, snapshot map[string]bool, showNarration bool, logf func(format string, args ...any)) *TurnPoller {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &TurnPoller{
		transcript:     transcript,
		conversationID: conversationID,
		baseIdx:        baseIdx,
		lastIdx:        baseIdx,
		snapshot:       snapshot,
		showNarration:  showNarration,
		logf:           logf,
		advisoryLogf:   logf,
		emittedText:    map[int64]int{},
		emittedTools:   map[int64]bool{},
		warnedNoText:   map[int64]bool{},
		warnedNoName:   map[int64]bool{},
	}
}

// SetAdvisoryLog sets a separate logger for per-step poller advisories. If nil,
// advisories are dropped.
func (p *TurnPoller) SetAdvisoryLog(fn func(format string, args ...any)) {
	if fn == nil {
		fn = func(string, ...any) {}
	}
	p.advisoryLogf = fn
}

// AdvisoryCounts returns the number of steps that had no extractable text and
// the number of tool-shaped steps that lacked names during this turn.
func (p *TurnPoller) AdvisoryCounts() (noText int, noName int) {
	return len(p.warnedNoText), len(p.warnedNoName)
}

// ConversationID returns the conversation this poller is reading, empty until a
// first turn's conversation has been identified.
func (p *TurnPoller) ConversationID() string { return p.conversationID }

// LastIdx returns the highest step index observed, for persistence.
func (p *TurnPoller) LastIdx() int64 { return p.lastIdx }

// HadUpdates reports whether any update was emitted, which distinguishes a
// genuinely empty answer from a swallowed backend failure.
// SchemaMissing reports whether the last read of the bound conversation lacked
// the steps table, which tells an empty answer apart from a schema change once
// the turn is over.
func (p *TurnPoller) SchemaMissing() bool {
	return p.schemaMissing
}

func (p *TurnPoller) HadUpdates() bool { return p.hadUpdates }

// Poll binds the conversation if needed and returns the updates produced since
// the previous poll. Errors are advisory: a busy database simply produces no
// updates until the next poll.
func (p *TurnPoller) Poll(ctx context.Context) ([]Update, error) {
	if err := p.bind(ctx); err != nil {
		return nil, err
	}
	if p.conversationID == "" {
		return nil, nil
	}

	steps, err := p.transcript.Steps(ctx, p.conversationID, p.baseIdx)
	if err != nil {
		if errors.Is(err, ErrNoStepsTable) {
			// agy creates the steps table a moment after the file, so a missing table is
			// normally "not ready" rather than a schema change: remember it, stay quiet,
			// and let the next poll retry. The turn reports it if it never arrives.
			p.schemaMissing = true
			return nil, nil
		}
		p.logf("polling conversation %s: %v", p.conversationID, err)
		return nil, err
	}
	p.schemaMissing = false

	var updates []Update
	for _, step := range steps {
		if step.Idx > p.lastIdx {
			p.lastIdx = step.Idx
		}
		switch {
		case isTextStepType(step.StepType):
			if update, ok := p.textUpdate(step); ok {
				updates = append(updates, update)
			}
		case isToolStepType(step.StepType):
			updates = append(updates, p.toolUpdates(step)...)
		}
	}
	if len(updates) > 0 {
		p.hadUpdates = true
	}
	return updates, nil
}

// bind identifies the conversation created by this turn by diffing the
// conversations directory against the pre-spawn snapshot. Failure to bind is
// reported once per turn; it is not fatal, because agy may not have created the
// file yet.
func (p *TurnPoller) bind(ctx context.Context) error {
	if p.conversationID != "" || p.snapshot == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	id, err := p.transcript.OnlyNewSince(p.snapshot)
	switch {
	case err == nil:
	case errors.Is(err, ErrNoNewConversation):
		// The child has not created its conversation file yet, which is normal for
		// the first polls of a turn: stay quiet and let the next tick try again.
		return nil
	case errors.Is(err, ErrAmbiguousConversation) && !p.bindWarnedOnce:
		p.logf("cannot bind this turn to one agy conversation: %v", err)
		p.bindWarnedOnce = true
		return err
	default:
		return err
	}
	p.conversationID = id
	p.logf("bound conversation %s", id)
	return nil
}

// textUpdate emits the not-yet-sent suffix of a text row, dropping narration
// rows entirely unless narration display is enabled.
func (p *TurnPoller) textUpdate(step Step) (Update, bool) {
	text, ok := stepText(step.Payload)
	if !ok || text == "" {
		if !p.warnedNoText[step.Idx] {
			p.warnedNoText[step.Idx] = true
			p.advisoryLogf("step %d has no extractable text (agy field 20.1 missing; schema change?)", step.Idx)
		}
		return Update{}, false
	}

	delta, emitted := nextDelta(text, p.emittedText[step.Idx])
	if !p.showNarration && isNarration(text) {
		// Mark the whole row consumed so later growth of the same row emits only
		// the text that stops it from looking like narration.
		p.emittedText[step.Idx] = len(text)
		return Update{}, false
	}
	if delta == "" {
		return Update{}, false
	}
	p.emittedText[step.Idx] = emitted
	return Update{
		SessionUpdate: updateMessageChunk,
		Content:       &Content{Type: "text", Text: delta},
	}, true
}

// toolUpdates emits a tool call as a completed pair: agy only records a tool row
// once the call has finished, so there is no observable running state and no
// tool output to report.
func (p *TurnPoller) toolUpdates(step Step) []Update {
	if p.emittedTools[step.Idx] {
		return nil
	}
	call, ok := stepToolCall(step.Payload)
	if !ok {
		if !p.warnedNoName[step.Idx] {
			p.warnedNoName[step.Idx] = true
			p.advisoryLogf("step %d looks like a tool call but has no name (agy field 5.4 missing; schema change?)", step.Idx)
		}
		return nil
	}
	p.emittedTools[step.Idx] = true

	id := fmt.Sprintf("agy-%d-%d", step.Idx, step.StepType)
	title := toolCallTitle(call.Name, call.Input)
	return []Update{
		{SessionUpdate: updateToolCall, ToolCallID: id, Title: title, RawInput: call.Input},
		{SessionUpdate: updateToolDone, ToolCallID: id, Title: title, Status: "completed"},
	}
}

// nextDelta returns the part of text that has not been sent yet, snapping the
// start forward to the next rune boundary so a row that grew mid-character cannot
// emit half a rune.
func nextDelta(text string, emitted int) (string, int) {
	if emitted >= len(text) {
		return "", len(text)
	}
	start := emitted
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	if start >= len(text) {
		return "", len(text)
	}
	return text[start:], len(text)
}
