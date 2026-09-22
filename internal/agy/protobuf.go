package agy

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// agy persists each conversation step as a protobuf blob in the steps table.
// The schemas are not public, so the fields below are addressed by number only.
const (
	// fieldTextField is the sub-message inside a step payload holding assistant text.
	fieldTextField = 20
	// fieldTextBody is the string inside fieldTextField.
	fieldTextBody = 1
	// fieldToolWrapper is the sub-message inside a tool step payload.
	fieldToolWrapper = 5
	// fieldToolCall is the call sub-message inside fieldToolWrapper.
	fieldToolCall = 4
	// fieldToolName / fieldToolNameAlt name the tool; agy uses either.
	fieldToolName    = 2
	fieldToolNameAlt = 9
	// fieldToolArgs holds the tool arguments as a JSON string.
	fieldToolArgs = 3
)

// maxVarintBytes bounds a varint to 64-bit width; a continuation bit past the
// tenth byte means the blob is corrupt rather than merely long.
const maxVarintShift = 70

// readVarint decodes a base-128 varint, returning the value and the number of
// bytes consumed.
func readVarint(buf []byte) (value uint64, consumed int, ok bool) {
	var shift uint
	for i, b := range buf {
		if shift >= maxVarintShift {
			return 0, 0, false
		}
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, i + 1, true
		}
		shift += 7
	}
	return 0, 0, false
}

// protoField returns the raw bytes of the first length-delimited field numbered
// target. Scanning stops when a wire type outside the fixed-width set appears,
// because the remaining offsets in that blob can no longer be trusted.
func protoField(blob []byte, target uint64) ([]byte, bool) {
	i := 0
	for i < len(blob) {
		tag, n, ok := readVarint(blob[i:])
		if !ok {
			return nil, false
		}
		i += n
		fieldNumber, wireType := tag>>3, tag&0x7
		switch wireType {
		case 0: // varint
			_, n, ok := readVarint(blob[i:])
			if !ok {
				return nil, false
			}
			i += n
		case 2: // length-delimited
			length, n, ok := readVarint(blob[i:])
			if !ok {
				return nil, false
			}
			i += n
			end := i + int(length)
			if int64(end) > int64(len(blob)) || end < i {
				return nil, false
			}
			if fieldNumber == target {
				return blob[i:end], true
			}
			i = end
		case 5: // fixed 32
			i += 4
		case 1: // fixed 64
			i += 8
		default: // groups (3, 4) and fixed 32/64 signatures (6, 7)
			return nil, false
		}
	}
	return nil, false
}

// protoText reads a length-delimited field as a UTF-8 string.
func protoText(blob []byte, target uint64) (string, bool) {
	raw, ok := protoField(blob, target)
	if !ok {
		return "", false
	}
	s := string(raw)
	if !utf8.ValidString(s) {
		return "", false
	}
	return s, true
}

// stepText extracts assistant text from a step payload (field 20 -> field 1).
func stepText(payload []byte) (string, bool) {
	inner, ok := protoField(payload, fieldTextField)
	if !ok {
		return "", false
	}
	return protoText(inner, fieldTextBody)
}

// toolCall is a tool invocation recorded in a tool step row.
type toolCall struct {
	Name  string
	Input json.RawMessage
}

// stepToolCall extracts a tool name and JSON arguments from a tool step payload
// (field 5 -> field 4). Arguments that are absent or not valid JSON are dropped
// rather than surfaced as raw text, so a client never receives a malformed
// rawInput.
func stepToolCall(payload []byte) (toolCall, bool) {
	wrapper, ok := protoField(payload, fieldToolWrapper)
	if !ok {
		return toolCall{}, false
	}
	call, ok := protoField(wrapper, fieldToolCall)
	if !ok {
		return toolCall{}, false
	}
	name, ok := protoText(call, fieldToolName)
	if !ok || name == "" {
		if alt, altOK := protoText(call, fieldToolNameAlt); altOK && alt != "" {
			name, ok = alt, true
		}
	}
	if !ok || name == "" {
		return toolCall{}, false
	}
	tc := toolCall{Name: name}
	if rawArgs, argsOK := protoText(call, fieldToolArgs); argsOK && json.Valid([]byte(rawArgs)) {
		tc.Input = json.RawMessage(rawArgs)
	}
	return tc, true
}

// isTextStepType reports whether a step row carries assistant text.
func isTextStepType(stepType int64) bool { return stepType == stepTypeText }

// stepTypeText is the agy step type holding assistant message text.
const stepTypeText int64 = 15

// toolStepTypes are the agy step types that represent a tool invocation.
var toolStepTypes = map[int64]bool{
	5: true, 7: true, 8: true, 9: true, 17: true, 21: true, 33: true, 101: true, 132: true, 138: true,
}

// isToolStepType reports whether a step row represents a tool invocation.
func isToolStepType(stepType int64) bool { return toolStepTypes[stepType] }

// titleArgKeys are probed in order to build a human-readable tool title. The
// first key present wins, mirroring the reference bridge's display behaviour.
var titleArgKeys = [][]string{
	{"path", "file", "AbsolutePath", "FilePath"},
	{"query", "command", "text"},
}

const maxTitleArgRunes = 60

// toolCallTitle renders a short title for a tool call from its arguments.
func toolCallTitle(name string, input json.RawMessage) string {
	if len(input) == 0 {
		return name
	}
	var args map[string]any
	if err := json.Unmarshal(input, &args); err != nil {
		return name
	}
	for _, keys := range titleArgKeys {
		for _, key := range keys {
			value, ok := args[key].(string)
			if !ok || value == "" {
				continue
			}
			return fmt.Sprintf("%s: %s", name, truncateRunes(value, maxTitleArgRunes))
		}
	}
	return name
}

// truncateRunes cuts s to at most max runes without splitting a rune.
func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// isNarration reports whether an assistant step is internal narration that the
// reference bridge hides from clients: every non-blank line begins with
// "I will".
func isNarration(text string) bool {
	sawLine := false
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !isNarrationLine(strings.TrimLeft(line, leadingWhitespace)) {
			return false
		}
		sawLine = true
	}
	return sawLine
}

const (
	narrationPrefix   = "I will"
	leadingWhitespace = " \t\r\n"
)

// isNarrationLine reports whether a single line opens with the narration marker
// as a whole word, so prose that merely starts with the same letters ("I
// willpower ...") is not mistaken for internal planning and hidden.
func isNarrationLine(line string) bool {
	if !strings.HasPrefix(line, narrationPrefix) {
		return false
	}
	rest := line[len(narrationPrefix):]
	if rest == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return !unicode.IsLetter(r) && !unicode.IsDigit(r)
}
