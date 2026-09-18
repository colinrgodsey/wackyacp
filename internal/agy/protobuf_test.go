package agy

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReadVarint(t *testing.T) {
	tests := []struct {
		name   string
		in     []byte
		want   uint64
		used   int
		wantOK bool
	}{
		{name: "zero", in: []byte{0x00}, want: 0, used: 1, wantOK: true},
		{name: "single byte", in: []byte{0x7f}, want: 127, used: 1, wantOK: true},
		{name: "two bytes", in: []byte{0xac, 0x02}, want: 300, used: 2, wantOK: true},
		{name: "ten bytes fill all bits", in: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, want: ^uint64(0), used: 10, wantOK: true},
		{name: "empty", in: []byte{}, wantOK: false},
		{name: "truncated continuation", in: []byte{0x80}, wantOK: false},
		{name: "overlong", in: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, used, ok := readVarint(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (value=%d)", ok, tc.wantOK, value)
			}
			if !ok {
				return
			}
			if value != tc.want || used != tc.used {
				t.Fatalf("value=%d used=%d, want value=%d used=%d", value, used, tc.want, tc.used)
			}
		})
	}
}

func TestProtoFieldLengthDelimited(t *testing.T) {
	blob := append(pbString(3, "alpha"), pbString(20, "beta")...)
	raw, ok := protoField(blob, 20)
	if !ok || string(raw) != "beta" {
		t.Fatalf("protoField(20) = %q, %v; want %q", raw, ok, "beta")
	}
	if _, ok := protoField(blob, 4); ok {
		t.Fatal("protoField(4) succeeded, want miss")
	}
}

func TestProtoFieldSkipsVarintAndKeepsScanning(t *testing.T) {
	// Only length-delimited fields are addressable; varint fields must be stepped
	// over correctly so a later length-delimited target is still found.
	blob := append(pbInt(7, 42), pbString(20, "after")...)
	if _, ok := protoField(blob, 7); ok {
		t.Fatal("protoField returned a varint field")
	}
	raw, ok := protoField(blob, 20)
	if !ok || string(raw) != "after" {
		t.Fatalf("protoField(20) = %q, %v; want after", raw, ok)
	}
}

func TestProtoFieldTruncatedLength(t *testing.T) {
	// A length longer than the blob must be rejected rather than sliced, or a
	// corrupt payload read off disk would panic the bridge.
	blob := append(pbTag(20, 2), pbVarint(1<<40)...)
	if _, ok := protoField(blob, 20); ok {
		t.Fatal("protoField accepted a length past the end of the blob")
	}
}

func TestProtoFieldFixedWidth(t *testing.T) {
	blob := append([]byte{pbTag(3, 5)[0], 0, 0, 0, 0}, pbString(20, "tail")...)
	raw, ok := protoField(blob, 20)
	if !ok || string(raw) != "tail" {
		t.Fatalf("protoField(20) after fixed32 = %q, %v; want tail", raw, ok)
	}
}

func TestProtoFieldRejectsUnknownWireType(t *testing.T) {
	// Wire type 3 (start group) is not decidable without the matching end group,
	// so the scan must abort rather than skip ahead into the middle of the blob.
	blob := append([]byte{0x1b, 0x01, 0x02}, pbString(20, "visible")...)
	if _, ok := protoField(blob, 20); ok {
		t.Fatal("protoField walked past an unsupported wire type")
	}
}

func TestProtoTextRejectsInvalidUTF8(t *testing.T) {
	blob := pbBytes(1, []byte{0xff, 0xfe})
	if _, ok := protoText(blob, 1); ok {
		t.Fatal("protoText accepted invalid UTF-8")
	}
}

func TestStepText(t *testing.T) {
	text, ok := stepText(textPayload("hello world"))
	if !ok || text != "hello world" {
		t.Fatalf("stepText = %q, %v; want %q", text, ok, "hello world")
	}
}

func TestStepTextRejectsWrongNesting(t *testing.T) {
	// Negative control: the same string at field 1 of the payload root must not be
	// found, proving the extraction really addresses 20 -> 1.
	if text, ok := stepText(pbString(fieldTextBody, "hello world")); ok {
		t.Fatalf("stepText found text at the root (%q)", text)
	}
}

func TestStepTextMissingInnerField(t *testing.T) {
	if _, ok := stepText(pbBytes(fieldTextField, []byte("not-a-message"))); ok {
		t.Fatal("stepText accepted a field 20 with no string body")
	}
}

func TestStepToolCall(t *testing.T) {
	call, ok := stepToolCall(toolPayload("read_file", `{"path":"/tmp/x"}`))
	if !ok {
		t.Fatal("stepToolCall failed")
	}
	if call.Name != "read_file" {
		t.Fatalf("name = %q, want read_file", call.Name)
	}
	if string(call.Input) != `{"path":"/tmp/x"}` {
		t.Fatalf("input = %s", call.Input)
	}
}

func TestStepToolCallAltName(t *testing.T) {
	call, ok := stepToolCall(toolPayloadAltName("search", `{"query":"acp"}`))
	if !ok || call.Name != "search" {
		t.Fatalf("alt-name call = %+v, %v; want name search", call, ok)
	}
}

func TestStepToolCallDropsInvalidArgsJSON(t *testing.T) {
	call, ok := stepToolCall(toolPayload("run", "{not json"))
	if !ok {
		t.Fatal("stepToolCall failed")
	}
	if len(call.Input) != 0 {
		t.Fatalf("input = %s, want dropped", call.Input)
	}
}

func TestStepToolCallNeedsName(t *testing.T) {
	payload := pbBytes(fieldToolWrapper, pbBytes(fieldToolCall, pbString(fieldToolArgs, `{}`)))
	if _, ok := stepToolCall(payload); ok {
		t.Fatal("stepToolCall accepted a call with no name")
	}
}

func TestStepToolCallWrongNesting(t *testing.T) {
	// Negative control: name at field 2 of the root is not a tool call.
	if _, ok := stepToolCall(pbString(fieldToolName, "read_file")); ok {
		t.Fatal("stepToolCall found a tool call at the root")
	}
}

func TestToolCallTitle(t *testing.T) {
	tests := []struct {
		name  string
		tool  string
		input json.RawMessage
		want  string
	}{
		{name: "no args", tool: "list_dir", input: nil, want: "list_dir"},
		{name: "path wins", tool: "read_file", input: json.RawMessage(`{"path":"/a/b","query":"q"}`), want: "read_file: /a/b"},
		{name: "file key", tool: "write", input: json.RawMessage(`{"file":"/a"}`), want: "write: /a"},
		{name: "query fallback", tool: "search", input: json.RawMessage(`{"query":"needle"}`), want: "search: needle"},
		{name: "command fallback", tool: "shell", input: json.RawMessage(`{"command":"ls -la"}`), want: "shell: ls -la"},
		{name: "empty value skipped", tool: "search", input: json.RawMessage(`{"query":"","text":"t"}`), want: "search: t"},
		{name: "no known key", tool: "thing", input: json.RawMessage(`{"other":"x"}`), want: "thing"},
		{name: "non-object args", tool: "thing", input: json.RawMessage(`[1,2]`), want: "thing"},
		{name: "invalid json args", tool: "thing", input: json.RawMessage(`{`), want: "thing"},
		{
			name:  "long value truncated to 60 runes",
			tool:  "read_file",
			input: json.RawMessage(`{"path":"` + strings.Repeat("é", 80) + `"}`),
			want:  "read_file: " + strings.Repeat("é", 60),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := toolCallTitle(tc.tool, tc.input); got != tc.want {
				t.Fatalf("toolCallTitle(%q, %s) = %q, want %q", tc.tool, tc.input, got, tc.want)
			}
		})
	}
}

func TestIsNarration(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{text: "I will read the file", want: true},
		{text: "I will read the file\nI will summarise it", want: true},
		{text: "I will read the file\n\nI will summarise it\n", want: true},
		{text: "  I will read the file", want: true},
		{text: "Here is the answer", want: false},
		{text: "I will read the file\nIt is short", want: false},
		{text: "I willpower", want: false},
		{text: "", want: false},
		{text: "   \n\t\n", want: false},
	}
	for _, tc := range tests {
		if got := isNarration(tc.text); got != tc.want {
			t.Fatalf("isNarration(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestStepTypeClassification(t *testing.T) {
	if !isTextStepType(stepTypeText) {
		t.Fatal("text step type not recognised")
	}
	if isTextStepType(5) || isToolStepType(stepTypeText) {
		t.Fatal("text and tool step types overlap")
	}
	for _, toolType := range []int64{5, 7, 8, 9, 17, 21, 33, 101, 138} {
		if !isToolStepType(toolType) {
			t.Fatalf("tool step type %d not recognised", toolType)
		}
	}
	if isToolStepType(6) || isToolStepType(0) {
		t.Fatal("unclassified step type treated as a tool")
	}
}

func TestWithPrintTimeout(t *testing.T) {
	tests := []struct {
		name  string
		extra []string
		want  []string
	}{
		{name: "appended when absent", extra: []string{"--x"}, want: []string{"--x", "--print-timeout", defaultPrintTimeout}},
		{name: "empty extra", extra: nil, want: []string{"--print-timeout", defaultPrintTimeout}},
		{name: "spaced form respected", extra: []string{"--print-timeout", "5m"}, want: []string{"--print-timeout", "5m"}},
		{name: "equals form respected", extra: []string{"--print-timeout=5m"}, want: []string{"--print-timeout=5m"}},
		{name: "similar flag does not count", extra: []string{"--print-timeout-other", "1s"}, want: []string{"--print-timeout-other", "1s", "--print-timeout", defaultPrintTimeout}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := withPrintTimeout(tc.extra, "")
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("withPrintTimeout(%v) = %v, want %v", tc.extra, got, tc.want)
			}
		})
	}
}

func TestWithPrintTimeoutDoesNotMutateCaller(t *testing.T) {
	extra := []string{"--x"}
	_ = withPrintTimeout(extra, "")
	if len(extra) != 1 {
		t.Fatalf("caller slice grew to %v", extra)
	}
}
