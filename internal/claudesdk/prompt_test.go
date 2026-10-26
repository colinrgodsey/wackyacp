package claudesdk

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// nl avoids an escaped newline literal in fixture building.
var nl = string([]byte{10})

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestComposeSystemPromptCarriesAgentsSkillsAndAddendum(t *testing.T) {
	ws := t.TempDir()
	agentDir := filepath.Join(ws, "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "You are claude for this workspace."+nl)
	writeFile(t, filepath.Join(agentDir, "skills", "always-one", "SKILL.md"),
		"---"+nl+"name: always-one"+nl+"description: always loaded"+nl+"always_load: true"+nl+"---"+nl+"Always-one body text."+nl)
	writeFile(t, filepath.Join(agentDir, "skills", "on-demand-one", "SKILL.md"),
		"---"+nl+"name: on-demand-one"+nl+"description: loaded by the Skill tool"+nl+"always_load: false"+nl+"---"+nl+"On-demand body text."+nl)

	got, err := ComposeSystemPromptForAgentFolder(agentDir)
	if err != nil {
		t.Fatalf("ComposeSystemPromptForAgentFolder: %v", err)
	}
	for _, want := range []string{
		"You are claude for this workspace.",
		"<AUTOLOADED_SKILLS>",
		`name="always-one"`,
		"Always-one body text.",
		"<WACKYPUB_SCRATCHPAD_PROTOCOL>",
	} {
		if !strings.Contains(got.Content, want) {
			t.Errorf("composed prompt missing %q", want)
		}
	}
	// The on-demand skill must NOT be inlined: it is offered through the Skill
	// tool instead, and inlining it would load it twice.
	if strings.Contains(got.Content, "On-demand body text.") {
		t.Errorf("always_load:false skill was inlined into the system prompt")
	}
	if got.SHA256 == "" {
		t.Errorf("empty digest")
	}
}

func TestComposePromptDigestDrivesSnapshotDecision(t *testing.T) {
	ws := t.TempDir()
	agentDir := filepath.Join(ws, "claude")
	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "first prompt"+nl)

	first, err := ComposeSystemPromptForAgentFolder(agentDir)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if first.ShouldSnapshot("") {
		t.Errorf("no previous digest must not mean snapshot")
	}
	if !first.ShouldSnapshot(first.SHA256) {
		t.Errorf("unchanged prompt must snapshot")
	}

	second, err := ComposeSystemPromptForAgentFolder(agentDir)
	if err != nil {
		t.Fatalf("compose 2: %v", err)
	}
	if second.SHA256 != first.SHA256 {
		t.Errorf("digest moved without a prompt change")
	}

	writeFile(t, filepath.Join(agentDir, "AGENTS.md"), "edited prompt"+nl)
	third, err := ComposeSystemPromptForAgentFolder(agentDir)
	if err != nil {
		t.Fatalf("compose 3: %v", err)
	}
	if third.ShouldSnapshot(first.SHA256) {
		t.Errorf("edited AGENTS.md must force a fresh system prompt, not a snapshot")
	}
}

func TestComposeSystemPromptRejectsRelativeFolder(t *testing.T) {
	if _, err := ComposeSystemPromptForAgentFolder("relative/claude"); err == nil {
		t.Errorf("expected an error for a relative agent folder")
	}
}

func TestParseSystemPromptMode(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    SystemPromptMode
		wantErr bool
	}{
		{"", ModeAppend, false},
		{"append", ModeAppend, false},
		{"replace", ModeReplace, false},
		{"swapsies", "", true},
	} {
		got, err := ParseSystemPromptMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseSystemPromptMode(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSystemPromptMode(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseSystemPromptMode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
