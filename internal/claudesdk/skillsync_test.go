package claudesdk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func skillFixture(t *testing.T, agentDir, name string, alwaysLoad bool) {
	t.Helper()
	flag := "false"
	if alwaysLoad {
		flag = "true"
	}
	writeFile(t, filepath.Join(agentDir, "skills", name, "SKILL.md"),
		"---"+nl+"name: "+name+nl+"description: fixture "+name+nl+"always_load: "+flag+nl+"---"+nl+"body of "+name+nl)
}

func TestSyncOnDemandSkillsLinksOnlyOnDemand(t *testing.T) {
	agentDir := filepath.Join(t.TempDir(), "claude")
	skillFixture(t, agentDir, "always-one", true)
	skillFixture(t, agentDir, "on-demand-one", false)
	skillFixture(t, agentDir, "on-demand-two", false)

	res, err := SyncOnDemandSkills(agentDir)
	if err != nil {
		t.Fatalf("SyncOnDemandSkills: %v", err)
	}
	want := []string{"on-demand-one", "on-demand-two"}
	if len(res.Names) != len(want) {
		t.Fatalf("names = %v, want %v", res.Names, want)
	}
	for i := range want {
		if res.Names[i] != want[i] {
			t.Errorf("names[%d] = %q, want %q", i, res.Names[i], want[i])
		}
	}
	link := filepath.Join(agentDir, ".claude", "skills", "on-demand-one")
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("expected a symlink at %s: %v", link, err)
	}
	if _, err := os.Stat(filepath.Join(target, "SKILL.md")); err != nil {
		t.Errorf("symlink target has no SKILL.md: %v", err)
	}
	// An auto-loaded skill belongs in the prompt only. Linking it would let the
	// model load a second copy of text it already has.
	if _, err := os.Lstat(filepath.Join(agentDir, ".claude", "skills", "always-one")); !os.IsNotExist(err) {
		t.Errorf("always_load:true skill was linked into .claude/skills")
	}
}

func TestSyncOnDemandSkillsPrunesOnlyItsOwnLinks(t *testing.T) {
	agentDir := filepath.Join(t.TempDir(), "claude")
	skillFixture(t, agentDir, "on-demand-one", false)
	if _, err := SyncOnDemandSkills(agentDir); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	// Somebody else's directory in the same place must survive.
	manual := filepath.Join(agentDir, ".claude", "skills", "hand-made")
	writeFile(t, filepath.Join(manual, "SKILL.md"), "hand authored"+nl)

	if err := os.RemoveAll(filepath.Join(agentDir, "skills", "on-demand-one")); err != nil {
		t.Fatalf("remove skill: %v", err)
	}
	res, err := SyncOnDemandSkills(agentDir)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(res.Pruned) != 1 || res.Pruned[0] != "on-demand-one" {
		t.Errorf("pruned = %v, want [on-demand-one]", res.Pruned)
	}
	if _, err := os.Lstat(filepath.Join(agentDir, ".claude", "skills", "on-demand-one")); !os.IsNotExist(err) {
		t.Errorf("stale managed link survived")
	}
	if _, err := os.Stat(filepath.Join(manual, "SKILL.md")); err != nil {
		t.Errorf("pruning deleted a directory wackyacp did not create: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(agentDir, ".claude", "skills", manifestFileName))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var m managedManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("manifest unreadable: %v", err)
	}
	if len(m.Links) != 0 {
		t.Errorf("manifest still lists %v", m.Links)
	}
}

func TestSyncOnDemandSkillsReportsCollisionWithRealDirectory(t *testing.T) {
	agentDir := filepath.Join(t.TempDir(), "claude")
	skillFixture(t, agentDir, "collider", false)
	// A real directory already occupies the link slot for the same name.
	writeFile(t, filepath.Join(agentDir, ".claude", "skills", "collider", "SKILL.md"), "existing"+nl)

	res, err := SyncOnDemandSkills(agentDir)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "not a symlink") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a warning about the colliding directory, got %v", res.Warnings)
	}
	if _, err := os.Stat(filepath.Join(agentDir, ".claude", "skills", "collider", "SKILL.md")); err != nil {
		t.Errorf("collision handling destroyed existing content: %v", err)
	}
}
