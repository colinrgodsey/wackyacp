package claudesdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/colinrgodsey/wackypub/pkg/agent"
)

const (
	claudeSettingsDirName = ".claude"
	skillsDirName         = "skills"
	manifestFileName      = ".wackyacp-managed.json"
)

// SkillSyncResult carries what Claude should be told to load, plus what the
// sync did to disk.
type SkillSyncResult struct {
	// Names is the on-demand skill allowlist for Options.skills. Passing the
	// list rather than "all" is what keeps auto-loaded skills, already inlined
	// in the system prompt, from being offered a second time as Skill tools.
	Names []string
	// Dirs maps skill name to the directory Claude reads its SKILL.md from.
	Dirs map[string]string
	// Linked and Pruned name the symlinks this sync created and removed.
	Linked   []string
	Pruned   []string
	Warnings []string
}

// managedManifest records only the link names wackyacp itself created, so a
// later sync can retire its own links without ever touching a directory or
// symlink somebody made by hand.
type managedManifest struct {
	Links []string `json:"links"`
}

func manifestPath(agentFolder string) string {
	return filepath.Join(agentFolder, claudeSettingsDirName, skillsDirName, manifestFileName)
}

func readManifest(agentFolder string) managedManifest {
	var m managedManifest
	data, err := os.ReadFile(manifestPath(agentFolder))
	if err != nil {
		return managedManifest{}
	}
	_ = json.Unmarshal(data, &m)
	return m
}

// SyncOnDemandSkills mirrors the wackypub skill tree into the agent folder's
// .claude/skills so the SDK discovers it as project skills, and returns the
// allowlist of always_load:false skills.
//
// It never deletes anything it did not create: a stale entry is only removed
// when it is a symlink AND it is named in the managed manifest.
func SyncOnDemandSkills(agentFolder string) (*SkillSyncResult, error) {
	if !filepath.IsAbs(agentFolder) {
		return nil, fmt.Errorf("agent folder must be absolute: %q", agentFolder)
	}
	_, onDemand, _, shadowed, err := agent.DiscoverAgentSkillsMap(agentFolder)
	if err != nil {
		return nil, fmt.Errorf("discovering skills in %s: %w", agentFolder, err)
	}

	target := filepath.Join(agentFolder, claudeSettingsDirName, skillsDirName)
	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", target, err)
	}

	res := &SkillSyncResult{
		Dirs:     make(map[string]string, len(onDemand)),
		Warnings: append([]string(nil), shadowed...),
	}
	keep := make(map[string]bool, len(onDemand))

	for _, sk := range onDemand {
		if sk == nil || sk.Name == "" || sk.Path == "" {
			continue
		}
		src, err := filepath.EvalSymlinks(filepath.Dir(sk.Path))
		if err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("skill %s: cannot resolve %s: %v", sk.Name, sk.Path, err))
			continue
		}
		// The name is reserved even when its link slot turns out to be occupied by
		// something we did not create: the manifest records only names wackyacp
		// might have linked, and pruning additionally requires the path to be a
		// symlink, so an unlinked collision can never be deleted here.
		keep[sk.Name] = true
		res.Names = append(res.Names, sk.Name)
		res.Dirs[sk.Name] = src

		link := filepath.Join(target, sk.Name)
		existing, linkErr := os.Readlink(link)
		switch {
		case linkErr == nil && existing == src:
		case linkErr == nil:
			if err := os.Remove(link); err != nil {
				return nil, fmt.Errorf("replacing skill link %s: %w", link, err)
			}
			if err := os.Symlink(src, link); err != nil {
				return nil, fmt.Errorf("linking skill %s: %w", sk.Name, err)
			}
			res.Linked = append(res.Linked, sk.Name)
		case errors.Is(linkErr, fs.ErrNotExist):
			if err := os.Symlink(src, link); err != nil {
				return nil, fmt.Errorf("linking skill %s: %w", sk.Name, err)
			}
			res.Linked = append(res.Linked, sk.Name)
		default:
			// Something that is not a symlink: a hand-made directory or file.
			// Claude will read it; we report rather than delete.
			res.Warnings = append(res.Warnings, fmt.Sprintf("skill %s: %s exists and is not a symlink, left alone", sk.Name, link))
			continue
		}
	}

	prev := readManifest(agentFolder)
	for _, name := range prev.Links {
		if keep[name] {
			continue
		}
		link := filepath.Join(target, name)
		if _, err := os.Readlink(link); err == nil {
			if err := os.Remove(link); err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("pruning stale skill link %s: %v", link, err))
				continue
			}
			res.Pruned = append(res.Pruned, name)
		}
	}

	sortedNames := append([]string(nil), res.Names...)
	sort.Strings(sortedNames)
	res.Names = sortedNames

	managed := make([]string, 0, len(keep))
	for name := range keep {
		managed = append(managed, name)
	}
	sort.Strings(managed)
	data, err := json.MarshalIndent(managedManifest{Links: managed}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling skill manifest: %w", err)
	}
	if err := os.WriteFile(manifestPath(agentFolder), data, 0o644); err != nil {
		return nil, fmt.Errorf("writing skill manifest: %w", err)
	}

	return res, nil
}
