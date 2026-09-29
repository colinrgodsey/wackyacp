package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/anmitsu/go-shlex"
)

// ResolvedHarness holds the harness command, arguments, and permission mode for an agent folder.
type ResolvedHarness struct {
	Command        string
	Args           []string
	PermissionMode string
}

// samePath checks if two paths refer to the same location, resolving symlinks if needed.
func samePath(a, b string) bool {
	if filepath.Clean(a) == filepath.Clean(b) {
		return true
	}
	realA, errA := filepath.EvalSymlinks(a)
	realB, errB := filepath.EvalSymlinks(b)
	if errA == nil && errB == nil {
		return filepath.Clean(realA) == filepath.Clean(realB)
	}
	return false
}

// findManifest searches for REMOTE_MANIFEST starting from startDir and walking upwards.
func findManifest(startDir string) string {
	curr := startDir
	for {
		candidate := filepath.Join(curr, "REMOTE_MANIFEST")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(curr)
		if parent == curr {
			break
		}
		curr = parent
	}
	return ""
}

// resolveHarnessFromManifest finds and parses the route for agentFolder in any ancestor REMOTE_MANIFEST.
func resolveHarnessFromManifest(agentFolder string) (*ResolvedHarness, error) {
	manifestPath := findManifest(agentFolder)
	if manifestPath == "" {
		return nil, fmt.Errorf("REMOTE_MANIFEST not found in ancestor directories of %s", agentFolder)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", manifestPath, err)
	}

	cleanFolder := filepath.Clean(agentFolder)
	folderBase := filepath.Base(cleanFolder)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		idx := strings.Index(line, ":")
		if idx == -1 {
			continue
		}

		agentID := strings.TrimSpace(line[:idx])
		cmdPart := strings.TrimSpace(line[idx+1:])
		tokens, err := shlex.Split(cmdPart, true)
		if err != nil || len(tokens) == 0 {
			continue
		}

		// Check if this route matches agentFolder:
		// 1. By --agent-folder flag in tokens
		// 2. Or by agentID matching folder basename
		matches := false
		var routeAgentFolder string
		for i, tok := range tokens {
			if strings.HasPrefix(tok, "--agent-folder=") {
				routeAgentFolder = strings.TrimPrefix(tok, "--agent-folder=")
			} else if tok == "--agent-folder" && i+1 < len(tokens) {
				routeAgentFolder = tokens[i+1]
			}
		}

		if routeAgentFolder != "" && samePath(routeAgentFolder, cleanFolder) {
			matches = true
		} else if agentID == folderBase {
			matches = true
		}

		if matches {
			// Extract harness flags from tokens
			var harnessCmd string
			var harnessArgs string
			permissionMode := "deny" // default

			for i, tok := range tokens {
				if strings.HasPrefix(tok, "--harness-cmd=") {
					harnessCmd = strings.TrimPrefix(tok, "--harness-cmd=")
				} else if tok == "--harness-cmd" && i+1 < len(tokens) {
					harnessCmd = tokens[i+1]
				} else if strings.HasPrefix(tok, "--harness-args=") {
					harnessArgs = strings.TrimPrefix(tok, "--harness-args=")
				} else if tok == "--harness-args" && i+1 < len(tokens) {
					harnessArgs = tokens[i+1]
				} else if strings.HasPrefix(tok, "--permission-mode=") {
					permissionMode = strings.TrimPrefix(tok, "--permission-mode=")
				} else if tok == "--permission-mode" && i+1 < len(tokens) {
					permissionMode = tokens[i+1]
				}
			}

			if harnessCmd == "" {
				return nil, fmt.Errorf("%s:%d: route for %s is missing --harness-cmd", manifestPath, lineNum, agentID)
			}

			var args []string
			if harnessArgs != "" {
				parsed, err := shlex.Split(harnessArgs, true)
				if err != nil {
					return nil, fmt.Errorf("parsing harness-args %q: %w", harnessArgs, err)
				}
				args = parsed
			}

			return &ResolvedHarness{
				Command:        harnessCmd,
				Args:           args,
				PermissionMode: permissionMode,
			}, nil
		}
	}

	return nil, fmt.Errorf("no route matching agent folder %s found in %s", agentFolder, manifestPath)
}

// resolveHarnessConfig resolves harness config from explicit flags if provided,
// falling back to ancestor REMOTE_MANIFEST resolution.
func resolveHarnessConfig(agentFolder, explicitCmd, explicitArgs, explicitPerm string) (*ResolvedHarness, error) {
	if explicitCmd != "" {
		var args []string
		if explicitArgs != "" {
			parsed, err := shlex.Split(explicitArgs, true)
			if err != nil {
				return nil, fmt.Errorf("parsing --harness-args %q: %w", explicitArgs, err)
			}
			args = parsed
		}
		perm := explicitPerm
		if perm == "" {
			perm = "deny"
		}
		return &ResolvedHarness{
			Command:        explicitCmd,
			Args:           args,
			PermissionMode: perm,
		}, nil
	}

	res, err := resolveHarnessFromManifest(agentFolder)
	if err != nil {
		return nil, err
	}
	if explicitPerm != "" {
		res.PermissionMode = explicitPerm
	}
	if explicitArgs != "" {
		parsed, err := shlex.Split(explicitArgs, true)
		if err != nil {
			return nil, fmt.Errorf("parsing --harness-args %q: %w", explicitArgs, err)
		}
		res.Args = parsed
	}
	return res, nil
}
