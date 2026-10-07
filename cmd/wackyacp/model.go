package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/colinrgodsey/wackyacp/internal/acp"
	"github.com/colinrgodsey/wackyacp/internal/harness"
	"github.com/colinrgodsey/wackyacp/internal/session"
)

var errExit = errors.New("command failed")

type modelOptions struct {
	verb           string
	modelArg       string
	agentFolder    string
	harnessCmd     string
	harnessArgs    string
	permissionMode string
}

func parseModelArgs(args []string) (*modelOptions, error) {
	opts := &modelOptions{}
	var positionals []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		// Agent folder: --agent-folder is the canonical name (matches serve
		// mode); --workspace and -w are deprecated aliases kept for scripts.
		case strings.HasPrefix(arg, "--agent-folder="):
			opts.agentFolder = strings.TrimPrefix(arg, "--agent-folder=")
		case arg == "--agent-folder":
			if i+1 < len(args) {
				opts.agentFolder = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--workspace="):
			opts.agentFolder = strings.TrimPrefix(arg, "--workspace=")
		case arg == "--workspace" || arg == "-w":
			if i+1 < len(args) {
				opts.agentFolder = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--harness-cmd="):
			opts.harnessCmd = strings.TrimPrefix(arg, "--harness-cmd=")
		case arg == "--harness-cmd":
			if i+1 < len(args) {
				opts.harnessCmd = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--harness-args="):
			opts.harnessArgs = strings.TrimPrefix(arg, "--harness-args=")
		case arg == "--harness-args":
			if i+1 < len(args) {
				opts.harnessArgs = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--permission-mode="):
			opts.permissionMode = strings.TrimPrefix(arg, "--permission-mode=")
		case arg == "--permission-mode":
			if i+1 < len(args) {
				opts.permissionMode = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "-"):
			return nil, fmt.Errorf("unknown flag: %s", arg)
		default:
			positionals = append(positionals, arg)
		}
	}

	// Filter out "model" from positionals if it appears as the first command token
	var cleanPos []string
	for _, p := range positionals {
		if p == "model" && len(cleanPos) == 0 {
			continue
		}
		cleanPos = append(cleanPos, p)
	}

	if len(cleanPos) == 0 {
		return nil, fmt.Errorf("missing model subcommand (expected 'get' or 'set')")
	}

	opts.verb = cleanPos[0]
	if opts.verb != "get" && opts.verb != "set" {
		return nil, fmt.Errorf("unknown model subcommand %q (expected 'get' or 'set')", opts.verb)
	}

	if opts.verb == "set" {
		if len(cleanPos) > 1 {
			opts.modelArg = cleanPos[1]
		}
	}

	return opts, nil
}

func extractModelInfo(raw json.RawMessage, fallback string) (string, any) {
	info := acp.ExtractModelInfo(raw, fallback)
	return info.CurrentModel, info.RawOptions
}

// emitJSONError writes the human-facing line to stderr and the machine-facing
// error to stdout (the model CLI contract: stdout carries JSON only), then
// returns errExit so main exits non-zero.
func emitJSONError(detail string, err error) error {
	if detail == "" {
		fmt.Fprintf(os.Stderr, "wackyacp: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "wackyacp: %s: %v\n", detail, err)
	}
	errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
	fmt.Println(string(errJSON))
	return errExit
}

// emitJSONErrorWithCode is the emitJSONError variant for harness config-option
// rejections, which carry a JSON-RPC error code.
func emitJSONErrorWithCode(detail string, code int, msg string) error {
	fmt.Fprintf(os.Stderr, "wackyacp: %s: [%d] %s\n", detail, code, msg)
	errJSON, _ := json.Marshal(map[string]any{"error": msg, "code": code})
	fmt.Println(string(errJSON))
	return errExit
}

func runModel(args []string) error {
	opts, err := parseModelArgs(args)
	if err != nil {
		return emitJSONError("", err)
	}

	if opts.verb == "set" && opts.modelArg == "" {
		return emitJSONError("", errors.New("model cannot be empty"))
	}

	agentFolder := opts.agentFolder
	if agentFolder == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return emitJSONError("resolving working directory", err)
		}
		agentFolder = cwd
	}

	absFolder, err := filepath.Abs(agentFolder)
	if err != nil {
		return emitJSONError("resolving agent folder path", err)
	}
	agentFolder = absFolder

	harnessCfg, err := resolveHarnessConfig(agentFolder, opts.harnessCmd, opts.harnessArgs, opts.permissionMode)
	if err != nil {
		return emitJSONError("", err)
	}

	if _, err := harness.Resolve(harnessCfg.Command); err != nil {
		return emitJSONError("resolving harness", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	lock, err := session.AcquireLock(ctx, agentFolder)
	if err != nil {
		return emitJSONError("acquiring session lock", err)
	}
	defer func() {
		_ = lock.Release()
	}()

	savedSession, err := session.ReadSession(agentFolder)
	if err != nil {
		savedSession = nil
	}

	proc, err := harness.Start(ctx, harness.Config{
		Command:     harnessCfg.Command,
		Args:        harnessCfg.Args,
		AgentFolder: agentFolder,
		Stderr:      os.Stderr,
	})
	if err != nil {
		return emitJSONError("starting harness", err)
	}
	defer func() {
		_ = proc.Close()
	}()

	acpClient := acp.NewClient(proc.Stdin, proc.Stdout)
	acpClient.PermissionMode = harnessCfg.PermissionMode
	if _, err := acpClient.Initialize(ctx); err != nil {
		return emitJSONError("acp initialize failed", err)
	}

	sessionID, warnings, err := acpClient.EstablishSession(ctx, agentFolder, savedSession)
	if err != nil {
		return emitJSONError("establishing session", err)
	}
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "wackyacp: %v\n", w)
	}

	if opts.verb == "set" {
		rawResult, err := acpClient.SetConfigOption(ctx, sessionID, "model", opts.modelArg)
		if err != nil {
			var cfgErr *acp.ConfigOptionError
			if errors.As(err, &cfgErr) {
				return emitJSONErrorWithCode(acp.MethodSessionSetConfigOption+" failed", cfgErr.Code, cfgErr.Message)
			}
			return emitJSONError(acp.MethodSessionSetConfigOption+" failed", err)
		}

		confirmedModel, configOptions := extractModelInfo(rawResult, opts.modelArg)
		respJSON, err := json.Marshal(map[string]any{
			"model":         confirmedModel,
			"configOptions": configOptions,
		})
		if err != nil {
			return fmt.Errorf("encoding response: %w", err)
		}
		fmt.Println(string(respJSON))
		return nil
	}

	// opts.verb == "get"
	rawOptions := acpClient.LastConfigOptions()
	currentModel, configOptions := extractModelInfo(rawOptions, "")
	respJSON, err := json.Marshal(map[string]any{
		"model":         currentModel,
		"configOptions": configOptions,
	})
	if err != nil {
		return fmt.Errorf("encoding response: %w", err)
	}
	fmt.Println(string(respJSON))
	return nil
}
