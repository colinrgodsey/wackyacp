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
	workspace      string
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
		case strings.HasPrefix(arg, "--workspace="):
			opts.workspace = strings.TrimPrefix(arg, "--workspace=")
		case arg == "--workspace" || arg == "-w":
			if i+1 < len(args) {
				opts.workspace = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--agent-folder="):
			opts.workspace = strings.TrimPrefix(arg, "--agent-folder=")
		case arg == "--agent-folder":
			if i+1 < len(args) {
				opts.workspace = args[i+1]
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
	if len(raw) == 0 {
		return fallback, []any{}
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fallback, raw
	}

	var optionsList []any
	if obj, ok := parsed.(map[string]any); ok {
		if co, exists := obj["configOptions"]; exists {
			if list, ok := co.([]any); ok {
				optionsList = list
			}
		}
	} else if list, ok := parsed.([]any); ok {
		optionsList = list
	}

	model := ""
	for _, item := range optionsList {
		if m, ok := item.(map[string]any); ok {
			id, _ := m["id"].(string)
			key, _ := m["key"].(string)
			cat, _ := m["category"].(string)
			if id == "model" || key == "model" || cat == "model" {
				if cv, ok := m["currentValue"].(string); ok && cv != "" {
					model = cv
					break
				}
				if sel, ok := m["selected"].(string); ok && sel != "" {
					model = sel
					break
				}
				if val, ok := m["value"].(string); ok && val != "" {
					model = val
					break
				}
				if opts, ok := m["options"].([]any); ok && len(opts) > 0 {
					if first, ok := opts[0].(map[string]any); ok {
						if v, ok := first["value"].(string); ok && v != "" {
							model = v
							break
						}
					}
				}
			}
		}
	}

	if model == "" {
		model = fallback
	}

	if len(optionsList) > 0 {
		return model, optionsList
	}
	return model, parsed
}

func runModel(args []string) error {
	opts, err := parseModelArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}

	if opts.verb == "set" && opts.modelArg == "" {
		fmt.Fprintf(os.Stderr, "wackyacp: model cannot be empty\n")
		errJSON, _ := json.Marshal(map[string]any{"error": "model cannot be empty"})
		fmt.Println(string(errJSON))
		return errExit
	}

	agentFolder := opts.workspace
	if agentFolder == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "wackyacp: resolving working directory: %v\n", err)
			errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
			fmt.Println(string(errJSON))
			return errExit
		}
		agentFolder = cwd
	}

	absFolder, err := filepath.Abs(agentFolder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: resolving agent folder path: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}
	agentFolder = absFolder

	harnessCfg, err := resolveHarnessConfig(agentFolder, opts.harnessCmd, opts.harnessArgs, opts.permissionMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}

	if _, err := harness.Resolve(harnessCfg.Command); err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: resolving harness: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	lock, err := session.AcquireLock(ctx, agentFolder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: acquiring session lock: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
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
		fmt.Fprintf(os.Stderr, "wackyacp: starting harness: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}
	defer func() {
		_ = proc.Close()
	}()

	acpClient := acp.NewClient(proc.Stdin, proc.Stdout)
	acpClient.PermissionMode = harnessCfg.PermissionMode
	if _, err := acpClient.Initialize(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: acp initialize failed: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}

	sessionID, err := acpClient.EstablishSession(ctx, agentFolder, savedSession)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wackyacp: establishing session: %v\n", err)
		errJSON, _ := json.Marshal(map[string]any{"error": err.Error()})
		fmt.Println(string(errJSON))
		return errExit
	}

	if opts.verb == "set" {
		rawResult, err := acpClient.SetConfigOption(ctx, sessionID, "model", opts.modelArg)
		if err != nil {
			var cfgErr *acp.ConfigOptionError
			if errors.As(err, &cfgErr) {
				fmt.Fprintf(os.Stderr, "wackyacp: session/setConfigOption failed: [%d] %s\n", cfgErr.Code, cfgErr.Message)
				errJSON, _ := json.Marshal(map[string]any{
					"error": cfgErr.Message,
					"code":  cfgErr.Code,
				})
				fmt.Println(string(errJSON))
			} else {
				fmt.Fprintf(os.Stderr, "wackyacp: session/setConfigOption failed: %v\n", err)
				errJSON, _ := json.Marshal(map[string]any{
					"error": err.Error(),
				})
				fmt.Println(string(errJSON))
			}
			return errExit
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
