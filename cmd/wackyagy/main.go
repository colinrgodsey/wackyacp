// Command wackyagy exposes Google Antigravity CLI (agy) as an ACP agent over
// newline-delimited JSON-RPC 2.0 on stdin/stdout.
//
// Every path the bridge touches is a flag, so a test can point it at fixtures
// instead of a real agy install.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/anmitsu/go-shlex"

	"github.com/colinrgodsey/wackyacp/internal/agy"
)

const (
	version                 = "0.1.0"
	defaultAgyBin           = "/usr/local/bin/agy"
	defaultConversationsDir = ".gemini/antigravity-cli/conversations"
	defaultLogDir           = ".gemini/antigravity-cli/log"
	defaultStateDir         = ".wackyacp/agy-acp"
)

type options struct {
	agyBin           string
	workDir          string
	conversationsDir string
	stateDir         string
	logDir           string
	extraArgs        string
	printTimeout     string
	pollInterval     time.Duration
	showNarration    bool
	permissionMode   string
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "wackyagy: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	opts, showVersion, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	if showVersion {
		fmt.Fprintf(stdout, "wackyagy %s\n", version)
		return nil
	}

	cfg, err := buildConfig(opts, stderr)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return agy.NewBridge(cfg).Serve(ctx, stdin, stdout)
}

func parseFlags(args []string, stderr io.Writer) (opts options, showVersion bool, err error) {
	flags := flag.NewFlagSet("wackyagy", flag.ContinueOnError)
	flags.SetOutput(stderr)

	flags.StringVar(&opts.agyBin, "agy-bin", "", "agy executable (default "+defaultAgyBin+", falling back to agy on PATH)")
	flags.StringVar(&opts.workDir, "workdir", "", "working directory for agy and the directory shared with it (default: process cwd)")
	flags.StringVar(&opts.conversationsDir, "conversations-dir", "", "directory holding agy conversation databases (default ~/."+"gemini/antigravity-cli/conversations)")
	flags.StringVar(&opts.stateDir, "state-dir", "", "directory for bridge session state (default ~/."+"wackyacp/agy-acp)")
	flags.StringVar(&opts.logDir, "log-dir", "", "directory holding agy cli-*.log files (default ~/."+"gemini/antigravity-cli/log)")
	flags.StringVar(&opts.extraArgs, "extra-args", envOr("AGY_EXTRA_ARGS", ""), "extra agy arguments, shell-quoted (env AGY_EXTRA_ARGS)")
	flags.StringVar(&opts.printTimeout, "print-timeout", "20m", "agy --print-timeout value")
	flags.DurationVar(&opts.pollInterval, "poll-interval", 100*time.Millisecond, "how often to re-read the conversation database")
	flags.BoolVar(&opts.showNarration, "show-narration", envFlag("AGY_SHOW_NARRATION"), "keep agy's leading \"I will ...\" planning lines in the output (env AGY_SHOW_NARRATION)")
	flags.StringVar(&opts.permissionMode, "permission-mode", "deny", "How tool permission requests are handled: deny (agy default, which headless agy satisfies by denying) or approve (pass agy --dangerously-skip-permissions)")
	flags.BoolVar(&showVersion, "version", false, "print version and exit")

	if err := flags.Parse(args); err != nil {
		return opts, false, fmt.Errorf("parsing flags: %w", err)
	}
	return opts, showVersion, nil
}

func buildConfig(opts options, stderr io.Writer) (agy.Config, error) {
	agyBin, err := resolveAgyBin(opts.agyBin)
	if err != nil {
		return agy.Config{}, err
	}

	workDir := opts.workDir
	if workDir == "" {
		detected, err := os.Getwd()
		if err != nil {
			return agy.Config{}, fmt.Errorf("determining working directory: %w", err)
		}
		workDir = detected
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return agy.Config{}, fmt.Errorf("determining home directory for default paths: %w", err)
	}

	conversationsDir := defaultIfEmpty(opts.conversationsDir, filepath.Join(home, defaultConversationsDir))
	stateDir := defaultIfEmpty(opts.stateDir, filepath.Join(home, defaultStateDir))
	logDir := defaultIfEmpty(opts.logDir, filepath.Join(home, defaultLogDir))

	for name, path := range map[string]string{
		"--workdir":           workDir,
		"--conversations-dir": conversationsDir,
		"--state-dir":         stateDir,
		"--log-dir":           logDir,
	} {
		if !filepath.IsAbs(path) {
			return agy.Config{}, fmt.Errorf("%s must be an absolute path, got %q", name, path)
		}
		if err := rejectFile(path, name); err != nil {
			return agy.Config{}, err
		}
	}

	if opts.pollInterval <= 0 {
		return agy.Config{}, fmt.Errorf("--poll-interval must be positive, got %s", opts.pollInterval)
	}
	if opts.printTimeout == "" {
		return agy.Config{}, fmt.Errorf("--print-timeout must not be empty")
	}

	switch opts.permissionMode {
	case agy.PermissionDeny, agy.PermissionApprove:
	default:
		return agy.Config{}, fmt.Errorf("--permission-mode must be %q or %q, got %q", agy.PermissionDeny, agy.PermissionApprove, opts.permissionMode)
	}

	var extraArgs []string
	if strings.TrimSpace(opts.extraArgs) != "" {
		parsed, err := shlex.Split(opts.extraArgs, true)
		if err != nil {
			return agy.Config{}, fmt.Errorf("parsing --extra-args %q: %w", opts.extraArgs, err)
		}
		extraArgs = parsed
	}

	return agy.Config{
		AgyBin:           agyBin,
		WorkingDir:       workDir,
		ConversationsDir: conversationsDir,
		StateDir:         stateDir,
		LogDir:           logDir,
		ExtraArgs:        extraArgs,
		PrintTimeout:     opts.printTimeout,
		PollInterval:     opts.pollInterval,
		ShowNarration:    opts.showNarration,
		PermissionMode:   opts.permissionMode,
		Version:          version,
		Stderr:           stderr,
	}, nil
}

// resolveAgyBin accepts an explicit path or falls back from the install location
// to PATH, failing when neither yields a usable executable.
func resolveAgyBin(flagValue string) (string, error) {
	if flagValue != "" {
		if isExecutable(flagValue) {
			return flagValue, nil
		}
		return "", fmt.Errorf("--agy-bin %s is not an executable file", flagValue)
	}
	if isExecutable(defaultAgyBin) {
		return defaultAgyBin, nil
	}
	found, err := exec.LookPath("agy")
	if err != nil {
		return "", fmt.Errorf("agy not found: neither %s nor agy on PATH works (%w)", defaultAgyBin, err)
	}
	return found, nil
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode().Perm()&0o111 != 0
}

func rejectFile(path, flagName string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspecting %s %s: %w", flagName, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %s exists but is not a directory", flagName, path)
	}
	return nil
}

func defaultIfEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func envFlag(key string) bool {
	value := os.Getenv(key)
	return value == "1" || strings.EqualFold(value, "true")
}
