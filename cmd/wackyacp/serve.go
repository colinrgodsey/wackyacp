package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/colinrgodsey/wackyacp/internal/serve"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type serveOptions struct {
	agentFolder string
	wackyPubBin string
	host        string
	port        int
}

func parseServeArgs(args []string) (*serveOptions, error) {
	opts := &serveOptions{wackyPubBin: "wackypub"}
	var positionals []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case strings.HasPrefix(arg, "--agent-folder="):
			opts.agentFolder = strings.TrimPrefix(arg, "--agent-folder=")
		case arg == "--agent-folder":
			if i+1 < len(args) {
				opts.agentFolder = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--wackypub-bin="):
			opts.wackyPubBin = strings.TrimPrefix(arg, "--wackypub-bin=")
		case arg == "--wackypub-bin":
			if i+1 < len(args) {
				opts.wackyPubBin = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--host="):
			opts.host = strings.TrimPrefix(arg, "--host=")
		case arg == "--host":
			if i+1 < len(args) {
				opts.host = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--port="):
			p, err := strconv.Atoi(strings.TrimPrefix(arg, "--port="))
			if err != nil {
				return nil, fmt.Errorf("invalid --port: %s", strings.TrimPrefix(arg, "--port="))
			}
			opts.port = p
		case arg == "--port":
			if i+1 < len(args) {
				p, err := strconv.Atoi(args[i+1])
				if err != nil {
					return nil, fmt.Errorf("invalid --port: %s", args[i+1])
				}
				opts.port = p
				i++
			}
		case strings.HasPrefix(arg, "-"):
			return nil, fmt.Errorf("unknown flag: %s", arg)
		default:
			positionals = append(positionals, arg)
		}
	}

	// Filter out "serve" from positionals if it appears as the first command token
	var cleanPos []string
	for _, p := range positionals {
		if p == "serve" && len(cleanPos) == 0 {
			continue
		}
		cleanPos = append(cleanPos, p)
	}
	if len(cleanPos) > 0 {
		return nil, fmt.Errorf("unexpected argument: %s", cleanPos[0])
	}

	if opts.agentFolder == "" {
		return nil, fmt.Errorf("--agent-folder is required")
	}
	if opts.port < 0 {
		return nil, fmt.Errorf("--port must be non-negative")
	}
	if opts.host != "" && opts.port == 0 {
		return nil, fmt.Errorf("--host with --port 0: port 0 selects stdio mode and --host would be silently ignored; pass a nonzero --port for TCP")
	}
	return opts, nil
}

func runServe(args []string) error {
	opts, err := parseServeArgs(args)
	if err != nil {
		return err
	}

	// Validation posture matches the D117 bridge: absolute path only, no CWD
	// fallback, fail fast before anything is spawned.
	if !filepath.IsAbs(opts.agentFolder) {
		return fmt.Errorf("--agent-folder must be an absolute path: %q (no CWD fallback allowed)", opts.agentFolder)
	}
	st, err := os.Stat(opts.agentFolder)
	if err != nil {
		return fmt.Errorf("--agent-folder: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("--agent-folder is not a directory: %q", opts.agentFolder)
	}
	workspaceDir := filepath.Dir(opts.agentFolder)
	agentID := filepath.Base(opts.agentFolder)
	bin, err := exec.LookPath(opts.wackyPubBin)
	if err != nil {
		return fmt.Errorf("resolving wackypub binary %q: %w", opts.wackyPubBin, err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dialer := serve.NewDialer(ctx, bin, workspaceDir)
	defer func() { _ = dialer.Close() }()

	// The child spawns lazily on the first gRPC call (the first
	// session/prompt or session/load), not at startup or ACP initialize:
	// gRPC's context dialer fires on first use, and session/new is an
	// attach, not a spawn. The deferred Close reaps the child on shutdown;
	// runServeTCP's listener close is what keeps that path reachable from
	// SIGTERM.

	// The gRPC idle reaper must be disabled (timeout 0): the default 30-minute
	// idle closed the transport, which EOFs the child stdin and kills it - the
	// restart spiral observed in wackydiscord.
	gc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer.Dial),
		grpc.WithIdleTimeout(0),
	)
	if err != nil {
		_ = dialer.Close()
		return fmt.Errorf("constructing stdio grpc client: %w", err)
	}
	defer func() { _ = gc.Close() }()

	backend := serve.NewBackend(agentID, workspaceDir, opts.agentFolder, serve.NewGRPCWackypub(agentv1.NewAgentServiceClient(gc)), nil)
	if opts.port != 0 {
		return runServeTCP(ctx, backend, opts)
	}

	fmt.Fprintf(os.Stderr, "wackyacp serve: agent=%s workspace=%s session=%s transport=stdio\n",
		agentID, workspaceDir, serve.SessionID(opts.agentFolder))
	srv := serve.NewServer(os.Stdin, os.Stdout, backend, opts.agentFolder)
	return srv.Serve(ctx)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func runServeTCP(ctx context.Context, backend *serve.Backend, opts *serveOptions) error {
	host := opts.host
	if host == "" {
		host = "127.0.0.1"
	}
	if !isLoopbackHost(host) {
		fmt.Fprintln(os.Stderr, "wackyacp serve: WARNING: binding the ACP listener to a non-loopback host")
		fmt.Fprintf(os.Stderr, "wackyacp serve: WARNING: ACP sessions have no authentication; any process that can reach host %s can drive agent %q (read and write files, run tools) as the wackypub runtime user.\n",
			host, backend.AgentID())
	}
	addr := net.JoinHostPort(host, strconv.Itoa(opts.port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// The full detail reaches stderr via the returned error (main prints
		// it); a frontend that then dials the port sees a bare
		// connection-refused, so this is the only place the reason is visible.
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	defer func() { _ = ln.Close() }()

	// Accept blocks on a kernel socket and never observes ctx: without this,
	// SIGTERM is a no-op until the next connect arrives, the supervisor
	// escalates to SIGKILL, and the defers that reap the child are skipped
	// - orphaning it. Close the listener on shutdown so Accept returns
	// immediately and runServe's cleanup runs.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	fmt.Fprintf(os.Stderr, "wackyacp serve: agent=%s workspace=%s session=%s transport=tcp %s\n",
		backend.AgentID(), filepath.Dir(backend.AgentFolder()), serve.SessionID(backend.AgentFolder()), ln.Addr().String())

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Fprintf(os.Stderr, "wackyacp serve: accept: %v\n", err)
			continue
		}
		go func() {
			srv := serve.NewServer(conn, conn, backend, backend.AgentFolder())
			if err := srv.Serve(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "wackyacp serve: connection: %v\n", err)
			}
			_ = conn.Close()
		}()
	}
}
