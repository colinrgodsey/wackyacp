package claudesdk

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
)

// workerFS carries the Node sidecar inside the Go binary, so a deployed
// wackyacp needs no sidecar install step and cannot be paired with a worker
// script from a different build.
//
//go:embed worker/claude-sdk-worker.mjs
var workerFS embed.FS

// workerScript returns the embedded worker source.
func workerScript() (string, error) {
	data, err := workerFS.ReadFile("worker/" + workerFileName)
	if err != nil {
		return "", fmt.Errorf("claudesdk: embedded worker script missing: %w", err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("claudesdk: embedded worker script is empty")
	}
	return string(data), nil
}

// contentTag is the short digest used to name the materialized worker file.
func contentTag(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])[:12]
}
