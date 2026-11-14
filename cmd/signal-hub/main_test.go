package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lkshrk/signal-hub/internal/testupstream"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestMainVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-version"}, envFrom(nil), &stdout, &stderr, context.Background()); code != 0 {
		t.Fatalf("code = %d, stderr %q", code, stderr.String())
	}
	if stdout.String() != "dev\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestMainBadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeFile(t, path, "version: 2\n")
	var stdout, stderr bytes.Buffer
	code := run(nil, envFrom(map[string]string{"SIGNAL_HUB_CONFIG": path}), &stdout, &stderr, context.Background())
	if code != 1 || !strings.Contains(stderr.String(), "version: must be 1") {
		t.Fatalf("code = %d, stderr %q", code, stderr.String())
	}
}

func TestMainMissingConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "absent.yaml")
	code := run(nil, envFrom(map[string]string{"SIGNAL_HUB_CONFIG": path}), &stdout, &stderr, context.Background())
	if code != 1 || !strings.Contains(stderr.String(), path) {
		t.Fatalf("code = %d, stderr %q", code, stderr.String())
	}
}

func TestMainBadLogSettings(t *testing.T) {
	for key, value := range map[string]string{"SIGNAL_HUB_LOG_LEVEL": "loud", "SIGNAL_HUB_LOG_FORMAT": "xml"} {
		t.Run(key, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(nil, envFrom(map[string]string{key: value}), &stdout, &stderr, context.Background())
			if code != 1 || !strings.Contains(stderr.String(), key) {
				t.Fatalf("code = %d, stderr %q", code, stderr.String())
			}
		})
	}
}

func TestMainRunsUntilCancelledWithoutLoggingTheAccount(t *testing.T) {
	up := testupstream.New(t)
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "tokens", "dsh"), strings.Repeat("d", 32))
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, fmt.Sprintf(`version: 1
upstream:
  url: %s
listen: %s
adminListen: %s
database:
  url: sqlite://%s/data/hub.db
tokenDir: %s/tokens
consumers:
  - name: dsh
    groups: ["AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="]
`, up.HTTPURL(), freeAddr(t), freeAddr(t), dir, dir))

	env := map[string]string{
		"SIGNAL_HUB_CONFIG":     path,
		"SIGNAL_HUB_ACCOUNT":    "+4915112345678",
		"SIGNAL_HUB_LOG_LEVEL":  "debug",
		"SIGNAL_HUB_LOG_FORMAT": "text",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(nil, envFrom(env), stdout, stderr, ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(stdout.String(), "upstream connected") {
		select {
		case code := <-done:
			t.Fatalf("run exited early with %d: %s %s", code, stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("never connected: %s", stdout.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code = %d, stderr %q", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	out := stdout.String() + stderr.String()
	if !strings.Contains(out, "msg=starting") {
		t.Fatalf("no startup line: %s", out)
	}
	if strings.Contains(out, "4915112345678") {
		t.Fatalf("output contains the account number:\n%s", out)
	}
}
