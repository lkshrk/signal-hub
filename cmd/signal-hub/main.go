package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lkshrk/signal-hub/internal/app"
	"github.com/lkshrk/signal-hub/internal/config"
)

const defaultConfigPath = "/etc/signal-hub/config.yaml"

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	code := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr, ctx)
	stop()
	os.Exit(code)
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer, ctx context.Context) int {
	fs := flag.NewFlagSet("signal-hub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}
	if *showVersion {
		_, _ = fmt.Fprintln(stdout, version)
		return 0
	}
	if err := serve(ctx, getenv, stdout); err != nil {
		_, _ = fmt.Fprintln(stderr, "signal-hub:", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, getenv func(string) string, stdout io.Writer) error {
	log, err := newLogger(getenv, stdout)
	if err != nil {
		return err
	}
	path := getenv("SIGNAL_HUB_CONFIG")
	if path == "" {
		path = defaultConfigPath
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	cfg, err := config.Parse(data, config.Env{Getenv: getenv, ReadFile: os.ReadFile})
	if err != nil {
		return fmt.Errorf("config %s:\n%w", path, err)
	}
	a, err := app.New(cfg, log)
	if err != nil {
		return err
	}
	log.Info("starting", "version", version, "config", path)
	return a.Run(ctx)
}

func newLogger(getenv func(string) string, w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch v := getenv("SIGNAL_HUB_LOG_LEVEL"); v {
	case "debug":
		level = slog.LevelDebug
	case "", "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("SIGNAL_HUB_LOG_LEVEL: %q is not one of debug, info, warn, error", v)
	}
	opts := &slog.HandlerOptions{Level: level}
	switch v := getenv("SIGNAL_HUB_LOG_FORMAT"); v {
	case "", "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("SIGNAL_HUB_LOG_FORMAT: %q is not one of json, text", v)
	}
}
