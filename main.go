package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func run() error {
	mountpoint := flag.String("mountpoint", "", "FUSE mount point (required)")
	cacheDir := flag.String("cache-dir", "", "PebbleDB cache directory (default: ~/.cache/gmailfs)")
	configDir := flag.String("config-dir", "", "Config directory for credentials/token (default: ~/.config/gmailfs)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	fuseDebug := flag.Bool("fuse-debug", false, "Enable FUSE debug logging")
	syncInterval := flag.Duration("sync-interval", 30*time.Second, "History sync polling interval")
	flag.Parse()

	if *mountpoint == "" {
		fmt.Fprintln(os.Stderr, "usage: gmailfs -mountpoint <path> [-cache-dir <path>] [-config-dir <path>] [-debug] [-fuse-debug]")
		os.Exit(1)
	}

	if *debug {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("cannot determine home directory: %w", err)
	}

	if *cacheDir == "" {
		*cacheDir = filepath.Join(home, ".cache", "gmailfs")
	}
	if *configDir == "" {
		*configDir = filepath.Join(home, ".config", "gmailfs")
	}

	if err := os.MkdirAll(*mountpoint, 0o755); err != nil {
		return fmt.Errorf("cannot create mountpoint: %w", err)
	}
	if err := os.MkdirAll(*cacheDir, 0o755); err != nil {
		return fmt.Errorf("cannot create cache directory: %w", err)
	}
	if err := os.MkdirAll(*configDir, 0o755); err != nil {
		return fmt.Errorf("cannot create config directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	credPath := filepath.Join(*configDir, "credentials.json")
	tokPath := filepath.Join(*configDir, "token.json")
	httpClient, err := getOAuth2Client(ctx, credPath, tokPath)
	if err != nil {
		return fmt.Errorf("OAuth2 setup failed: %w", err)
	}

	gmailClient, err := NewGmailClient(ctx, httpClient)
	if err != nil {
		return fmt.Errorf("gmail client init failed: %w", err)
	}

	cache, err := NewCache(*cacheDir)
	if err != nil {
		return fmt.Errorf("cache init failed: %w", err)
	}
	defer func() { _ = cache.Close() }()

	tz := time.Now().Location().String()
	flushed, err := cache.CheckTimezone(tz)
	if err != nil {
		return fmt.Errorf("checking timezone: %w", err)
	}
	if flushed {
		slog.Info("timezone changed, flushed caches", slog.String("tz", tz))
	}

	labels, err := gmailClient.ListLabels(ctx)
	if err != nil {
		return fmt.Errorf("listing labels: %w", err)
	}
	if err := cache.SetLabels(labels); err != nil {
		return fmt.Errorf("caching labels: %w", err)
	}
	slog.Info("loaded labels", slog.Int("count", len(labels)))

	index := NewLabelIndex(gmailClient, cache)

	if _, err := syncHistory(ctx, gmailClient, cache, index); err != nil {
		return fmt.Errorf("history sync: %w", err)
	}

	fsCtx := &fsContext{
		gmail:  gmailClient,
		cache:  cache,
		labels: labels,
	}

	maxTimeout := time.Duration(math.MaxInt64)

	root := &rootNode{fsCtx: fsCtx}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			FsName:     "gmailfs",
			Name:       "gmailfs",
			Debug:      *fuseDebug,
			Options:    []string{"ro"},
		},
		AttrTimeout:     &maxTimeout,
		EntryTimeout:    &maxTimeout,
		NegativeTimeout: &maxTimeout,
	}

	server, err := fs.Mount(*mountpoint, root, opts)
	if err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}
	fsCtx.root = &root.Inode
	slog.Info("mounted", slog.String("mountpoint", *mountpoint))

	go historySyncLoop(ctx, fsCtx, *syncInterval)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("unmounting...")
		cancel()
		_ = server.Unmount()
	}()

	server.Wait()
	slog.Info("done")
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", slog.String("err", err.Error()))
		os.Exit(1)
	}
}
