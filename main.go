package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type app struct {
	gmail  *GmailClient
	cache  *Cache
	index  *LabelIndex
	labels []LabelInfo
}

func newApp(ctx context.Context, cacheDir, configDir string) (_ *app, retErr error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("cannot determine home directory: %w", err)
	}

	if cacheDir == "" {
		cacheDir = filepath.Join(home, ".cache", "gmailfs")
	}
	if configDir == "" {
		configDir = filepath.Join(home, ".config", "gmailfs")
	}

	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create cache directory: %w", err)
	}
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create config directory: %w", err)
	}

	credPath := filepath.Join(configDir, "credentials.json")
	tokPath := filepath.Join(configDir, "token.json")
	httpClient, err := getOAuth2Client(ctx, credPath, tokPath)
	if err != nil {
		return nil, fmt.Errorf("OAuth2 setup failed: %w", err)
	}

	gmailClient, err := NewGmailClient(ctx, httpClient)
	if err != nil {
		return nil, fmt.Errorf("gmail client init failed: %w", err)
	}

	cache, err := NewCache(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("cache init failed: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = cache.Close()
		}
	}()

	tz := time.Now().Location().String()
	flushed, err := cache.CheckTimezone(tz)
	if err != nil {
		return nil, fmt.Errorf("checking timezone: %w", err)
	}
	if flushed {
		slog.Info("timezone changed, flushed caches", slog.String("tz", tz))
	}

	labels, err := gmailClient.ListLabels(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing labels: %w", err)
	}
	if err := cache.SetLabels(labels); err != nil {
		return nil, fmt.Errorf("caching labels: %w", err)
	}
	slog.Info("loaded labels", slog.Int("count", len(labels)))

	index := NewLabelIndex(gmailClient, cache)

	if _, err := syncHistory(ctx, gmailClient, cache, index); err != nil {
		return nil, fmt.Errorf("history sync: %w", err)
	}

	return &app{gmail: gmailClient, cache: cache, index: index, labels: labels}, nil
}

func runMount(ctx context.Context, cmd *cli.Command) error {
	mountpoint := cmd.String("mountpoint")
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return fmt.Errorf("cannot create mountpoint: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	a, err := newApp(ctx, cmd.String("cache-dir"), cmd.String("config-dir"))
	if err != nil {
		return err
	}
	defer func() { _ = a.cache.Close() }()

	fsCtx := &fsContext{
		gmail:  a.gmail,
		cache:  a.cache,
		index:  a.index,
		labels: a.labels,
	}

	maxTimeout := time.Duration(math.MaxInt64)
	root := &rootNode{fsCtx: fsCtx}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			FsName:     "gmailfs",
			Name:       "gmailfs",
			Debug:      cmd.Bool("fuse-debug"),
			Options:    []string{"ro"},
		},
		AttrTimeout:     &maxTimeout,
		EntryTimeout:    &maxTimeout,
		NegativeTimeout: &maxTimeout,
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}
	fsCtx.root = &root.Inode
	slog.Info("mounted", slog.String("mountpoint", mountpoint))

	go historySyncLoop(ctx, fsCtx, cmd.Duration("sync-interval"))

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
	root := &cli.Command{
		Name:  "gmailfs",
		Usage: "Gmail filesystem and sync tool",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "cache-dir",
				Usage: "PebbleDB cache directory (default: ~/.cache/gmailfs)",
			},
			&cli.StringFlag{
				Name:  "config-dir",
				Usage: "Config directory for credentials/token (default: ~/.config/gmailfs)",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "enable debug logging",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Bool("debug") {
				slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
			}
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:  "mount",
				Usage: "Mount Gmail as a FUSE filesystem",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "mountpoint",
						Aliases:  []string{"m"},
						Usage:    "FUSE mount point",
						Required: true,
					},
					&cli.BoolFlag{
						Name:  "fuse-debug",
						Usage: "enable FUSE debug logging",
					},
					&cli.DurationFlag{
						Name:  "sync-interval",
						Value: 30 * time.Second,
						Usage: "history sync polling interval",
					},
				},
				Action: runMount,
			},
			syncCommand,
		},
	}

	if err := root.Run(context.Background(), os.Args); err != nil {
		slog.Error("fatal", slog.String("err", err.Error()))
		os.Exit(1)
	}
}
