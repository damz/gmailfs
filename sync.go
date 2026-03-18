package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"
)

var syncCommand = &cli.Command{
	Name:  "sync",
	Usage: "Sync Gmail emails to a local directory",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "target",
			Aliases:  []string{"t"},
			Usage:    "target directory for synced emails",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "label",
			Aliases: []string{"l"},
			Usage:   "sync only this label (default: all)",
		},
		&cli.BoolFlag{
			Name:  "oneshot",
			Usage: "run once and exit (default: loop with history polling)",
		},
		&cli.DurationFlag{
			Name:  "sync-interval",
			Value: 5 * time.Minute,
			Usage: "history sync polling interval (loop mode)",
		},
	},
	Action: runSync,
}

func runSync(ctx context.Context, cmd *cli.Command) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := newApp(ctx, cmd.String("cache-dir"), cmd.String("config-dir"))
	if err != nil {
		return err
	}
	defer func() { _ = a.cache.Close() }()

	target := cmd.String("target")
	labelFilter := cmd.String("label")

	labels, err := filterLabels(a.labels, labelFilter)
	if err != nil {
		return err
	}

	// Initial full sync.
	if err := syncAllLabels(ctx, a, labels, target); err != nil {
		return err
	}

	if cmd.Bool("oneshot") {
		return nil
	}

	// Loop: wait for history changes, then sync affected labels.
	slog.Info("watching for changes", slog.Duration("interval", cmd.Duration("sync-interval")))
	ticker := time.NewTicker(cmd.Duration("sync-interval"))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("sync interrupted")
			return nil
		case <-ticker.C:
		}

		result, err := syncHistory(ctx, a.gmail, a.cache, a.index)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("sync interrupted")
				return nil
			}
			slog.Error("history sync failed", slog.Any("err", err))
			continue
		}

		if result.fullFlush {
			// History expired — re-sync everything.
			slog.Info("history expired, re-syncing all labels")
			if err := syncAllLabels(ctx, a, labels, target); err != nil {
				return err
			}
			continue
		}

		if len(result.labelDates) == 0 {
			continue
		}

		// Sync only affected labels.
		for _, label := range labels {
			if _, ok := result.labelDates[label.ID]; !ok {
				continue
			}
			if ctx.Err() != nil {
				slog.Info("sync interrupted")
				return nil
			}
			if err := syncLabel(ctx, a, label, target); err != nil {
				if ctx.Err() != nil {
					slog.Info("sync interrupted")
					return nil
				}
				slog.Error("syncing label failed",
					slog.String("label", label.Name), slog.Any("err", err))
			}
		}
	}
}

func filterLabels(all []LabelInfo, filter string) ([]LabelInfo, error) {
	if filter == "" {
		return all, nil
	}
	for _, l := range all {
		if l.Name == filter || sanitizeLabelName(l.Name) == filter {
			return []LabelInfo{l}, nil
		}
	}
	return nil, fmt.Errorf("label %q not found", filter)
}

func syncAllLabels(ctx context.Context, a *app, labels []LabelInfo, target string) error {
	for _, label := range labels {
		if ctx.Err() != nil {
			break
		}
		if err := syncLabel(ctx, a, label, target); err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("syncing label %s: %w", label.Name, err)
		}
	}

	if ctx.Err() != nil {
		slog.Info("sync interrupted")
		return nil
	}
	slog.Info("sync complete")
	return nil
}

type syncItem struct {
	stub MessageStub
	path string
}

func syncLabel(ctx context.Context, a *app, label LabelInfo, target string) error {
	labelDir := filepath.Join(target, sanitizeLabelName(label.Name))

	stubs, err := a.index.AllStubs(ctx, label.ID)
	if err != nil {
		return fmt.Errorf("building index: %w", err)
	}
	slog.Info("indexed messages", slog.String("label", label.Name), slog.Int("count", len(stubs)))

	var items []syncItem
	for _, stub := range stubs {
		t := time.UnixMilli(stub.InternalDate).Local()
		dayDir := filepath.Join(labelDir,
			strconv.Itoa(t.Year()),
			fmt.Sprintf("%02d", int(t.Month())),
			fmt.Sprintf("%02d", t.Day()))
		path := filepath.Join(dayDir, emlFilename(t, stub.Subject, stub.ID))

		if _, serr := os.Stat(path); serr == nil {
			continue
		}
		items = append(items, syncItem{stub: stub, path: path})
	}

	if len(items) == 0 {
		slog.Info("label up to date", slog.String("label", label.Name))
		return nil
	}

	slog.Info("downloading messages",
		slog.String("label", label.Name),
		slog.Int("new", len(items)),
		slog.Int("total", len(stubs)))

	const maxConcurrent = 20
	sem := make(chan struct{}, maxConcurrent)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		synced   int
	)

loop:
	for _, item := range items {
		mu.Lock()
		failed := firstErr != nil
		mu.Unlock()
		if failed {
			break
		}

		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		}

		wg.Go(func() {
			defer func() { <-sem }()
			if err := syncMessage(context.WithoutCancel(ctx), a, item); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			synced++
			n := synced
			mu.Unlock()

			if n%100 == 0 {
				slog.Info("progress",
					slog.String("label", label.Name),
					slog.Int("synced", n),
					slog.Int("remaining", len(items)-n))
			}
		})
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	slog.Info("label synced",
		slog.String("label", label.Name),
		slog.Int("new", synced),
		slog.Int("total", len(stubs)))
	return nil
}

func syncMessage(ctx context.Context, a *app, item syncItem) error {
	raw, cerr := a.cache.GetRawMessage(item.stub.ID)
	if cerr != nil || raw == nil {
		var err error
		raw, err = a.gmail.GetRawMessage(ctx, item.stub.ID)
		if err != nil {
			return fmt.Errorf("downloading message %s: %w", item.stub.ID, err)
		}
		if raw == nil {
			return nil // message not fetchable (e.g. 404), skip
		}
		if werr := a.cache.SetRawMessage(item.stub.ID, raw); werr != nil {
			slog.Warn("cache write error", slog.String("msgID", item.stub.ID), slog.Any("err", werr))
		}
	}

	dir := filepath.Dir(item.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	if err := os.WriteFile(item.path, raw, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", item.path, err)
	}

	t := time.UnixMilli(item.stub.InternalDate).Local()
	if err := os.Chtimes(item.path, t, t); err != nil {
		slog.Warn("failed to set file times", slog.String("path", item.path), slog.Any("err", err))
	}

	return nil
}
