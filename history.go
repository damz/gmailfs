package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type historyCache interface {
	stubCache
	GetHistoryID() (uint64, error)
	SetHistoryID(uint64) error
	FlushListings() error
	FlushLabel(string) error
	FlushDays(string, []time.Time) error
}

type syncResult struct {
	// labelDates maps label ID → affected local dates. A nil slice means
	// the label had deletions and the entire label was flushed.
	labelDates map[string][]time.Time
	// fullFlush is true when history expired and all listings were flushed.
	fullFlush bool
	// labelsChanged is true when the label list itself changed (added/removed).
	labelsChanged bool
}

func syncHistory(ctx context.Context, gmail historySyncer, cache historyCache) (syncResult, error) {
	storedID, err := cache.GetHistoryID()
	if err != nil {
		return syncResult{}, err
	}

	if storedID == 0 {
		// No history ID yet — seed from profile, no invalidation needed.
		profile, err := gmail.GetProfile(ctx)
		if err != nil {
			return syncResult{}, fmt.Errorf("getting profile for history ID: %w", err)
		}
		slog.Info("seeding history ID", slog.Uint64("historyId", profile.HistoryId))
		return syncResult{}, cache.SetHistoryID(profile.HistoryId)
	}

	changes, err := gmail.SyncHistory(ctx, storedID)
	if errors.Is(err, ErrHistoryExpired) {
		slog.Warn("history expired, flushing all caches")
		if err := cache.FlushListings(); err != nil {
			return syncResult{}, err
		}

		profile, err := gmail.GetProfile(ctx)
		if err != nil {
			return syncResult{}, fmt.Errorf("getting profile after history expiry: %w", err)
		}
		return syncResult{fullFlush: true}, cache.SetHistoryID(profile.HistoryId)
	}
	if err != nil {
		return syncResult{}, err
	}

	var result syncResult
	if len(changes.Labels) > 0 {
		slog.Info("history sync", slog.Int("affectedLabels", len(changes.Labels)), slog.Uint64("newHistoryId", changes.NewHistoryID))
		result.labelDates = make(map[string][]time.Time)

		// Collect all unique message IDs from labels that don't have deletions,
		// plus globally added messages (for All Mail resolution).
		allIDs := make(map[string]bool)
		for _, lc := range changes.Labels {
			if !lc.HasDeleted {
				for id := range lc.MessageIDs {
					allIDs[id] = true
				}
			}
		}
		for id := range changes.Added {
			allIDs[id] = true
		}

		var stubs map[string]MessageStub
		if len(allIDs) > 0 {
			ids := make([]string, 0, len(allIDs))
			for id := range allIDs {
				ids = append(ids, id)
			}
			stubs, err = gmail.GetMessageStubs(ctx, ids, cache)
			if err != nil {
				return syncResult{}, fmt.Errorf("getting message stubs: %w", err)
			}
		}

		for labelID, lc := range changes.Labels {
			if hiddenLabels[labelID] {
				continue
			}
			if lc.HasDeleted {
				slog.Info("flushing entire label (has deletions)", slog.String("label", labelID))
				if err := cache.FlushLabel(labelID); err != nil {
					return syncResult{}, fmt.Errorf("flushing label %s: %w", labelID, err)
				}
				result.labelDates[labelID] = nil
				continue
			}
			var affectedDates []time.Time
			for msgID := range lc.MessageIDs {
				if s, ok := stubs[msgID]; ok {
					affectedDates = append(affectedDates, time.UnixMilli(s.InternalDate).In(time.Local))
				}
			}
			if len(affectedDates) > 0 {
				slog.Info("flushing specific days", slog.String("label", labelID), slog.Int("days", len(affectedDates)))
				if err := cache.FlushDays(labelID, affectedDates); err != nil {
					return syncResult{}, fmt.Errorf("flushing days for label %s: %w", labelID, err)
				}
				result.labelDates[labelID] = affectedDates
			}
		}

		// Compute "All Mail" invalidation using only MessagesAdded/MessagesDeleted
		// events — LabelsAdded/LabelsRemoved don't change All Mail's content.
		var allMailDates []time.Time
		fullFlushAllMail := false

		// Deleted messages: resolve dates from stub cache (stubs are immutable
		// and survive message deletion). If any can't be resolved, full flush.
		for msgID := range changes.Deleted {
			stub, cerr := cache.GetMessageStub(msgID)
			if cerr != nil {
				fullFlushAllMail = true
				break
			}
			allMailDates = append(allMailDates, time.UnixMilli(stub.InternalDate).In(time.Local))
		}

		if !fullFlushAllMail {
			for msgID := range changes.Added {
				if s, ok := stubs[msgID]; ok {
					allMailDates = append(allMailDates, time.UnixMilli(s.InternalDate).In(time.Local))
				}
			}
		}

		if fullFlushAllMail {
			slog.Info("flushing All Mail (unresolvable deletion)")
			if err := cache.FlushLabel(AllMailLabelID); err != nil {
				return syncResult{}, fmt.Errorf("flushing All Mail: %w", err)
			}
			result.labelDates[AllMailLabelID] = nil
		} else if len(allMailDates) > 0 {
			slog.Info("flushing All Mail specific days", slog.Int("days", len(allMailDates)))
			if err := cache.FlushDays(AllMailLabelID, allMailDates); err != nil {
				return syncResult{}, fmt.Errorf("flushing All Mail days: %w", err)
			}
			result.labelDates[AllMailLabelID] = allMailDates
		}
	}
	return result, cache.SetHistoryID(changes.NewHistoryID)
}

func historySyncLoop(ctx context.Context, fsCtx *fsContext, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := syncHistory(ctx, fsCtx.gmail, fsCtx.cache)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("background history sync failed", slog.Any("err", err))
				continue
			}

			// labels.list costs 1 quota unit, so re-fetching every tick is cheap.
			newLabels, err := fsCtx.gmail.ListLabels(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("background label refresh failed", slog.Any("err", err))
			} else {
				result.labelsChanged = syncLabels(fsCtx, newLabels)
			}

			invalidateKernel(fsCtx, result)
		}
	}
}

func syncLabels(fsCtx *fsContext, newLabels []LabelInfo) bool {
	oldByID := make(map[string]string, len(fsCtx.labels))
	for _, l := range fsCtx.labels {
		oldByID[l.ID] = l.Name
	}
	newByID := make(map[string]string, len(newLabels))
	for _, l := range newLabels {
		newByID[l.ID] = l.Name
	}

	if len(oldByID) == len(newByID) {
		same := true
		for id, name := range oldByID {
			if newByID[id] != name {
				same = false
				break
			}
		}
		if same {
			return false
		}
	}

	for id := range oldByID {
		if _, ok := newByID[id]; !ok {
			slog.Info("label removed", slog.String("label", id), slog.String("name", oldByID[id]))
			if err := fsCtx.cache.FlushLabel(id); err != nil {
				slog.Error("flushing removed label", slog.String("label", id), slog.Any("err", err))
			}
		}
	}

	slog.Info("labels changed", slog.Int("old", len(fsCtx.labels)), slog.Int("new", len(newLabels)))
	fsCtx.labels = newLabels
	if err := fsCtx.cache.SetLabels(newLabels); err != nil {
		slog.Error("caching labels", slog.Any("err", err))
	}
	return true
}
