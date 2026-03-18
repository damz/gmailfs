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

// resolveAffectedDates returns the local dates affected by the given label
// changes. For added messages, missing stubs are silently skipped (the message
// date is unknown but harmless). For removed messages, a missing stub means we
// can't determine which date to invalidate, so ok is false.
func resolveAffectedDates(lc *LabelChanges, stubs map[string]MessageStub) (dates []time.Time, ok bool) {
	for msgID := range lc.Added {
		if s, found := stubs[msgID]; found {
			dates = append(dates, time.UnixMilli(s.InternalDate).In(time.Local))
		}
	}
	for msgID := range lc.Removed {
		s, found := stubs[msgID]
		if !found {
			return nil, false
		}
		dates = append(dates, time.UnixMilli(s.InternalDate).In(time.Local))
	}
	return dates, true
}

// processLabelChanges resolves affected dates and updates the day index for a
// single label. When a removed message's stub can't be resolved, the entire
// label is flushed.
func processLabelChanges(labelID string, lc *LabelChanges, stubs map[string]MessageStub, index indexUpdater, result *syncResult) error {
	dates, ok := resolveAffectedDates(lc, stubs)
	if !ok {
		slog.Info("flushing label (unresolvable deletion)", slog.String("label", labelID))
		if err := index.Flush(labelID); err != nil {
			return fmt.Errorf("flushing label %s: %w", labelID, err)
		}
		result.labelDates[labelID] = nil
	} else if len(dates) > 0 {
		result.labelDates[labelID] = dates
	}

	if err := index.Update(labelID, lc, stubs); err != nil {
		slog.Warn("index update failed, flushing",
			slog.String("label", labelID), slog.Any("err", err))
		if ferr := index.Flush(labelID); ferr != nil {
			slog.Error("flush index failed", slog.String("label", labelID), slog.Any("err", ferr))
		}
	}
	return nil
}

func syncHistory(ctx context.Context, gmail historySyncer, cache historyCache, index indexUpdater) (syncResult, error) {
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
		if err := index.FlushAll(); err != nil {
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

		// Collect all unique message IDs that need stub resolution.
		allIDs := make(map[string]bool)
		for _, lc := range changes.Labels {
			for id := range lc.Added {
				allIDs[id] = true
			}
			for id := range lc.Removed {
				allIDs[id] = true
			}
		}
		for id := range changes.Added {
			allIDs[id] = true
		}
		for id := range changes.Deleted {
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

		// Process per-label changes.
		for labelID, lc := range changes.Labels {
			if hiddenLabels[labelID] {
				continue
			}
			if err := processLabelChanges(labelID, lc, stubs, index, &result); err != nil {
				return syncResult{}, err
			}
		}

		// All Mail uses MessagesAdded/MessagesDeleted events only —
		// LabelsAdded/LabelsRemoved don't change All Mail's content.
		allMailLC := &LabelChanges{
			Added:   changes.Added,
			Removed: changes.Deleted,
		}
		if err := processLabelChanges(AllMailLabelID, allMailLC, stubs, index, &result); err != nil {
			return syncResult{}, err
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
			result, err := syncHistory(ctx, fsCtx.gmail, fsCtx.cache, fsCtx.index)
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
			if err := fsCtx.index.Flush(id); err != nil {
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
