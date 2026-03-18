package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type indexClient interface {
	ListAllMessageIDs(ctx context.Context, labelID string) ([]string, error)
	GetMessageStubs(ctx context.Context, messageIDs []string, cache stubCache) (map[string]MessageStub, error)
}

type indexUpdater interface {
	Update(labelID string, lc *LabelChanges, stubs map[string]MessageStub) error
	Flush(labelID string) error
	FlushAll() error
}

type LabelIndex struct {
	client indexClient
	cache  *Cache
	group  singleflight.Group
	locks  sync.Map // labelID → *sync.RWMutex
}

func NewLabelIndex(client indexClient, cache *Cache) *LabelIndex {
	return &LabelIndex{client: client, cache: cache}
}

func (idx *LabelIndex) labelMu(labelID string) *sync.RWMutex {
	v, _ := idx.locks.LoadOrStore(labelID, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}

// EnsureIndexed builds the index for a label if it isn't already complete.
// Concurrent callers for the same label are deduplicated via singleflight.
func (idx *LabelIndex) EnsureIndexed(ctx context.Context, labelID string) error {
	// Fast path: already done (no lock needed — reading a pebble key is safe).
	if idx.cache.IsLabelIndexComplete(labelID) {
		return nil
	}

	_, err, _ := idx.group.Do("ensure:"+labelID, func() (any, error) {
		mu := idx.labelMu(labelID)
		mu.Lock()
		defer mu.Unlock()

		// Double-check under write lock.
		if idx.cache.IsLabelIndexComplete(labelID) {
			return nil, nil
		}

		slog.Info("building index", slog.String("label", labelID))
		ids, err := idx.client.ListAllMessageIDs(ctx, labelID)
		if err != nil {
			return nil, fmt.Errorf("listing messages for index: %w", err)
		}

		if len(ids) == 0 {
			slog.Info("building index: done (empty label)", slog.String("label", labelID))
			return nil, idx.cache.SetLabelIndexComplete(labelID)
		}

		stubs, err := idx.client.GetMessageStubs(ctx, ids, idx.cache)
		if err != nil {
			return nil, fmt.Errorf("fetching stubs for index: %w", err)
		}

		type dayKey struct{ year, month, day int }
		dayGroups := make(map[dayKey][]string)
		for _, id := range ids {
			stub, ok := stubs[id]
			if !ok {
				continue
			}
			t := time.UnixMilli(stub.InternalDate).In(time.Local)
			dk := dayKey{t.Year(), int(t.Month()), t.Day()}
			dayGroups[dk] = append(dayGroups[dk], id)
		}

		for dk, msgIDs := range dayGroups {
			if err := idx.cache.SetDayIndex(labelID, dk.year, dk.month, dk.day, msgIDs); err != nil {
				return nil, fmt.Errorf("writing day index: %w", err)
			}
		}

		slog.Info("building index: done",
			slog.String("label", labelID),
			slog.Int("messages", len(stubs)),
			slog.Int("days", len(dayGroups)))
		return nil, idx.cache.SetLabelIndexComplete(labelID)
	})
	return err
}

// Update applies incremental label changes to the index. If the label is not
// yet fully indexed, the update is skipped (the in-progress build will capture
// the current state).
func (idx *LabelIndex) Update(labelID string, lc *LabelChanges, stubs map[string]MessageStub) error {
	mu := idx.labelMu(labelID)
	mu.Lock()
	defer mu.Unlock()

	if !idx.cache.IsLabelIndexComplete(labelID) {
		return nil
	}

	for msgID := range lc.Added {
		s, ok := stubs[msgID]
		if !ok {
			continue
		}
		t := time.UnixMilli(s.InternalDate).In(time.Local)
		if err := idx.cache.AddToDayIndex(labelID, t.Year(), int(t.Month()), t.Day(), msgID); err != nil {
			return err
		}
	}
	for msgID := range lc.Removed {
		s, ok := stubs[msgID]
		if !ok {
			return fmt.Errorf("cannot resolve stub for removed message %s", msgID)
		}
		t := time.UnixMilli(s.InternalDate).In(time.Local)
		if err := idx.cache.RemoveFromDayIndex(labelID, t.Year(), int(t.Month()), t.Day(), msgID); err != nil {
			return err
		}
	}
	return nil
}

// Flush clears all index data for a label.
func (idx *LabelIndex) Flush(labelID string) error {
	mu := idx.labelMu(labelID)
	mu.Lock()
	defer mu.Unlock()

	return idx.cache.FlushLabel(labelID)
}

// FlushAll clears all index data across all labels. Used for history expiry
// and timezone changes. Does not acquire per-label locks — see plan notes on
// acceptable staleness.
func (idx *LabelIndex) FlushAll() error {
	return idx.cache.FlushListings()
}

// Years returns the populated years for a label, building the index on demand.
func (idx *LabelIndex) Years(ctx context.Context, labelID string) ([]int, error) {
	if err := idx.EnsureIndexed(ctx, labelID); err != nil {
		return nil, err
	}

	mu := idx.labelMu(labelID)
	mu.RLock()
	defer mu.RUnlock()

	return idx.cache.IndexedYears(labelID)
}

// Months returns the populated months for a year, building the index on demand.
func (idx *LabelIndex) Months(ctx context.Context, labelID string, year int) ([]int, error) {
	if err := idx.EnsureIndexed(ctx, labelID); err != nil {
		return nil, err
	}

	mu := idx.labelMu(labelID)
	mu.RLock()
	defer mu.RUnlock()

	return idx.cache.IndexedMonths(labelID, year)
}

// Days returns the populated days for a year/month, building the index on demand.
func (idx *LabelIndex) Days(ctx context.Context, labelID string, year, month int) ([]int, error) {
	if err := idx.EnsureIndexed(ctx, labelID); err != nil {
		return nil, err
	}

	mu := idx.labelMu(labelID)
	mu.RLock()
	defer mu.RUnlock()

	return idx.cache.IndexedDays(labelID, year, month)
}

// AllStubs returns every message stub in the index for a label, building on demand.
func (idx *LabelIndex) AllStubs(ctx context.Context, labelID string) ([]MessageStub, error) {
	if err := idx.EnsureIndexed(ctx, labelID); err != nil {
		return nil, err
	}

	mu := idx.labelMu(labelID)
	mu.RLock()
	defer mu.RUnlock()

	years, err := idx.cache.IndexedYears(labelID)
	if err != nil {
		return nil, err
	}

	var all []MessageStub
	for _, y := range years {
		months, err := idx.cache.IndexedMonths(labelID, y)
		if err != nil {
			return nil, err
		}
		for _, m := range months {
			days, err := idx.cache.IndexedDays(labelID, y, m)
			if err != nil {
				return nil, err
			}
			for _, d := range days {
				stubs, err := idx.cache.DayStubsFromIndex(labelID, y, m, d)
				if err != nil {
					return nil, err
				}
				all = append(all, stubs...)
			}
		}
	}
	return all, nil
}

// DayStubs returns the message stubs for a specific day, building the index on demand.
func (idx *LabelIndex) DayStubs(ctx context.Context, labelID string, year, month, day int) ([]MessageStub, error) {
	if err := idx.EnsureIndexed(ctx, labelID); err != nil {
		return nil, err
	}

	mu := idx.labelMu(labelID)
	mu.RLock()
	defer mu.RUnlock()

	return idx.cache.DayStubsFromIndex(labelID, year, month, day)
}
