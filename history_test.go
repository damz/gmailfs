package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/gmail/v1"
)

type mockGmail struct {
	profile    *gmail.Profile
	profileErr error
	changes    *HistoryChanges
	changesErr error
	stubs      map[string]MessageStub
	stubsErr   error
}

func (m *mockGmail) GetProfile(_ context.Context) (*gmail.Profile, error) {
	return m.profile, m.profileErr
}

func (m *mockGmail) SyncHistory(_ context.Context, _ uint64) (*HistoryChanges, error) {
	return m.changes, m.changesErr
}

func (m *mockGmail) GetMessageStubs(_ context.Context, _ []string, _ stubCache) (map[string]MessageStub, error) {
	return m.stubs, m.stubsErr
}

type indexCall struct {
	method  string
	labelID string
}

type mockIndex struct {
	*LabelIndex
	calls []indexCall
}

func newMockIndex(t *testing.T) *mockIndex {
	t.Helper()
	c := newTestCache(t)
	return &mockIndex{LabelIndex: NewLabelIndex(nil, c)}
}

func (m *mockIndex) Update(labelID string, lc *LabelChanges, stubs map[string]MessageStub) error {
	m.calls = append(m.calls, indexCall{method: "Update", labelID: labelID})
	return m.LabelIndex.Update(labelID, lc, stubs)
}

func (m *mockIndex) Flush(labelID string) error {
	m.calls = append(m.calls, indexCall{method: "Flush", labelID: labelID})
	return m.LabelIndex.Flush(labelID)
}

func (m *mockIndex) FlushAll() error {
	m.calls = append(m.calls, indexCall{method: "FlushAll"})
	return m.LabelIndex.FlushAll()
}

type mockCache struct {
	*Cache
}

func newMockCache(t *testing.T) *mockCache {
	t.Helper()
	return &mockCache{Cache: newTestCache(t)}
}

func TestSyncHistorySeed(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	mock := &mockGmail{
		profile: &gmail.Profile{HistoryId: 1000},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.Zero(t, result.labelDates)
	require.False(t, result.fullFlush)
	require.Empty(t, mi.calls)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), id)
}

func TestSyncHistoryExpired(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changesErr: ErrHistoryExpired,
		profile:    &gmail.Profile{HistoryId: 2000},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.True(t, result.fullFlush)
	require.Equal(t, []indexCall{{method: "FlushAll"}}, mi.calls)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(2000), id)
}

func TestSyncHistoryNoChanges(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels:       map[string]*LabelChanges{},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 501,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.Nil(t, result.labelDates)
	require.Empty(t, mi.calls)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(501), id)
}

func TestSyncHistoryLabelAddition(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{"msg1": true},
					Removed: map[string]bool{},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli(), Subject: "Test"},
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, "INBOX")
	require.Len(t, result.labelDates["INBOX"], 1)

	// Should have Update calls for INBOX and All Mail.
	var inboxUpdate, allMailUpdate bool
	for _, c := range mi.calls {
		if c.method == "Update" && c.labelID == "INBOX" {
			inboxUpdate = true
		}
		if c.method == "Update" && c.labelID == AllMailLabelID {
			allMailUpdate = true
		}
	}
	require.True(t, inboxUpdate)
	require.True(t, allMailUpdate)
}

func TestSyncHistoryDeletionUnresolvable(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	// No stubs available — deletion is unresolvable.
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{},
					Removed: map[string]bool{"msg1": true},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, "INBOX")
	require.Nil(t, result.labelDates["INBOX"])

	// INBOX gets Flush (unresolvable deletion).
	var inboxFlush bool
	for _, c := range mi.calls {
		if c.method == "Flush" && c.labelID == "INBOX" {
			inboxFlush = true
		}
	}
	require.True(t, inboxFlush)
}

func TestSyncHistoryDeletionResolvable(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	// Stub is available (e.g. from cache) — deletion can be resolved to a date.
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{},
					Removed: map[string]bool{"msg1": true},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli(), Subject: "Deleted"},
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)

	// INBOX should get targeted dates, NOT a flush.
	require.Contains(t, result.labelDates, "INBOX")
	require.Len(t, result.labelDates["INBOX"], 1)

	for _, c := range mi.calls {
		if c.labelID == "INBOX" {
			require.Equal(t, "Update", c.method, "INBOX should only get Update, not Flush")
		}
	}
}

func TestSyncHistoryHiddenLabelsSkipped(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"UNREAD": {
					Added:   map[string]bool{"msg1": true},
					Removed: map[string]bool{},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: 1000},
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.NotContains(t, result.labelDates, "UNREAD")

	// Only All Mail Update should be present (no UNREAD calls).
	for _, c := range mi.calls {
		require.NotEqual(t, "UNREAD", c.labelID)
	}
}

func TestSyncHistoryAllMailAdded(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{"msg1": true},
					Removed: map[string]bool{},
				},
			},
			Added:        map[string]bool{"msg1": true},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli(), Subject: "New"},
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, AllMailLabelID)
	require.Len(t, result.labelDates[AllMailLabelID], 1)
}

func TestSyncHistoryAllMailDeleted(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	// Stub is resolvable (e.g. from cache) — date can be determined.
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{},
					Removed: map[string]bool{"msg1": true},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli()},
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)

	// All Mail should get dates (not a full flush) since stub was resolvable.
	require.Contains(t, result.labelDates, AllMailLabelID)
	require.Len(t, result.labelDates[AllMailLabelID], 1)
}

func TestSyncHistoryAllMailUnresolvableDeletion(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	// No cached stub for msg1 — date can't be resolved.
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{},
					Removed: map[string]bool{"msg1": true},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)

	// All Mail should get Flush since stub was unresolvable.
	var allMailFlush bool
	for _, c := range mi.calls {
		if c.method == "Flush" && c.labelID == AllMailLabelID {
			allMailFlush = true
		}
	}
	require.True(t, allMailFlush)
	require.Nil(t, result.labelDates[AllMailLabelID])
}

func TestSyncHistoryUpdatesIndex(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	// Pre-populate an index for INBOX.
	ts1 := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	require.NoError(t, mc.SetMessageStub(MessageStub{ID: "existing1", InternalDate: ts1.UnixMilli(), Subject: "Old"}))
	require.NoError(t, mi.cache.SetDayIndex("INBOX", 2026, 2, 15, []string{"existing1"}))
	require.NoError(t, mi.cache.SetLabelIndexComplete("INBOX"))

	ts2 := time.Date(2026, 2, 15, 14, 0, 0, 0, time.Local)
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{"msg2": true},
					Removed: map[string]bool{},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg2": {ID: "msg2", InternalDate: ts2.UnixMilli(), Subject: "New"},
		},
	}

	_, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)

	// Index should now contain both messages.
	ids, ierr := mi.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, ierr)
	require.ElementsMatch(t, []string{"existing1", "msg2"}, ids)
	require.True(t, mi.cache.IsLabelIndexComplete("INBOX"))
}

func TestSyncHistoryRemovesFromIndex(t *testing.T) {
	mc := newMockCache(t)
	mi := newMockIndex(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	require.NoError(t, mc.SetMessageStub(MessageStub{ID: "msg1", InternalDate: ts.UnixMilli(), Subject: "Old"}))
	require.NoError(t, mc.SetMessageStub(MessageStub{ID: "msg2", InternalDate: ts.UnixMilli(), Subject: "Keep"}))
	require.NoError(t, mi.cache.SetDayIndex("INBOX", 2026, 2, 15, []string{"msg1", "msg2"}))
	require.NoError(t, mi.cache.SetLabelIndexComplete("INBOX"))

	// msg1 removed from INBOX (label removed, not globally deleted).
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					Added:   map[string]bool{},
					Removed: map[string]bool{"msg1": true},
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli(), Subject: "Old"},
		},
	}

	_, err := syncHistory(context.Background(), mock, mc, mi)
	require.NoError(t, err)

	ids, ierr := mi.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, ierr)
	require.Equal(t, []string{"msg2"}, ids)
	require.True(t, mi.cache.IsLabelIndexComplete("INBOX"))
}
