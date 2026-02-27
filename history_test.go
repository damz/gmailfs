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

type flushCall struct {
	method  string
	labelID string
	dates   []time.Time
}

type mockCache struct {
	*Cache
	flushes []flushCall
}

func newMockCache(t *testing.T) *mockCache {
	t.Helper()
	return &mockCache{Cache: newTestCache(t)}
}

func (m *mockCache) FlushListings() error {
	m.flushes = append(m.flushes, flushCall{method: "FlushListings"})
	return m.Cache.FlushListings()
}

func (m *mockCache) FlushLabel(labelID string) error {
	m.flushes = append(m.flushes, flushCall{method: "FlushLabel", labelID: labelID})
	return m.Cache.FlushLabel(labelID)
}

func (m *mockCache) FlushDays(labelID string, dates []time.Time) error {
	m.flushes = append(m.flushes, flushCall{method: "FlushDays", labelID: labelID, dates: dates})
	return m.Cache.FlushDays(labelID, dates)
}

func TestSyncHistorySeed(t *testing.T) {
	mc := newMockCache(t)
	mock := &mockGmail{
		profile: &gmail.Profile{HistoryId: 1000},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.Zero(t, result.labelDates)
	require.False(t, result.fullFlush)
	require.Empty(t, mc.flushes)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), id)
}

func TestSyncHistoryExpired(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changesErr: ErrHistoryExpired,
		profile:    &gmail.Profile{HistoryId: 2000},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.True(t, result.fullFlush)
	require.Equal(t, []flushCall{{method: "FlushListings"}}, mc.flushes)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(2000), id)
}

func TestSyncHistoryNoChanges(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels:       map[string]*LabelChanges{},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 501,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.Nil(t, result.labelDates)
	require.Empty(t, mc.flushes)

	id, err := mc.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(501), id)
}

func TestSyncHistoryLabelAddition(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					MessageIDs: map[string]bool{"msg1": true},
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

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, "INBOX")
	require.Len(t, result.labelDates["INBOX"], 1)

	require.Len(t, mc.flushes, 1)
	require.Equal(t, "FlushDays", mc.flushes[0].method)
	require.Equal(t, "INBOX", mc.flushes[0].labelID)
	require.Len(t, mc.flushes[0].dates, 1)
}

func TestSyncHistoryDeletion(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					MessageIDs: map[string]bool{"msg1": true},
					HasDeleted: true,
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{},
			NewHistoryID: 600,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, "INBOX")
	require.Nil(t, result.labelDates["INBOX"])

	require.Len(t, mc.flushes, 1)
	require.Equal(t, "FlushLabel", mc.flushes[0].method)
	require.Equal(t, "INBOX", mc.flushes[0].labelID)
}

func TestSyncHistoryHiddenLabelsSkipped(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"UNREAD": {
					MessageIDs: map[string]bool{"msg1": true},
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

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.NotContains(t, result.labelDates, "UNREAD")
	require.Empty(t, mc.flushes)
}

func TestSyncHistoryAllMailAdded(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					MessageIDs: map[string]bool{"msg1": true},
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

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)
	require.Contains(t, result.labelDates, AllMailLabelID)

	// Should have FlushDays for INBOX and FlushDays for All Mail.
	var allMailFlush *flushCall
	for i := range mc.flushes {
		if mc.flushes[i].labelID == AllMailLabelID {
			allMailFlush = &mc.flushes[i]
		}
	}
	require.NotNil(t, allMailFlush)
	require.Equal(t, "FlushDays", allMailFlush.method)
	require.Len(t, allMailFlush.dates, 1)
}

func TestSyncHistoryAllMailDeleted(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	ts := time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)
	// Pre-cache the stub so deleted message date can be resolved.
	require.NoError(t, mc.SetMessageStub(MessageStub{ID: "msg1", InternalDate: ts.UnixMilli()}))

	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					MessageIDs: map[string]bool{"msg1": true},
					HasDeleted: true,
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)

	// All Mail should get FlushDays (not FlushLabel) since stub was resolvable.
	var allMailFlush *flushCall
	for i := range mc.flushes {
		if mc.flushes[i].labelID == AllMailLabelID {
			allMailFlush = &mc.flushes[i]
		}
	}
	require.NotNil(t, allMailFlush)
	require.Equal(t, "FlushDays", allMailFlush.method)
	require.Len(t, allMailFlush.dates, 1)
	require.Contains(t, result.labelDates, AllMailLabelID)
}

func TestSyncHistoryAllMailUnresolvableDeletion(t *testing.T) {
	mc := newMockCache(t)
	require.NoError(t, mc.SetHistoryID(500))

	// No cached stub for msg1 — date can't be resolved.
	mock := &mockGmail{
		changes: &HistoryChanges{
			Labels: map[string]*LabelChanges{
				"INBOX": {
					MessageIDs: map[string]bool{"msg1": true},
					HasDeleted: true,
				},
			},
			Added:        map[string]bool{},
			Deleted:      map[string]bool{"msg1": true},
			NewHistoryID: 600,
		},
	}

	result, err := syncHistory(context.Background(), mock, mc)
	require.NoError(t, err)

	// All Mail should get FlushLabel since stub was unresolvable.
	var allMailFlush *flushCall
	for i := range mc.flushes {
		if mc.flushes[i].labelID == AllMailLabelID {
			allMailFlush = &mc.flushes[i]
		}
	}
	require.NotNil(t, allMailFlush)
	require.Equal(t, "FlushLabel", allMailFlush.method)
	require.Nil(t, result.labelDates[AllMailLabelID])
}
