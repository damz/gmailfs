package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeIndexClient struct {
	ids      map[string][]string
	stubs    map[string]MessageStub
	listErr  error
	stubsErr error
}

func (f *fakeIndexClient) ListAllMessageIDs(_ context.Context, labelID string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.ids[labelID], nil
}

func (f *fakeIndexClient) GetMessageStubs(_ context.Context, messageIDs []string, cache stubCache) (map[string]MessageStub, error) {
	if f.stubsErr != nil {
		return nil, f.stubsErr
	}
	result := make(map[string]MessageStub, len(messageIDs))
	for _, id := range messageIDs {
		if s, ok := f.stubs[id]; ok {
			result[id] = s
			if cache != nil {
				_ = cache.SetMessageStub(s)
			}
		}
	}
	return result, nil
}

func newTestIndex(t *testing.T, client indexClient) *LabelIndex {
	t.Helper()
	c := newTestCache(t)
	return NewLabelIndex(client, c)
}

func TestEnsureIndexedBuildsWhenNotPresent(t *testing.T) {
	ts1 := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	ts2 := time.Date(2026, 2, 27, 14, 0, 0, 0, time.Local)
	client := &fakeIndexClient{
		ids: map[string][]string{
			"INBOX": {"msg1", "msg2"},
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts1.UnixMilli(), Subject: "First"},
			"msg2": {ID: "msg2", InternalDate: ts2.UnixMilli(), Subject: "Second"},
		},
	}
	idx := newTestIndex(t, client)

	require.NoError(t, idx.EnsureIndexed(context.Background(), "INBOX"))
	require.True(t, idx.cache.IsLabelIndexComplete("INBOX"))

	ids, err := idx.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg1"}, ids)

	ids, err = idx.cache.GetDayIndex("INBOX", 2026, 2, 27)
	require.NoError(t, err)
	require.Equal(t, []string{"msg2"}, ids)
}

func TestEnsureIndexedNoOpWhenComplete(t *testing.T) {
	client := &fakeIndexClient{
		ids: map[string][]string{
			"INBOX": {"msg1"},
		},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: 1000},
		},
	}
	idx := newTestIndex(t, client)

	// Mark already complete.
	require.NoError(t, idx.cache.SetLabelIndexComplete("INBOX"))

	// Should be a no-op (wouldn't have stubs to build anyway if client was nil).
	require.NoError(t, idx.EnsureIndexed(context.Background(), "INBOX"))
}

func TestEnsureIndexedEmptyLabel(t *testing.T) {
	client := &fakeIndexClient{
		ids: map[string][]string{
			"INBOX": {},
		},
	}
	idx := newTestIndex(t, client)

	require.NoError(t, idx.EnsureIndexed(context.Background(), "INBOX"))
	require.True(t, idx.cache.IsLabelIndexComplete("INBOX"))
}

func TestYearsMonthsDaysDayStubs(t *testing.T) {
	ts1 := time.Date(2024, 3, 10, 8, 0, 0, 0, time.Local)
	ts2 := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	ts3 := time.Date(2026, 2, 15, 14, 0, 0, 0, time.Local)
	ts4 := time.Date(2026, 7, 1, 9, 0, 0, 0, time.Local)
	client := &fakeIndexClient{
		ids: map[string][]string{
			"INBOX": {"a", "b", "c", "d"},
		},
		stubs: map[string]MessageStub{
			"a": {ID: "a", InternalDate: ts1.UnixMilli(), Subject: "A"},
			"b": {ID: "b", InternalDate: ts2.UnixMilli(), Subject: "B"},
			"c": {ID: "c", InternalDate: ts3.UnixMilli(), Subject: "C"},
			"d": {ID: "d", InternalDate: ts4.UnixMilli(), Subject: "D"},
		},
	}
	idx := newTestIndex(t, client)
	ctx := context.Background()

	years, err := idx.Years(ctx, "INBOX")
	require.NoError(t, err)
	require.Equal(t, []int{2024, 2026}, years)

	months, err := idx.Months(ctx, "INBOX", 2026)
	require.NoError(t, err)
	require.Equal(t, []int{2, 7}, months)

	days, err := idx.Days(ctx, "INBOX", 2026, 2)
	require.NoError(t, err)
	require.Equal(t, []int{15}, days)

	stubs, err := idx.DayStubs(ctx, "INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Len(t, stubs, 2)
	// Sorted by InternalDate.
	require.Equal(t, "B", stubs[0].Subject)
	require.Equal(t, "C", stubs[1].Subject)
}

func TestUpdateAddsAndRemoves(t *testing.T) {
	ts := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	client := &fakeIndexClient{
		ids: map[string][]string{"INBOX": {"msg1"}},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli()},
		},
	}
	idx := newTestIndex(t, client)
	ctx := context.Background()

	require.NoError(t, idx.EnsureIndexed(ctx, "INBOX"))

	ts2 := time.Date(2026, 2, 15, 14, 0, 0, 0, time.Local)
	stubs := map[string]MessageStub{
		"msg2": {ID: "msg2", InternalDate: ts2.UnixMilli()},
	}
	lc := &LabelChanges{
		Added:   map[string]bool{"msg2": true},
		Removed: map[string]bool{},
	}
	require.NoError(t, idx.Update("INBOX", lc, stubs))

	ids, err := idx.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"msg1", "msg2"}, ids)

	// Now remove msg1.
	stubs["msg1"] = MessageStub{ID: "msg1", InternalDate: ts.UnixMilli()}
	lc2 := &LabelChanges{
		Added:   map[string]bool{},
		Removed: map[string]bool{"msg1": true},
	}
	require.NoError(t, idx.Update("INBOX", lc2, stubs))

	ids, err = idx.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg2"}, ids)
}

func TestUpdateSkipsWhenNotIndexed(t *testing.T) {
	idx := newTestIndex(t, nil)

	lc := &LabelChanges{
		Added:   map[string]bool{"msg1": true},
		Removed: map[string]bool{},
	}
	// Should be a no-op, not error.
	require.NoError(t, idx.Update("INBOX", lc, nil))
}

func TestFlushClearsIndex(t *testing.T) {
	ts := time.Date(2026, 2, 15, 10, 0, 0, 0, time.Local)
	client := &fakeIndexClient{
		ids: map[string][]string{"INBOX": {"msg1"}},
		stubs: map[string]MessageStub{
			"msg1": {ID: "msg1", InternalDate: ts.UnixMilli()},
		},
	}
	idx := newTestIndex(t, client)
	ctx := context.Background()

	require.NoError(t, idx.EnsureIndexed(ctx, "INBOX"))
	require.True(t, idx.cache.IsLabelIndexComplete("INBOX"))

	require.NoError(t, idx.Flush("INBOX"))
	require.False(t, idx.cache.IsLabelIndexComplete("INBOX"))

	_, err := idx.cache.GetDayIndex("INBOX", 2026, 2, 15)
	require.Error(t, err)
}
