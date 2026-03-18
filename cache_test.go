package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T) *Cache {
	t.Helper()
	c, err := NewCache(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestLabels(t *testing.T) {
	c := newTestCache(t)

	got, err := c.GetLabels()
	require.NoError(t, err)
	require.Nil(t, got)

	labels := []LabelInfo{{ID: "INBOX", Name: "Inbox"}, {ID: "SENT", Name: "Sent"}}
	require.NoError(t, c.SetLabels(labels))

	got, err = c.GetLabels()
	require.NoError(t, err)
	require.Equal(t, labels, got)
}

func TestMessageStub(t *testing.T) {
	c := newTestCache(t)

	stub := MessageStub{ID: "msg1", InternalDate: 1000, Subject: "Test", SizeEstimate: 256}
	require.NoError(t, c.SetMessageStub(stub))

	got, err := c.GetMessageStub("msg1")
	require.NoError(t, err)
	require.Equal(t, stub, got)
}

func TestRawMessage(t *testing.T) {
	c := newTestCache(t)

	got, err := c.GetRawMessage("msg1")
	require.NoError(t, err)
	require.Nil(t, got)

	data := []byte("From: test@example.com\r\nSubject: Hi\r\n\r\nBody")
	require.NoError(t, c.SetRawMessage("msg1", data))

	got, err = c.GetRawMessage("msg1")
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestHistoryID(t *testing.T) {
	c := newTestCache(t)

	id, err := c.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(0), id)

	require.NoError(t, c.SetHistoryID(12345))

	id, err = c.GetHistoryID()
	require.NoError(t, err)
	require.Equal(t, uint64(12345), id)
}

func TestCheckTimezone(t *testing.T) {
	c := newTestCache(t)

	flushed, err := c.CheckTimezone("America/New_York")
	require.NoError(t, err)
	require.True(t, flushed)

	flushed, err = c.CheckTimezone("America/New_York")
	require.NoError(t, err)
	require.False(t, flushed)

	flushed, err = c.CheckTimezone("Europe/Paris")
	require.NoError(t, err)
	require.True(t, flushed)
}

func TestFlushListings(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 15, []string{"msg1"}))
	require.NoError(t, c.SetLabelIndexComplete("INBOX"))
	require.NoError(t, c.SetDayIndex("SENT", 2026, 1, 1, []string{"msg2"}))
	require.NoError(t, c.SetLabelIndexComplete("SENT"))

	require.NoError(t, c.FlushListings())

	require.False(t, c.IsLabelIndexComplete("INBOX"))
	require.False(t, c.IsLabelIndexComplete("SENT"))
	_, err := c.GetDayIndex("INBOX", 2026, 2, 15)
	require.Error(t, err)
}

func TestFlushLabel(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 15, []string{"m1"}))
	require.NoError(t, c.SetLabelIndexComplete("INBOX"))

	// Another label should be untouched.
	require.NoError(t, c.SetDayIndex("SENT", 2025, 1, 1, []string{"m2"}))
	require.NoError(t, c.SetLabelIndexComplete("SENT"))

	require.NoError(t, c.FlushLabel("INBOX"))

	require.False(t, c.IsLabelIndexComplete("INBOX"))
	_, err := c.GetDayIndex("INBOX", 2026, 2, 15)
	require.Error(t, err)

	// SENT untouched.
	require.True(t, c.IsLabelIndexComplete("SENT"))
	ids, err := c.GetDayIndex("SENT", 2025, 1, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"m2"}, ids)
}

func TestDayIndex(t *testing.T) {
	c := newTestCache(t)

	// Not found initially.
	_, err := c.GetDayIndex("INBOX", 2026, 2, 15)
	require.Error(t, err)

	// Set and retrieve.
	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 15, []string{"msg1", "msg2"}))
	ids, err := c.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg1", "msg2"}, ids)

	// Add a new ID.
	require.NoError(t, c.AddToDayIndex("INBOX", 2026, 2, 15, "msg3"))
	ids, err = c.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg1", "msg2", "msg3"}, ids)

	// Add duplicate — no-op.
	require.NoError(t, c.AddToDayIndex("INBOX", 2026, 2, 15, "msg2"))
	ids, err = c.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg1", "msg2", "msg3"}, ids)

	// Remove.
	require.NoError(t, c.RemoveFromDayIndex("INBOX", 2026, 2, 15, "msg2"))
	ids, err = c.GetDayIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Equal(t, []string{"msg1", "msg3"}, ids)

	// Remove last entries — key deleted.
	require.NoError(t, c.RemoveFromDayIndex("INBOX", 2026, 2, 15, "msg1"))
	require.NoError(t, c.RemoveFromDayIndex("INBOX", 2026, 2, 15, "msg3"))
	_, err = c.GetDayIndex("INBOX", 2026, 2, 15)
	require.Error(t, err)
}

func TestIndexedYearsMonthsDays(t *testing.T) {
	c := newTestCache(t)

	// Populate index across multiple years/months/days.
	require.NoError(t, c.SetDayIndex("INBOX", 2024, 3, 10, []string{"a"}))
	require.NoError(t, c.SetDayIndex("INBOX", 2024, 11, 5, []string{"b"}))
	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 15, []string{"c", "d"}))
	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 27, []string{"e"}))
	require.NoError(t, c.SetDayIndex("INBOX", 2026, 7, 1, []string{"f"}))

	// Years.
	years, err := c.IndexedYears("INBOX")
	require.NoError(t, err)
	require.Equal(t, []int{2024, 2026}, years)

	// Months.
	months, err := c.IndexedMonths("INBOX", 2026)
	require.NoError(t, err)
	require.Equal(t, []int{2, 7}, months)

	months, err = c.IndexedMonths("INBOX", 2024)
	require.NoError(t, err)
	require.Equal(t, []int{3, 11}, months)

	// Days.
	days, err := c.IndexedDays("INBOX", 2026, 2)
	require.NoError(t, err)
	require.Equal(t, []int{15, 27}, days)

	// Empty results for non-existent data.
	years, err = c.IndexedYears("SENT")
	require.NoError(t, err)
	require.Nil(t, years)
}

func TestDayStubsFromIndex(t *testing.T) {
	c := newTestCache(t)

	// Populate stubs and index.
	s1 := MessageStub{ID: "msg1", InternalDate: 2000, Subject: "Second", SizeEstimate: 100}
	s2 := MessageStub{ID: "msg2", InternalDate: 1000, Subject: "First", SizeEstimate: 200}
	require.NoError(t, c.SetMessageStub(s1))
	require.NoError(t, c.SetMessageStub(s2))
	require.NoError(t, c.SetDayIndex("INBOX", 2026, 2, 15, []string{"msg1", "msg2"}))

	stubs, err := c.DayStubsFromIndex("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	// Should be sorted by InternalDate.
	require.Equal(t, []MessageStub{s2, s1}, stubs)

	// Empty day returns nil stubs.
	stubs, err = c.DayStubsFromIndex("INBOX", 2026, 2, 20)
	require.NoError(t, err)
	require.Nil(t, stubs)
}
