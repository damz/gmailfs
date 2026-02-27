package main

import (
	"testing"
	"time"

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

func TestPopulatedYears(t *testing.T) {
	c := newTestCache(t)

	_, err := c.GetPopulatedYears("INBOX")
	require.Error(t, err)

	require.NoError(t, c.SetPopulatedYears("INBOX", []int{2024, 2025, 2026}))

	years, err := c.GetPopulatedYears("INBOX")
	require.NoError(t, err)
	require.Equal(t, []int{2024, 2025, 2026}, years)
}

func TestPopulatedMonths(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetPopulatedMonths("INBOX", 2026, []int{1, 2}))

	months, err := c.GetPopulatedMonths("INBOX", 2026)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, months)
}

func TestPopulatedDays(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetPopulatedDays("INBOX", 2026, 2, []int{15, 27}))

	days, err := c.GetPopulatedDays("INBOX", 2026, 2)
	require.NoError(t, err)
	require.Equal(t, []int{15, 27}, days)
}

func TestDayListing(t *testing.T) {
	c := newTestCache(t)

	stubs := []MessageStub{
		{ID: "msg1", InternalDate: 1000, Subject: "Hello", SizeEstimate: 512},
		{ID: "msg2", InternalDate: 2000, Subject: "World", SizeEstimate: 1024},
	}
	require.NoError(t, c.SetDayListing("INBOX", 2026, 2, 27, stubs))

	got, err := c.GetDayListing("INBOX", 2026, 2, 27)
	require.NoError(t, err)
	require.Equal(t, stubs, got)
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

	require.NoError(t, c.SetPopulatedYears("INBOX", []int{2026}))
	require.NoError(t, c.SetPopulatedMonths("INBOX", 2026, []int{2}))
	require.NoError(t, c.SetPopulatedDays("INBOX", 2026, 2, []int{27}))
	require.NoError(t, c.SetDayListing("INBOX", 2026, 2, 27, []MessageStub{{ID: "m1"}}))

	require.NoError(t, c.FlushListings())

	_, err := c.GetPopulatedYears("INBOX")
	require.Error(t, err)

	_, err = c.GetPopulatedMonths("INBOX", 2026)
	require.Error(t, err)

	_, err = c.GetPopulatedDays("INBOX", 2026, 2)
	require.Error(t, err)

	_, err = c.GetDayListing("INBOX", 2026, 2, 27)
	require.Error(t, err)
}

func TestFlushDays(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetPopulatedYears("INBOX", []int{2026}))
	require.NoError(t, c.SetPopulatedMonths("INBOX", 2026, []int{2}))
	require.NoError(t, c.SetPopulatedDays("INBOX", 2026, 2, []int{15, 27}))
	require.NoError(t, c.SetDayListing("INBOX", 2026, 2, 15, []MessageStub{{ID: "m1"}}))
	require.NoError(t, c.SetDayListing("INBOX", 2026, 2, 27, []MessageStub{{ID: "m2"}}))

	// Flush day 27 — already in cached days list, so parents stay.
	dates := []time.Time{time.Date(2026, 2, 27, 12, 0, 0, 0, time.Local)}
	require.NoError(t, c.FlushDays("INBOX", dates))

	_, err := c.GetDayListing("INBOX", 2026, 2, 27)
	require.Error(t, err)

	// Day 15 listing untouched.
	stubs, err := c.GetDayListing("INBOX", 2026, 2, 15)
	require.NoError(t, err)
	require.Len(t, stubs, 1)

	// Parents not flushed since day 27 was already known.
	days, err := c.GetPopulatedDays("INBOX", 2026, 2)
	require.NoError(t, err)
	require.Equal(t, []int{15, 27}, days)
}

func TestFlushLabel(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetPopulatedYears("INBOX", []int{2026}))
	require.NoError(t, c.SetPopulatedMonths("INBOX", 2026, []int{2}))
	require.NoError(t, c.SetPopulatedDays("INBOX", 2026, 2, []int{15, 27}))
	require.NoError(t, c.SetDayListing("INBOX", 2026, 2, 15, []MessageStub{{ID: "m1"}}))

	// Another label should be untouched.
	require.NoError(t, c.SetPopulatedYears("SENT", []int{2025}))

	require.NoError(t, c.FlushLabel("INBOX"))

	_, err := c.GetPopulatedYears("INBOX")
	require.Error(t, err)

	_, err = c.GetPopulatedMonths("INBOX", 2026)
	require.Error(t, err)

	_, err = c.GetPopulatedDays("INBOX", 2026, 2)
	require.Error(t, err)

	_, err = c.GetDayListing("INBOX", 2026, 2, 15)
	require.Error(t, err)

	// SENT untouched.
	years, err := c.GetPopulatedYears("SENT")
	require.NoError(t, err)
	require.Equal(t, []int{2025}, years)
}

func TestFlushDaysNewDay(t *testing.T) {
	c := newTestCache(t)

	require.NoError(t, c.SetPopulatedYears("INBOX", []int{2026}))
	require.NoError(t, c.SetPopulatedMonths("INBOX", 2026, []int{2}))
	require.NoError(t, c.SetPopulatedDays("INBOX", 2026, 2, []int{15}))

	// Flush day 20 — not in cached days, so parents get flushed too.
	dates := []time.Time{time.Date(2026, 2, 20, 12, 0, 0, 0, time.Local)}
	require.NoError(t, c.FlushDays("INBOX", dates))

	_, err := c.GetPopulatedDays("INBOX", 2026, 2)
	require.Error(t, err)

	// Months and years not flushed since month 2 and year 2026 were already known.
	months, err := c.GetPopulatedMonths("INBOX", 2026)
	require.NoError(t, err)
	require.Equal(t, []int{2}, months)

	years, err := c.GetPopulatedYears("INBOX")
	require.NoError(t, err)
	require.Equal(t, []int{2026}, years)
}
