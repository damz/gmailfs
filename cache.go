package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type Cache struct {
	db *pebble.DB
}

func NewCache(dir string) (*Cache, error) {
	db, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatNewest,
	})
	if err != nil {
		return nil, fmt.Errorf("opening cache: %w", err)
	}

	return &Cache{db: db}, nil
}

func (c *Cache) Close() error {
	return c.db.Close()
}

// getJSON retrieves a key and JSON-decodes it into dest.
// Returns pebble.ErrNotFound if the key does not exist.
func (c *Cache) getJSON(key []byte, dest any) error {
	val, closer, err := c.db.Get(key)
	if err != nil {
		return err
	}

	defer func() { _ = closer.Close() }()
	return json.Unmarshal(val, dest)
}

func (c *Cache) setJSON(key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return c.db.Set(key, data, pebble.Sync)
}

// GetLabels returns cached label info, or nil if not cached.
func (c *Cache) GetLabels() ([]LabelInfo, error) {
	var labels []LabelInfo
	if err := c.getJSON([]byte("labels"), &labels); err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	return labels, nil
}

func (c *Cache) SetLabels(labels []LabelInfo) error {
	return c.setJSON([]byte("labels"), labels)
}

func populatedDaysKey(labelID string, year, month int) []byte {
	return fmt.Appendf(nil, "days:%s:%04d:%02d", labelID, year, month)
}

func (c *Cache) GetPopulatedDays(labelID string, year, month int) ([]int, error) {
	var days []int
	err := c.getJSON(populatedDaysKey(labelID, year, month), &days)
	return days, err
}

func (c *Cache) SetPopulatedDays(labelID string, year, month int, days []int) error {
	if days == nil {
		days = []int{}
	}

	return c.setJSON(populatedDaysKey(labelID, year, month), days)
}

func populatedYearsKey(labelID string) []byte {
	return fmt.Appendf(nil, "years:%s", labelID)
}

func (c *Cache) GetPopulatedYears(labelID string) ([]int, error) {
	var years []int
	err := c.getJSON(populatedYearsKey(labelID), &years)
	return years, err
}

func (c *Cache) SetPopulatedYears(labelID string, years []int) error {
	if years == nil {
		years = []int{}
	}

	return c.setJSON(populatedYearsKey(labelID), years)
}

func populatedMonthsKey(labelID string, year int) []byte {
	return fmt.Appendf(nil, "months:%s:%04d", labelID, year)
}

func (c *Cache) GetPopulatedMonths(labelID string, year int) ([]int, error) {
	var months []int
	err := c.getJSON(populatedMonthsKey(labelID, year), &months)
	return months, err
}

func (c *Cache) SetPopulatedMonths(labelID string, year int, months []int) error {
	if months == nil {
		months = []int{}
	}

	return c.setJSON(populatedMonthsKey(labelID, year), months)
}

func dayKey(labelID string, year, month, day int) []byte {
	return fmt.Appendf(nil, "day:%s:%04d:%02d:%02d", labelID, year, month, day)
}

func (c *Cache) GetDayListing(labelID string, year, month, day int) ([]MessageStub, error) {
	var stubs []MessageStub
	err := c.getJSON(dayKey(labelID, year, month, day), &stubs)
	return stubs, err
}

func (c *Cache) SetDayListing(labelID string, year, month, day int, stubs []MessageStub) error {
	if stubs == nil {
		stubs = []MessageStub{}
	}

	return c.setJSON(dayKey(labelID, year, month, day), stubs)
}

func stubKey(messageID string) []byte {
	return []byte("stub:" + messageID)
}

func (c *Cache) GetMessageStub(messageID string) (MessageStub, error) {
	var stub MessageStub
	err := c.getJSON(stubKey(messageID), &stub)
	return stub, err
}

func (c *Cache) SetMessageStub(stub MessageStub) error {
	return c.setJSON(stubKey(stub.ID), stub)
}

func rawMsgKey(messageID string) []byte {
	return []byte("msg:" + messageID)
}

// GetRawMessage returns cached raw .eml bytes, or nil if not cached.
func (c *Cache) GetRawMessage(messageID string) ([]byte, error) {
	val, closer, err := c.db.Get(rawMsgKey(messageID))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	defer func() { _ = closer.Close() }()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (c *Cache) SetRawMessage(messageID string, data []byte) error {
	return c.db.Set(rawMsgKey(messageID), data, pebble.Sync)
}

// GetHistoryID returns the cached history ID, or 0 if not set.
func (c *Cache) GetHistoryID() (uint64, error) {
	val, closer, err := c.db.Get([]byte("meta:historyId"))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	defer func() { _ = closer.Close() }()
	return strconv.ParseUint(string(val), 10, 64)
}

func (c *Cache) SetHistoryID(id uint64) error {
	return c.db.Set([]byte("meta:historyId"), []byte(strconv.FormatUint(id, 10)), pebble.Sync)
}

// CheckTimezone flushes all day-level caches if the local timezone changed
// since the last run. Returns true if the cache was flushed.
func (c *Cache) CheckTimezone(tz string) (bool, error) {
	val, closer, err := c.db.Get([]byte("meta:timezone"))
	if err != nil && err != pebble.ErrNotFound {
		return false, err
	}

	if err == nil {
		old := string(val)
		_ = closer.Close()
		if old == tz {
			return false, nil
		}
	}

	if err := c.FlushListings(); err != nil {
		return false, err
	}

	return true, c.db.Set([]byte("meta:timezone"), []byte(tz), pebble.Sync)
}

// FlushListings deletes all cached listings (days, months, years) across all labels.
func (c *Cache) FlushListings() error {
	return c.deleteByPrefixes("day:", "days:", "months:", "years:")
}

// FlushDays deletes cache entries for specific days. Parent listings are only
// flushed when an affected date isn't already in the cached listing, since a
// new day/month/year may have appeared.
func (c *Cache) FlushDays(labelID string, dates []time.Time) error {
	type dayKey struct{ y, m, d int }
	type monthKey struct{ y, m int }

	allDays := make(map[dayKey]struct{})
	for _, t := range dates {
		y, m, d := t.Date()
		allDays[dayKey{y, int(m), d}] = struct{}{}
	}

	// Determine which months have a potentially new day.
	newDayMonths := make(map[monthKey]struct{})
	for dk := range allDays {
		mk := monthKey{dk.y, dk.m}
		if _, already := newDayMonths[mk]; already {
			continue
		}

		cachedDays, err := c.GetPopulatedDays(labelID, dk.y, dk.m)
		if err == pebble.ErrNotFound {
			newDayMonths[mk] = struct{}{}
			continue
		}
		if err != nil {
			return err
		}

		if !slices.Contains(cachedDays, dk.d) {
			newDayMonths[mk] = struct{}{}
		}
	}

	// Determine which years have a potentially new month.
	newMonthYears := make(map[int]struct{})
	for mk := range newDayMonths {
		if _, already := newMonthYears[mk.y]; already {
			continue
		}

		cachedMonths, err := c.GetPopulatedMonths(labelID, mk.y)
		if err == pebble.ErrNotFound {
			newMonthYears[mk.y] = struct{}{}
			continue
		}
		if err != nil {
			return err
		}

		if !slices.Contains(cachedMonths, mk.m) {
			newMonthYears[mk.y] = struct{}{}
		}
	}

	// Determine whether the years list itself needs flushing.
	flushYears := false
	if len(newMonthYears) > 0 {
		cachedYears, err := c.GetPopulatedYears(labelID)
		if err == pebble.ErrNotFound {
			flushYears = true
		} else if err != nil {
			return err
		} else {
			yearSet := make(map[int]bool, len(cachedYears))
			for _, y := range cachedYears {
				yearSet[y] = true
			}
			for y := range newMonthYears {
				if !yearSet[y] {
					flushYears = true
					break
				}
			}
		}
	}

	batch := c.db.NewBatch()

	for dk := range allDays {
		key := fmt.Appendf(nil, "day:%s:%04d:%02d:%02d", labelID, dk.y, dk.m, dk.d)
		slog.Debug("cache flush", slog.String("key", string(key)))
		if err := batch.Delete(key, pebble.Sync); err != nil {
			return err
		}
	}

	for mk := range newDayMonths {
		key := fmt.Appendf(nil, "days:%s:%04d:%02d", labelID, mk.y, mk.m)
		slog.Debug("cache flush", slog.String("key", string(key)))
		if err := batch.Delete(key, pebble.Sync); err != nil {
			return err
		}
	}

	for y := range newMonthYears {
		key := fmt.Appendf(nil, "months:%s:%04d", labelID, y)
		slog.Debug("cache flush", slog.String("key", string(key)))
		if err := batch.Delete(key, pebble.Sync); err != nil {
			return err
		}
	}

	if flushYears {
		ykey := fmt.Appendf(nil, "years:%s", labelID)
		slog.Debug("cache flush", slog.String("key", string(ykey)))
		if err := batch.Delete(ykey, pebble.Sync); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// FlushLabel deletes all cached listings for a specific label.
func (c *Cache) FlushLabel(labelID string) error {
	if err := c.deleteByPrefixes(
		fmt.Sprintf("day:%s:", labelID),
		fmt.Sprintf("days:%s:", labelID),
		fmt.Sprintf("months:%s:", labelID),
	); err != nil {
		return err
	}

	// years key has no trailing colon after labelID.
	return c.deleteExact(fmt.Sprintf("years:%s", labelID))
}

func (c *Cache) deleteExact(key string) error {
	err := c.db.Delete([]byte(key), pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	slog.Debug("cache flush", slog.String("key", key))
	return nil
}

func (c *Cache) deleteByPrefixes(prefixes ...string) error {
	batch := c.db.NewBatch()
	for _, prefix := range prefixes {
		lower := []byte(prefix)
		upper := []byte(prefix[:len(prefix)-1] + ";") // ';' is one past ':'
		if err := batch.DeleteRange(lower, upper, pebble.Sync); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}
