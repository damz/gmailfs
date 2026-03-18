package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"

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

// FlushListings deletes all cached indexes across all labels.
func (c *Cache) FlushListings() error {
	return c.deleteByPrefixes("idx:", "idx-done:")
}

// FlushLabel deletes all cached index data for a specific label.
func (c *Cache) FlushLabel(labelID string) error {
	if err := c.deleteByPrefixes(fmt.Sprintf("idx:%s:", labelID)); err != nil {
		return err
	}
	return c.deleteExact(fmt.Sprintf("idx-done:%s", labelID))
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

// --- Day index: idx:<labelID>:<YYYY>:<MM>:<DD> → JSON []string (message IDs) ---

func dayIndexKey(labelID string, year, month, day int) []byte {
	return fmt.Appendf(nil, "idx:%s:%04d:%02d:%02d", labelID, year, month, day)
}

func (c *Cache) GetDayIndex(labelID string, year, month, day int) ([]string, error) {
	var ids []string
	err := c.getJSON(dayIndexKey(labelID, year, month, day), &ids)
	return ids, err
}

func (c *Cache) SetDayIndex(labelID string, year, month, day int, ids []string) error {
	key := dayIndexKey(labelID, year, month, day)
	if len(ids) == 0 {
		err := c.db.Delete(key, pebble.Sync)
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	return c.setJSON(key, ids)
}

func (c *Cache) AddToDayIndex(labelID string, year, month, day int, msgID string) error {
	ids, err := c.GetDayIndex(labelID, year, month, day)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if slices.Contains(ids, msgID) {
		return nil
	}
	ids = append(ids, msgID)
	return c.setJSON(dayIndexKey(labelID, year, month, day), ids)
}

func (c *Cache) RemoveFromDayIndex(labelID string, year, month, day int, msgID string) error {
	ids, err := c.GetDayIndex(labelID, year, month, day)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != msgID {
			filtered = append(filtered, id)
		}
	}
	return c.SetDayIndex(labelID, year, month, day, filtered)
}

// --- Index completeness marker ---

func indexCompleteKey(labelID string) []byte {
	return fmt.Appendf(nil, "idx-done:%s", labelID)
}

func (c *Cache) IsLabelIndexComplete(labelID string) bool {
	_, closer, err := c.db.Get(indexCompleteKey(labelID))
	if err != nil {
		return false
	}
	_ = closer.Close()
	return true
}

func (c *Cache) SetLabelIndexComplete(labelID string) error {
	return c.db.Set(indexCompleteKey(labelID), []byte("1"), pebble.Sync)
}

// --- Prefix-scan derived listings from the day index ---

// IndexedYears returns populated years by scanning day index keys.
func (c *Cache) IndexedYears(labelID string) ([]int, error) {
	prefix := fmt.Appendf(nil, "idx:%s:", labelID)
	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var years []int
	for valid := iter.First(); valid; {
		suffix := iter.Key()[len(prefix):]
		year, perr := strconv.Atoi(string(suffix[:4]))
		if perr != nil {
			valid = iter.Next()
			continue
		}
		years = append(years, year)
		valid = iter.SeekGE(fmt.Appendf(nil, "idx:%s:%04d;", labelID, year))
	}
	return years, nil
}

// IndexedMonths returns populated months for a year by scanning day index keys.
func (c *Cache) IndexedMonths(labelID string, year int) ([]int, error) {
	prefix := fmt.Appendf(nil, "idx:%s:%04d:", labelID, year)
	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var months []int
	for valid := iter.First(); valid; {
		suffix := iter.Key()[len(prefix):]
		month, perr := strconv.Atoi(string(suffix[:2]))
		if perr != nil {
			valid = iter.Next()
			continue
		}
		months = append(months, month)
		valid = iter.SeekGE(fmt.Appendf(nil, "idx:%s:%04d:%02d;", labelID, year, month))
	}
	return months, nil
}

// IndexedDays returns populated days for a year/month by scanning day index keys.
func (c *Cache) IndexedDays(labelID string, year, month int) ([]int, error) {
	prefix := fmt.Appendf(nil, "idx:%s:%04d:%02d:", labelID, year, month)
	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var days []int
	for valid := iter.First(); valid; valid = iter.Next() {
		suffix := iter.Key()[len(prefix):]
		day, perr := strconv.Atoi(string(suffix[:2]))
		if perr != nil {
			continue
		}
		days = append(days, day)
	}
	return days, nil
}

// DayStubsFromIndex returns MessageStubs for a day by looking up the index
// then resolving each stub individually.
func (c *Cache) DayStubsFromIndex(labelID string, year, month, day int) ([]MessageStub, error) {
	ids, err := c.GetDayIndex(labelID, year, month, day)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	stubs := make([]MessageStub, 0, len(ids))
	for _, id := range ids {
		stub, serr := c.GetMessageStub(id)
		if serr != nil {
			return nil, fmt.Errorf("resolving stub %s: %w", id, serr)
		}
		stubs = append(stubs, stub)
	}
	sort.Slice(stubs, func(i, j int) bool { return stubs[i].InternalDate < stubs[j].InternalDate })
	return stubs, nil
}

func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	upper[len(upper)-1]++
	return upper
}
