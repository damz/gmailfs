package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type fsContext struct {
	gmail  *GmailClient
	cache  *Cache
	labels []LabelInfo
	root   *fs.Inode
}

func stableInode(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	ino := h.Sum64()
	if ino == 0 || ino == 1 {
		ino = 2 // Reserve 0 and 1
	}
	return ino
}

var dirAttr = fuse.Attr{
	Mode: syscall.S_IFDIR | 0o555,
}

type rootNode struct {
	fs.Inode
	fsCtx *fsContext
}

var _ = (fs.NodeReaddirer)((*rootNode)(nil))
var _ = (fs.NodeLookuper)((*rootNode)(nil))
var _ = (fs.NodeOpendirHandler)((*rootNode)(nil))
var _ = (fs.NodeAccesser)((*rootNode)(nil))

func (n *rootNode) Access(_ context.Context, _ uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (n *rootNode) OpendirHandle(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.ENOSYS
}

func (n *rootNode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	var entries []fuse.DirEntry
	for _, l := range n.fsCtx.labels {
		name := sanitizeLabelName(l.Name)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  stableInode("label", l.ID),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	for _, l := range n.fsCtx.labels {
		if sanitizeLabelName(l.Name) == name {
			child := &labelNode{fsCtx: n.fsCtx, label: l}
			ino := stableInode("label", l.ID)
			out.Attr = dirAttr
			out.Ino = ino
			return n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: ino}), 0
		}
	}
	return nil, syscall.ENOENT
}

type labelNode struct {
	fs.Inode
	fsCtx *fsContext
	label LabelInfo
}

var _ = (fs.NodeReaddirer)((*labelNode)(nil))
var _ = (fs.NodeLookuper)((*labelNode)(nil))
var _ = (fs.NodeOpendirHandler)((*labelNode)(nil))

func (n *labelNode) OpendirHandle(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.ENOSYS
}

func (n *labelNode) getPopulatedYears(ctx context.Context) ([]int, error) {
	years, err := n.fsCtx.cache.GetPopulatedYears(n.label.ID)
	if err == nil {
		return years, nil
	}

	years, err = n.fsCtx.gmail.PopulatedYears(ctx, n.label.ID, n.fsCtx.cache)
	if err != nil {
		return nil, err
	}

	if err := n.fsCtx.cache.SetPopulatedYears(n.label.ID, years); err != nil {
		slog.Warn("cache write error", slog.Any("err", err))
	}
	return years, nil
}

func (n *labelNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	years, err := n.getPopulatedYears(ctx)
	if err != nil {
		slog.Error("labelNode readdir error", slog.Any("err", err))
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, y := range years {
		name := strconv.Itoa(y)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  stableInode("label", n.label.ID, name),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *labelNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	year, err := strconv.Atoi(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	years, yerr := n.getPopulatedYears(ctx)
	if yerr != nil {
		slog.Error("labelNode lookup error", slog.Any("err", yerr))
		return nil, syscall.EIO
	}
	if !slices.Contains(years, year) {
		return nil, syscall.ENOENT
	}

	child := &yearNode{fsCtx: n.fsCtx, label: n.label, year: year}
	ino := stableInode("label", n.label.ID, name)
	out.Attr = dirAttr
	out.Ino = ino
	return n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: ino}), 0
}

type yearNode struct {
	fs.Inode
	fsCtx *fsContext
	label LabelInfo
	year  int
}

var _ = (fs.NodeReaddirer)((*yearNode)(nil))
var _ = (fs.NodeLookuper)((*yearNode)(nil))
var _ = (fs.NodeOpendirHandler)((*yearNode)(nil))

func (n *yearNode) OpendirHandle(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.ENOSYS
}

func (n *yearNode) getPopulatedMonths(ctx context.Context) ([]int, error) {
	months, err := n.fsCtx.cache.GetPopulatedMonths(n.label.ID, n.year)
	if err == nil {
		return months, nil
	}

	months, err = n.fsCtx.gmail.PopulatedMonths(ctx, n.label.ID, n.year, n.fsCtx.cache)
	if err != nil {
		return nil, err
	}

	if err := n.fsCtx.cache.SetPopulatedMonths(n.label.ID, n.year, months); err != nil {
		slog.Warn("cache write error", slog.Any("err", err))
	}
	return months, nil
}

func (n *yearNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	months, err := n.getPopulatedMonths(ctx)
	if err != nil {
		slog.Error("yearNode readdir error", slog.Any("err", err))
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, m := range months {
		name := fmt.Sprintf("%02d", m)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  stableInode("label", n.label.ID, strconv.Itoa(n.year), name),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *yearNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	month, err := strconv.Atoi(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	months, merr := n.getPopulatedMonths(ctx)
	if merr != nil {
		slog.Error("yearNode lookup error", slog.Any("err", merr))
		return nil, syscall.EIO
	}
	if !slices.Contains(months, month) {
		return nil, syscall.ENOENT
	}

	child := &monthNode{fsCtx: n.fsCtx, label: n.label, year: n.year, month: month}
	ino := stableInode("label", n.label.ID, strconv.Itoa(n.year), name)
	out.Attr = dirAttr
	out.Ino = ino
	return n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: ino}), 0
}

type monthNode struct {
	fs.Inode
	fsCtx *fsContext
	label LabelInfo
	year  int
	month int
}

var _ = (fs.NodeReaddirer)((*monthNode)(nil))
var _ = (fs.NodeLookuper)((*monthNode)(nil))
var _ = (fs.NodeOpendirHandler)((*monthNode)(nil))

func (n *monthNode) OpendirHandle(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.ENOSYS
}

func (n *monthNode) getPopulatedDays(ctx context.Context) ([]int, error) {
	days, err := n.fsCtx.cache.GetPopulatedDays(n.label.ID, n.year, n.month)
	if err == nil {
		return days, nil
	}

	days, err = n.fsCtx.gmail.PopulatedDays(ctx, n.label.ID, n.year, n.month, n.fsCtx.cache)
	if err != nil {
		return nil, err
	}

	if err := n.fsCtx.cache.SetPopulatedDays(n.label.ID, n.year, n.month, days); err != nil {
		slog.Warn("cache write error", slog.Any("err", err))
	}
	return days, nil
}

func (n *monthNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	days, err := n.getPopulatedDays(ctx)
	if err != nil {
		slog.Error("monthNode readdir error", slog.Any("err", err))
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, d := range days {
		name := fmt.Sprintf("%02d", d)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  stableInode("label", n.label.ID, strconv.Itoa(n.year), fmt.Sprintf("%02d", n.month), name),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *monthNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	day, err := strconv.Atoi(name)
	if err != nil || day < 1 || day > 31 {
		return nil, syscall.ENOENT
	}

	days, derr := n.getPopulatedDays(ctx)
	if derr != nil {
		slog.Error("monthNode lookup error", slog.Any("err", derr))
		return nil, syscall.EIO
	}
	if !slices.Contains(days, day) {
		return nil, syscall.ENOENT
	}

	child := &dayNode{fsCtx: n.fsCtx, label: n.label, year: n.year, month: n.month, day: day}
	ino := stableInode("label", n.label.ID, strconv.Itoa(n.year), fmt.Sprintf("%02d", n.month), name)
	out.Attr = dirAttr
	out.Ino = ino
	return n.NewPersistentInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: ino}), 0
}

type dayNode struct {
	fs.Inode
	fsCtx *fsContext
	label LabelInfo
	year  int
	month int
	day   int
}

var _ = (fs.NodeReaddirer)((*dayNode)(nil))
var _ = (fs.NodeLookuper)((*dayNode)(nil))
var _ = (fs.NodeOpendirHandler)((*dayNode)(nil))

func (n *dayNode) OpendirHandle(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, syscall.ENOSYS
}

func (n *dayNode) getDayStubs(ctx context.Context) ([]MessageStub, error) {
	stubs, err := n.fsCtx.cache.GetDayListing(n.label.ID, n.year, n.month, n.day)
	if err == nil {
		return stubs, nil
	}

	stubs, err = n.fsCtx.gmail.ListDayMessages(ctx, n.label.ID, n.year, n.month, n.day, n.fsCtx.cache)
	if err != nil {
		return nil, err
	}

	if err := n.fsCtx.cache.SetDayListing(n.label.ID, n.year, n.month, n.day, stubs); err != nil {
		slog.Warn("cache write error", slog.Any("err", err))
	}
	return stubs, nil
}

func (n *dayNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	stubs, err := n.getDayStubs(ctx)
	if err != nil {
		slog.Error("dayNode readdir error", slog.Any("err", err))
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, s := range stubs {
		t := time.UnixMilli(s.InternalDate).Local()
		name := emlFilename(t, s.Subject, s.ID)
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  stableInode("msg", s.ID),
			Mode: syscall.S_IFREG,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (n *dayNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	stubs, err := n.getDayStubs(ctx)
	if err != nil {
		slog.Error("dayNode lookup error", slog.Any("err", err))
		return nil, syscall.EIO
	}

	for _, s := range stubs {
		t := time.UnixMilli(s.InternalDate).Local()
		if emlFilename(t, s.Subject, s.ID) == name {
			child := &emlNode{fsCtx: n.fsCtx, stub: s}
			ino := stableInode("msg", s.ID)
			out.Attr = fuse.Attr{
				Mode: syscall.S_IFREG | 0o444,
				Size: n.fileSize(s),
				Ino:  ino,
			}
			setEmailTimes(&out.Attr, t)
			return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: ino}), 0
		}
	}
	return nil, syscall.ENOENT
}

func (n *dayNode) fileSize(s MessageStub) uint64 {
	raw, err := n.fsCtx.cache.GetRawMessage(s.ID)
	if err == nil && raw != nil {
		return uint64(len(raw))
	}
	return uint64(s.SizeEstimate)
}

type emlNode struct {
	fs.Inode
	fsCtx *fsContext
	stub  MessageStub
}

var _ = (fs.NodeGetattrer)((*emlNode)(nil))
var _ = (fs.NodeOpener)((*emlNode)(nil))
var _ = (fs.NodeReader)((*emlNode)(nil))

func (n *emlNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	t := time.UnixMilli(n.stub.InternalDate).Local()
	out.Mode = syscall.S_IFREG | 0o444
	out.Ino = stableInode("msg", n.stub.ID)

	raw, err := n.fsCtx.cache.GetRawMessage(n.stub.ID)
	if err == nil && raw != nil {
		out.Size = uint64(len(raw))
	} else {
		out.Size = uint64(n.stub.SizeEstimate)
	}

	setEmailTimes(&out.Attr, t)
	return 0
}

func (n *emlNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *emlNode) Read(ctx context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := n.getRawContent(ctx)
	if err != nil {
		slog.Error("emlNode read error", slog.String("msgID", n.stub.ID), slog.Any("err", err))
		return nil, syscall.EIO
	}

	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := min(off+int64(len(dest)), int64(len(data)))
	return fuse.ReadResultData(data[off:end]), 0
}

func (n *emlNode) getRawContent(ctx context.Context) ([]byte, error) {
	raw, err := n.fsCtx.cache.GetRawMessage(n.stub.ID)
	if err == nil && raw != nil {
		return raw, nil
	}

	raw, err = n.fsCtx.gmail.GetRawMessage(ctx, n.stub.ID)
	if err != nil {
		return nil, err
	}

	if cerr := n.fsCtx.cache.SetRawMessage(n.stub.ID, raw); cerr != nil {
		slog.Warn("cache write error", slog.String("msgID", n.stub.ID), slog.Any("err", cerr))
	}

	// If the actual size differs from the estimate the kernel cached,
	// invalidate so Getattr returns the correct size on next stat.
	if int64(len(raw)) != n.stub.SizeEstimate {
		_ = n.NotifyContent(0, 0)
	}

	return raw, nil
}

func setEmailTimes(attr *fuse.Attr, t time.Time) {
	sec := uint64(t.Unix())
	nsec := uint32(t.Nanosecond())
	attr.Atime = sec
	attr.Atimensec = nsec
	attr.Mtime = sec
	attr.Mtimensec = nsec
	attr.Ctime = sec
	attr.Ctimensec = nsec
}

// invalidateKernel issues NotifyEntry/NotifyContent calls to drop stale
// kernel-cached dentries and directory page cache entries.
func invalidateKernel(fsCtx *fsContext, result syncResult) {
	root := fsCtx.root
	if root == nil {
		return
	}

	if !result.fullFlush && !result.labelsChanged && len(result.labelDates) == 0 {
		return
	}

	if result.fullFlush {
		invalidateAllLabels(root)
		return
	}

	if result.labelsChanged {
		for name := range root.Children() {
			_ = root.NotifyEntry(name)
		}
		_ = root.NotifyContent(0, 0)
	}

	for labelID, dates := range result.labelDates {
		labelName := labelDisplayName(fsCtx, labelID)
		if labelName == "" {
			continue
		}

		labelInode := root.GetChild(labelName)
		if labelInode == nil {
			continue
		}

		if dates == nil {
			invalidateLabelSubtree(labelInode)
		} else {
			invalidateLabelDates(labelInode, dates)
		}
	}
}

func labelDisplayName(fsCtx *fsContext, labelID string) string {
	for _, l := range fsCtx.labels {
		if l.ID == labelID {
			return sanitizeLabelName(l.Name)
		}
	}
	return ""
}

func invalidateAllLabels(root *fs.Inode) {
	for name, labelInode := range root.Children() {
		invalidateLabelSubtree(labelInode)
		_ = root.NotifyEntry(name)
	}
	_ = root.NotifyContent(0, 0)
}

func invalidateLabelSubtree(labelInode *fs.Inode) {
	for yearName, yearInode := range labelInode.Children() {
		for monthName, monthInode := range yearInode.Children() {
			invalidateMonthInode(monthInode)
			_ = yearInode.NotifyEntry(monthName)
		}
		_ = labelInode.NotifyEntry(yearName)
	}
	_ = labelInode.NotifyContent(0, 0)
}

func invalidateLabelDates(labelInode *fs.Inode, dates []time.Time) {
	type yearMonth struct{ year, month int }
	grouped := make(map[yearMonth]map[int]bool)
	affectedYears := make(map[int]bool)
	for _, t := range dates {
		t = t.Local()
		key := yearMonth{t.Year(), int(t.Month())}
		if grouped[key] == nil {
			grouped[key] = make(map[int]bool)
		}
		grouped[key][t.Day()] = true
		affectedYears[t.Year()] = true
	}

	for ym, days := range grouped {
		yearStr := strconv.Itoa(ym.year)
		monthStr := fmt.Sprintf("%02d", ym.month)

		yearInode := labelInode.GetChild(yearStr)
		if yearInode == nil {
			continue
		}
		_ = yearInode.NotifyContent(0, 0)

		monthInode := yearInode.GetChild(monthStr)
		if monthInode == nil {
			continue
		}
		_ = monthInode.NotifyContent(0, 0)

		for day := range days {
			dayStr := fmt.Sprintf("%02d", day)
			_ = monthInode.NotifyEntry(dayStr)

			dayInode := monthInode.GetChild(dayStr)
			if dayInode == nil {
				continue
			}
			invalidateDayInode(dayInode)
		}
	}

	_ = labelInode.NotifyContent(0, 0)
}

func invalidateMonthInode(monthInode *fs.Inode) {
	for dayName, dayInode := range monthInode.Children() {
		invalidateDayInode(dayInode)
		_ = monthInode.NotifyEntry(dayName)
	}
	_ = monthInode.NotifyContent(0, 0)
}

func invalidateDayInode(dayInode *fs.Inode) {
	for emlName := range dayInode.Children() {
		_ = dayInode.NotifyEntry(emlName)
	}
	_ = dayInode.NotifyContent(0, 0)
}
