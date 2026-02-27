package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const AllMailLabelID = "_all_"

type stubCache interface {
	GetMessageStub(string) (MessageStub, error)
	SetMessageStub(MessageStub) error
}

type historySyncer interface {
	GetProfile(ctx context.Context) (*gmail.Profile, error)
	SyncHistory(ctx context.Context, startHistoryId uint64) (*HistoryChanges, error)
	GetMessageStubs(ctx context.Context, messageIDs []string, cache stubCache) (map[string]MessageStub, error)
}

type LabelInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type MessageStub struct {
	ID           string `json:"id"`
	InternalDate int64  `json:"internalDate"` // millis since epoch
	Subject      string `json:"subject"`
	SizeEstimate int64  `json:"sizeEstimate"`
}

type GmailClient struct {
	svc     *gmail.Service
	limiter *rate.Limiter
	user    string
}

func NewGmailClient(ctx context.Context, httpClient *http.Client) (*GmailClient, error) {
	svc, err := gmail.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("creating Gmail service: %w", err)
	}

	return &GmailClient{
		svc:     svc,
		limiter: rate.NewLimiter(40, 10),
		user:    "me",
	}, nil
}

func (g *GmailClient) messagesList(labelID string) *gmail.UsersMessagesListCall {
	req := g.svc.Users.Messages.List(g.user)
	if labelID != AllMailLabelID {
		req = req.LabelIds(labelID)
	}
	return req
}

var hiddenLabels = map[string]bool{
	"CHAT":                true,
	"UNREAD":              true,
	"CATEGORY_PERSONAL":   true,
	"CATEGORY_SOCIAL":     true,
	"CATEGORY_PROMOTIONS": true,
	"CATEGORY_UPDATES":    true,
	"CATEGORY_FORUMS":     true,
	"BLUE_STAR":           true,
	"ORANGE_STAR":         true,
	"GREEN_CIRCLE":        true,
	"RED_CIRCLE":          true,
	"YELLOW_STAR":         true,
}

func (g *GmailClient) ListLabels(ctx context.Context) ([]LabelInfo, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	slog.Debug("gmail API", slog.String("method", "labels.list"))
	resp, err := g.svc.Users.Labels.List(g.user).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("listing labels: %w", err)
	}

	var labels []LabelInfo
	for _, l := range resp.Labels {
		if hiddenLabels[l.Id] {
			continue
		}
		labels = append(labels, LabelInfo{ID: l.Id, Name: l.Name})
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
	labels = append([]LabelInfo{{ID: AllMailLabelID, Name: "All Mail"}}, labels...)
	return labels, nil
}

func dayBounds(year, month, day int) (int64, int64) {
	start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 1)
	return start.Unix(), end.Unix()
}

func yearBounds(year int) (int64, int64) {
	start := time.Date(year, 1, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(year+1, 1, 1, 0, 0, 0, 0, time.Local)
	return start.Unix(), end.Unix()
}

func monthBounds(year, month int) (int64, int64) {
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 1, 0)
	return start.Unix(), end.Unix()
}

func (g *GmailClient) newestMessageInRange(ctx context.Context, labelID string, afterEpoch, beforeEpoch int64, cache *Cache) (MessageStub, bool, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return MessageStub{}, false, err
	}

	var query string
	if afterEpoch > 0 {
		query = fmt.Sprintf("after:%d before:%d", afterEpoch, beforeEpoch)
	} else {
		query = fmt.Sprintf("before:%d", beforeEpoch)
	}
	slog.Debug("gmail API", slog.String("method", "messages.list"), slog.String("label", labelID), slog.String("q", query))
	req := g.messagesList(labelID).
		Q(query).
		MaxResults(1).
		Context(ctx)
	resp, err := req.Do()
	if err != nil {
		return MessageStub{}, false, fmt.Errorf("listing newest message: %w", err)
	}

	if len(resp.Messages) == 0 {
		return MessageStub{}, false, nil
	}

	stubs, err := g.GetMessageStubs(ctx, []string{resp.Messages[0].Id}, cache)
	if err != nil {
		return MessageStub{}, false, err
	}

	stub, ok := stubs[resp.Messages[0].Id]
	if !ok {
		return MessageStub{}, false, nil
	}
	return stub, true, nil
}

func (g *GmailClient) listMessageIDs(ctx context.Context, labelID string, afterEpoch, beforeEpoch int64) ([]string, error) {
	query := fmt.Sprintf("after:%d before:%d", afterEpoch, beforeEpoch)
	var allIDs []string
	pageToken := ""
	for {
		if err := g.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		slog.Debug("gmail API", slog.String("method", "messages.list"), slog.String("label", labelID), slog.String("q", query))
		req := g.messagesList(labelID).
			Q(query).
			MaxResults(500).
			Context(ctx)
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		resp, err := req.Do()
		if err != nil {
			return nil, fmt.Errorf("listing message IDs: %w", err)
		}

		for _, m := range resp.Messages {
			allIDs = append(allIDs, m.Id)
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return allIDs, nil
}

// Cost: O(populated_years + 1) API calls.
func (g *GmailClient) PopulatedYears(ctx context.Context, labelID string, cache *Cache) ([]int, error) {
	var years []int
	cursor := time.Now().AddDate(1, 0, 0).Unix()
	for {
		s, found, err := g.newestMessageInRange(ctx, labelID, 0, cursor, cache)
		if err != nil {
			return nil, err
		}

		if !found {
			break
		}

		y := time.UnixMilli(s.InternalDate).In(time.Local).Year()
		years = append(years, y)
		cursor = time.Date(y, 1, 1, 0, 0, 0, 0, time.Local).Unix()
	}

	slices.Reverse(years)
	return years, nil
}

// Cost: O(populated_months + 1) API calls.
func (g *GmailClient) PopulatedMonths(ctx context.Context, labelID string, year int, cache *Cache) ([]int, error) {
	_, endOfYear := yearBounds(year)
	cursor := endOfYear
	startOfYear, _ := yearBounds(year)

	var months []int
	for {
		s, found, err := g.newestMessageInRange(ctx, labelID, startOfYear, cursor, cache)
		if err != nil {
			return nil, err
		}

		if !found {
			break
		}

		m := int(time.UnixMilli(s.InternalDate).In(time.Local).Month())
		months = append(months, m)
		mStart, _ := monthBounds(year, m)
		cursor = mStart
	}

	slices.Reverse(months)
	return months, nil
}

// Cost: O(populated_days + 1) API calls.
func (g *GmailClient) PopulatedDays(ctx context.Context, labelID string, year, month int, cache *Cache) ([]int, error) {
	_, endOfMonth := monthBounds(year, month)
	cursor := endOfMonth
	startOfMonth, _ := monthBounds(year, month)

	var days []int
	for {
		s, found, err := g.newestMessageInRange(ctx, labelID, startOfMonth, cursor, cache)
		if err != nil {
			return nil, err
		}

		if !found {
			break
		}

		d := time.UnixMilli(s.InternalDate).In(time.Local).Day()
		days = append(days, d)
		dStart, _ := dayBounds(year, month, d)
		cursor = dStart
	}

	slices.Reverse(days)
	return days, nil
}

func (g *GmailClient) ListDayMessages(ctx context.Context, labelID string, year, month, day int, cache *Cache) ([]MessageStub, error) {
	start, end := dayBounds(year, month, day)

	allIDs, err := g.listMessageIDs(ctx, labelID, start, end)
	if err != nil {
		return nil, err
	}

	stubMap, err := g.GetMessageStubs(ctx, allIDs, cache)
	if err != nil {
		return nil, err
	}

	stubs := make([]MessageStub, 0, len(allIDs))
	for _, id := range allIDs {
		if s, ok := stubMap[id]; ok {
			stubs = append(stubs, s)
		}
	}
	sort.Slice(stubs, func(i, j int) bool { return stubs[i].InternalDate < stubs[j].InternalDate })
	return stubs, nil
}

func (g *GmailClient) GetRawMessage(ctx context.Context, messageID string) ([]byte, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	slog.Debug("gmail API", slog.String("method", "messages.get"), slog.String("id", messageID), slog.String("format", "raw"))
	msg, err := g.svc.Users.Messages.Get(g.user, messageID).
		Format("raw").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("getting raw message %s: %w", messageID, err)
	}

	data, err := base64.URLEncoding.DecodeString(msg.Raw)
	if err != nil {
		return nil, fmt.Errorf("decoding raw message %s: %w", messageID, err)
	}

	return data, nil
}

func (g *GmailClient) GetProfile(ctx context.Context) (*gmail.Profile, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	slog.Debug("gmail API", slog.String("method", "users.getProfile"))
	return g.svc.Users.GetProfile(g.user).Context(ctx).Do()
}

var ErrHistoryExpired = errors.New("history ID expired")

type LabelChanges struct {
	MessageIDs map[string]bool
	HasDeleted bool
}

type HistoryChanges struct {
	Labels       map[string]*LabelChanges
	Added        map[string]bool // message IDs from MessagesAdded (affect All Mail)
	Deleted      map[string]bool // message IDs from MessagesDeleted (affect All Mail)
	NewHistoryID uint64
}

func getOrCreateLabelChanges(m map[string]*LabelChanges, labelID string) *LabelChanges {
	lc := m[labelID]
	if lc == nil {
		lc = &LabelChanges{MessageIDs: make(map[string]bool)}
		m[labelID] = lc
	}
	return lc
}

// Returns ErrHistoryExpired when the historyId is too old (HTTP 404).
func (g *GmailClient) SyncHistory(ctx context.Context, startHistoryId uint64) (*HistoryChanges, error) {
	changes := &HistoryChanges{
		Labels:       make(map[string]*LabelChanges),
		Added:        make(map[string]bool),
		Deleted:      make(map[string]bool),
		NewHistoryID: startHistoryId,
	}

	pageToken := ""
	for {
		if err := g.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		slog.Debug("gmail API", slog.String("method", "history.list"), slog.Uint64("startHistoryId", startHistoryId))
		req := g.svc.Users.History.List(g.user).
			StartHistoryId(startHistoryId).
			MaxResults(500).
			Context(ctx)
		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		resp, err := req.Do()
		if err != nil {
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
				return nil, ErrHistoryExpired
			}
			return nil, fmt.Errorf("listing history: %w", err)
		}

		if resp.HistoryId > changes.NewHistoryID {
			changes.NewHistoryID = resp.HistoryId
		}

		for _, h := range resp.History {
			for _, added := range h.MessagesAdded {
				changes.Added[added.Message.Id] = true
				for _, lid := range added.Message.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.MessageIDs[added.Message.Id] = true
				}
			}
			for _, removed := range h.MessagesDeleted {
				changes.Deleted[removed.Message.Id] = true
				for _, lid := range removed.Message.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.MessageIDs[removed.Message.Id] = true
					lc.HasDeleted = true
				}
			}
			for _, la := range h.LabelsAdded {
				for _, lid := range la.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.MessageIDs[la.Message.Id] = true
				}
			}
			for _, lr := range h.LabelsRemoved {
				for _, lid := range lr.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.MessageIDs[lr.Message.Id] = true
				}
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return changes, nil
}

// Messages that no longer exist (404) are silently skipped.
func (g *GmailClient) GetMessageStubs(ctx context.Context, messageIDs []string, cache stubCache) (map[string]MessageStub, error) {
	stubs := make(map[string]MessageStub, len(messageIDs))
	var uncached []string
	for _, id := range messageIDs {
		if cache != nil {
			if stub, err := cache.GetMessageStub(id); err == nil {
				stubs[id] = stub
				continue
			}
		}
		uncached = append(uncached, id)
	}

	var (
		mu       sync.Mutex
		firstErr error
	)
	var wg sync.WaitGroup
	for _, id := range uncached {
		mu.Lock()
		failed := firstErr != nil
		mu.Unlock()
		if failed {
			break
		}

		if err := g.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		wg.Go(func() {
			slog.Debug("gmail API", slog.String("method", "messages.get"), slog.String("id", id), slog.String("format", "metadata"))
			msg, err := g.svc.Users.Messages.Get(g.user, id).
				Format("metadata").
				MetadataHeaders("Subject").
				Context(ctx).
				Do()
			if err != nil {
				var apiErr *googleapi.Error
				if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
					return
				}
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("getting message %s metadata: %w", id, err)
				}
				mu.Unlock()
				return
			}

			subject := ""
			if msg.Payload != nil {
				for _, h := range msg.Payload.Headers {
					if h.Name == "Subject" {
						subject = h.Value
						break
					}
				}
			}
			stub := MessageStub{
				ID:           msg.Id,
				InternalDate: msg.InternalDate,
				Subject:      subject,
				SizeEstimate: msg.SizeEstimate,
			}
			mu.Lock()
			stubs[id] = stub
			mu.Unlock()

			if cache != nil {
				if cerr := cache.SetMessageStub(stub); cerr != nil {
					slog.Warn("cache write error", slog.String("msgID", id), slog.Any("err", cerr))
				}
			}
		})
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return stubs, nil
}
