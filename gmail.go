package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const AllMailLabelID = "_all_"

// isRetryable returns true for transient API errors worth retrying.
func isRetryable(err error) bool {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Code >= 500 || apiErr.Code == http.StatusTooManyRequests ||
		apiErr.Code == http.StatusBadRequest // transient failedPrecondition
}

// retryDo calls fn up to 3 times on retryable errors, with exponential backoff.
func retryDo[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	const maxAttempts = 3
	var zero T
	for attempt := range maxAttempts {
		result, err := fn()
		if err == nil || !isRetryable(err) || attempt == maxAttempts-1 {
			return result, err
		}
		delay := time.Duration(1<<attempt) * time.Second
		slog.Warn("retrying after server error", slog.Any("err", err), slog.Duration("backoff", delay))
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
		}
	}
	return zero, nil // unreachable
}

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

func (g *GmailClient) listMessages(ctx context.Context, labelID, query string) ([]string, error) {
	var allIDs []string
	pageToken := ""
	for {
		if err := g.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		slog.Debug("gmail API", slog.String("method", "messages.list"), slog.String("label", labelID), slog.String("q", query))
		req := g.messagesList(labelID).
			MaxResults(500).
			Context(ctx)
		if query != "" {
			req = req.Q(query)
		}
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
		slog.Info("listing messages", slog.String("label", labelID), slog.Int("fetched", len(allIDs)))
		pageToken = resp.NextPageToken
	}
	return allIDs, nil
}

func (g *GmailClient) ListAllMessageIDs(ctx context.Context, labelID string) ([]string, error) {
	return g.listMessages(ctx, labelID, "")
}

func (g *GmailClient) GetRawMessage(ctx context.Context, messageID string) ([]byte, error) {
	msg, err := retryDo(ctx, func() (*gmail.Message, error) {
		if err := g.limiter.Wait(ctx); err != nil {
			return nil, err
		}
		slog.Debug("gmail API", slog.String("method", "messages.get"), slog.String("id", messageID), slog.String("format", "raw"))
		return g.svc.Users.Messages.Get(g.user, messageID).
			Format("raw").
			Context(ctx).
			Do()
	})
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound {
			return nil, nil
		}
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
	Added   map[string]bool
	Removed map[string]bool
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
		lc = &LabelChanges{
			Added:   make(map[string]bool),
			Removed: make(map[string]bool),
		}
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
					lc.Added[added.Message.Id] = true
				}
			}
			for _, removed := range h.MessagesDeleted {
				changes.Deleted[removed.Message.Id] = true
				for _, lid := range removed.Message.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.Removed[removed.Message.Id] = true
				}
			}
			for _, la := range h.LabelsAdded {
				for _, lid := range la.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.Added[la.Message.Id] = true
				}
			}
			for _, lr := range h.LabelsRemoved {
				for _, lid := range lr.LabelIds {
					lc := getOrCreateLabelChanges(changes.Labels, lid)
					lc.Removed[lr.Message.Id] = true
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

	if len(uncached) > 0 {
		slog.Info("fetching message metadata",
			slog.Int("total", len(messageIDs)),
			slog.Int("cached", len(messageIDs)-len(uncached)),
			slog.Int("to_fetch", len(uncached)))
	}

	var (
		mu       sync.Mutex
		firstErr error
		fetched  int
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
			msg, err := retryDo(ctx, func() (*gmail.Message, error) {
				slog.Debug("gmail API", slog.String("method", "messages.get"), slog.String("id", id), slog.String("format", "metadata"))
				return g.svc.Users.Messages.Get(g.user, id).
					Format("metadata").
					MetadataHeaders("Subject").
					Context(ctx).
					Do()
			})
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
			fetched++
			n := fetched
			mu.Unlock()

			if n%500 == 0 {
				slog.Info("fetching message metadata",
					slog.Int("fetched", n), slog.Int("total", len(uncached)))
			}

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
