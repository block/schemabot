package webhook

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
)

// startNoticeMetadataKey names the PR notice state the webhook keeps in an
// apply's start request metadata, beside the fields the API stores there. A
// later start re-arms the request with fresh metadata, which clears it.
const startNoticeMetadataKey = "pr_notice"

// startNotice orders the two PR comments one start can get: its
// acknowledgement, posted by the start command, and the withdrawal notice,
// posted when a stop from outside the PR withdraws the start. The two are
// posted by different handlers, possibly on different pods, so the state
// lives on the start request and every change to it is made under the
// request's row lock.
type startNotice struct {
	// Withdrawn records that a stop withdrew this start before any driver acted
	// on it. Only the withdrawal itself sets it, so a start a driver ran is
	// never reported as withdrawn.
	Withdrawn bool `json:"withdrawn,omitempty"`
	// Acknowledged records that the PR has the start's acknowledgement, or that
	// none is owed because the start was not a command on the PR.
	Acknowledged bool `json:"acknowledged,omitempty"`
	// Notified records that the withdrawal notice was claimed for posting.
	Notified bool `json:"notified,omitempty"`
}

// noticeDue reports whether the withdrawal notice should be posted now: the
// start was withdrawn, the PR already has whatever acknowledgement it was owed,
// so the notice lands after it, and no one has claimed the notice yet.
func (n startNotice) noticeDue() bool {
	return n.Withdrawn && n.Acknowledged && !n.Notified
}

// startNoticeUpdate is what one change to a start's notice state found.
type startNoticeUpdate struct {
	// found is false when the apply has no start request.
	found bool
	// applied is false when mark declined to change the request.
	applied bool
	// claimed is true when this change made the notice due and claimed it, so
	// the caller posts it.
	claimed bool
	// notice is the state the change left.
	notice startNotice
}

// updateStartNotice applies mark to the start request's notice state under the
// request's row lock and claims the withdrawal notice if the change made it due.
// mark returns false to leave the request unchanged. mark runs again if a lock
// conflict retries the write, so it decides from its arguments alone.
func updateStartNotice(ctx context.Context, controls storage.ControlRequestStore, applyID int64, mark func(*storage.ApplyControlRequest, *startNotice) bool) (startNoticeUpdate, error) {
	var update startNoticeUpdate
	found, err := controls.UpdateMetadata(ctx, applyID, storage.ControlOperationStart, func(req *storage.ApplyControlRequest) ([]byte, error) {
		update = startNoticeUpdate{found: true}
		fields := map[string]json.RawMessage{}
		if len(req.Metadata) > 0 {
			if err := json.Unmarshal(req.Metadata, &fields); err != nil {
				return nil, fmt.Errorf("decode start request metadata: %w", err)
			}
		}
		if raw, ok := fields[startNoticeMetadataKey]; ok {
			if err := json.Unmarshal(raw, &update.notice); err != nil {
				return nil, fmt.Errorf("decode start request %s: %w", startNoticeMetadataKey, err)
			}
		}
		if !mark(req, &update.notice) {
			return nil, nil
		}
		update.applied = true
		if update.notice.noticeDue() {
			update.notice.Notified = true
			update.claimed = true
		}
		raw, err := json.Marshal(update.notice)
		if err != nil {
			return nil, fmt.Errorf("encode start request %s: %w", startNoticeMetadataKey, err)
		}
		fields[startNoticeMetadataKey] = raw
		metadata, err := json.Marshal(fields)
		if err != nil {
			return nil, fmt.Errorf("encode start request metadata: %w", err)
		}
		return metadata, nil
	})
	if err != nil {
		return startNoticeUpdate{}, fmt.Errorf("update PR notice state of the start request for apply %d: %w", applyID, err)
	}
	if !found {
		return startNoticeUpdate{}, nil
	}
	return update, nil
}
