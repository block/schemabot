// webhook_events.go implements WebhookEventStore for durable webhook ingestion.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/storage"
)

const webhookEventColumns = `id, provider, delivery_id, event, action, repository, pull_request, head_sha, tenant_id,
	payload, state, attempts, lease_owner, lease_token, lease_expires_at, retry_after, last_error,
	received_at, started_at, completed_at, created_at, updated_at`

type webhookEventStore struct {
	db         *rebindDB
	dialect    Dialect
	identity   identityInserter
	classifier ErrorClassifier
}

func (s *webhookEventStore) Create(ctx context.Context, event *storage.WebhookEvent) (bool, error) {
	if event.DeliveryID == "" {
		return false, fmt.Errorf("webhook delivery ID is required")
	}
	if event.Event == "" {
		return false, fmt.Errorf("webhook event type is required")
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	payload := nullJSON(event.Payload)
	// New deliveries are always pending. Accepting any other state here would
	// let a caller persist a row that no lifecycle path can move — e.g. a
	// "processing" row with NULL lease columns is never claimable and never
	// expires, yet its delivery GUID dedups all future redeliveries into
	// no-ops, silently wedging that delivery forever.
	state := event.State
	if state == "" {
		state = storage.WebhookEventPending
	}
	if state != storage.WebhookEventPending {
		return false, fmt.Errorf("create webhook event (delivery_id=%s): new deliveries must be pending, got %q", event.DeliveryID, state)
	}
	receivedAt := event.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	// A caller-set RetryAfter is a not-before time: the row is durable
	// immediately but stays invisible to FindNext until the time passes.
	// Producers of deferred work — a redundant convergence signal that should
	// lose the race to the primary delivery — use it to schedule dispatch
	// without holding the delivery outside the inbox.
	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO webhook_events (
			provider, delivery_id, event, action, repository, pull_request, head_sha, tenant_id,
			payload, state, attempts, retry_after, received_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, provider, event.DeliveryID, event.Event, event.Action, event.Repository, event.PullRequest, event.HeadSHA, event.TenantID,
		payload, state, event.Attempts, nullTimePtr(event.RetryAfter), receivedAt)
	if err != nil {
		if s.classifier.IsDuplicateKey(err) {
			return s.reopenTerminalWebhookEvent(ctx, provider, event.DeliveryID, payload, receivedAt)
		}
		return false, fmt.Errorf("insert webhook event (provider=%s, delivery_id=%s): %w", provider, event.DeliveryID, err)
	}
	event.ID = id
	event.Provider = provider
	event.State = state
	event.Payload = []byte(payload)
	event.ReceivedAt = receivedAt
	return true, nil
}

// reopenTerminalWebhookEvent handles the duplicate-GUID branch of Create.
// GitHub's "Redeliver" reuses the original delivery GUID, so plain dedup would
// make redelivery a permanent no-op for a terminal row — the one case where an
// operator most needs it to work. Re-open:
//   - superseded rows: the row's work was discarded at claim time in favor of
//     a covering successor; an operator redelivering it explicitly asks for it
//     to run, and the reopen's refreshed received_at makes it the PR's newest
//     delivery so a fresh claim cannot immediately supersede it again.
//   - failed and completed rows: a completed row can still have lost its
//     follow-on work if the process died after the delivery was marked completed
//     but before the detached plan goroutines durably recorded their plans.
//     Re-running auto-plan on the same head SHA is idempotent, and re-running a
//     mutating command converges: the command cores re-evaluate the lock,
//     active-apply, and re-plan gates from current state — and unlock bounds
//     its release targets to locks created before the redelivery was received
//     (this reopen refreshes received_at) — so a redelivered succeeded command
//     answers from the state current at redelivery rather than double-driving
//     a live apply or releasing a lock newer than the delivery it processes.
//   - processing rows whose lease has expired: a driver hard-killed on its final
//     attempt leaves the row parked in processing with attempts at the cap,
//     which FindNext never reclaims — so it would otherwise be recoverable only
//     by the reconciler's periodic sweep. Matching it here makes Redeliver an
//     immediate recovery lever. An expired lease means no live owner, so the
//     reopen can't race a real driver, and the lease-token guard on
//     MarkCompleted/MarkFailed still rejects a returning zombie driver.
//   - permanently failed (dead-lettered) rows: Redeliver is the one sanctioned
//     lever that reopens them, and only organic GUIDs have one — a synthesized
//     GUID has no corresponding GitHub delivery, so its dead-lettered row is
//     never reopened and its head advances only through a fresh delivery (a
//     new head push or a check re-run). The reconciler cannot resurrect a
//     dead-lettered head — HasEventForHead reports it covered, so synthesis
//     never re-Creates its GUID — which leaves the reopen reachable only
//     through an operator's explicit redelivery, where a fresh attempt budget
//     is the point.
//
// pending/retryable rows and processing rows with a live (unexpired) lease are
// genuinely in flight, so they dedup (return false). last_error is kept for
// forensics until the next attempt overwrites it. The reopen clears
// retry_after: Redeliver is an operator recovery lever, so the reopened row is
// claimable immediately rather than re-deferred.
func (s *webhookEventStore) reopenTerminalWebhookEvent(ctx context.Context, provider, deliveryID, payload string, receivedAt time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = 0, payload = ?, received_at = ?,
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, completed_at = NULL, started_at = NULL, updated_at = NOW()
		WHERE provider = ? AND delivery_id = ?
			AND (state IN (?, ?, ?, ?) OR (state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+`))
	`, storage.WebhookEventPending, payload, receivedAt, provider, deliveryID,
		storage.WebhookEventFailed, storage.WebhookEventFailedPermanent, storage.WebhookEventCompleted, storage.WebhookEventSuperseded, storage.WebhookEventProcessing)
	if err != nil {
		return false, fmt.Errorf("reopen webhook delivery for redelivery (provider=%s, delivery_id=%s): %w", provider, deliveryID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reopened webhook delivery rows affected (provider=%s, delivery_id=%s): %w", provider, deliveryID, err)
	}
	return rows > 0, nil
}

func (s *webhookEventStore) GetByDeliveryID(ctx context.Context, provider, deliveryID string) (*storage.WebhookEvent, error) {
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT `+webhookEventColumns+`
		FROM webhook_events
		WHERE provider = ? AND delivery_id = ?
	`, provider, deliveryID)
	return scanWebhookEvent(row)
}

// webhookClaimablePredicate matches exactly the rows a driver would claim: a
// pending row whose not-before time (retry_after) is unset or has passed, a
// retryable row whose retry window has elapsed and is under the attempt cap,
// or a processing row whose lease has expired and is under the attempt cap.
// FindNext and the InboxStats backlog-age query derive from this single source
// so the "ready to claim" definition cannot drift between what a driver picks
// up and what the backlog gauge measures. Bind its placeholders with
// webhookClaimableArgs.
func (s *webhookEventStore) webhookClaimablePredicate() string {
	return `(
			(state = ? AND (retry_after IS NULL OR retry_after <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionDefault) + `))
			OR (state = ? AND (retry_after IS NULL OR retry_after <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionDefault) + `) AND attempts < ?)
			OR (state = ? AND lease_expires_at <= ` + s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond) + ` AND attempts < ?)
		)`
}

// webhookClaimableArgs returns the placeholder bindings for
// webhookClaimablePredicate, in order.
func webhookClaimableArgs() []any {
	return []any{
		storage.WebhookEventPending,
		storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts,
		storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts,
	}
}

func (s *webhookEventStore) HasEventForHead(ctx context.Context, provider, repository string, pullRequest int, headSHA string) (bool, error) {
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	if repository == "" || pullRequest == 0 || headSHA == "" {
		return false, fmt.Errorf("repository, pull request, and head SHA are required")
	}
	// Coverage means "a delivery exists that plans (or planned) this head", so
	// only auto-plannable pull_request rows count. Other event types can carry
	// the same PR + head SHA without planning it — a pull_request.closed row
	// from before a reopen, or a check_run row — and matching them would mask
	// the very loss the reconciler exists to recover. A terminally failed
	// organic row still covers its head: its GitHub Redeliver lever is the
	// operator's remediation, and synthesizing over it would loop a
	// deterministically failing head through a fresh claim budget every
	// pass. Only a plain failed synthesized row (the
	// SynthesizedWebhookDeliveryIDPrefix GUID form) fails to cover — there
	// is no Redeliver lever for a synthesized GUID, so the reconciler must
	// be able to synthesize a fresh recovery delivery, which lands in
	// Create's duplicate-GUID branch and reopens the failed row. A
	// permanently failed (dead-lettered) row covers its head in either GUID
	// form: the driver proved no retry can plan that head, so resurrecting
	// it would replay the same deterministic failure on every reconcile
	// pass. A superseded row never covers: its work was discarded on the
	// promise that a covering successor performs it, so it cannot itself
	// attest coverage — when the successor's coverage lapses (e.g. it
	// targeted a different head and the PR reopened on this one), the
	// reconciler must be free to synthesize.
	args := []any{provider, repository, pullRequest, headSHA}
	args = append(args, stringArgs(storage.AutoPlanPullRequestActions)...)
	args = append(args, storage.WebhookEventSuperseded, storage.WebhookEventFailed, storage.SynthesizedWebhookDeliveryIDPrefix+"%")
	var one int
	err := s.db.QueryRowContext(ctx, `
		SELECT 1
		FROM webhook_events
		WHERE provider = ? AND repository = ? AND pull_request = ? AND head_sha = ?
			AND event = 'pull_request' AND action IN (`+placeholders(len(storage.AutoPlanPullRequestActions))+`)
			AND state <> ?
			AND (state <> ? OR delivery_id NOT LIKE ?)
		LIMIT 1
	`, args...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// coveringSuccessorQuery builds the covering-successor predicate shared by
// HasCoveringSuccessor and SupersedeIfCovered as a standalone SELECT 1 ...
// LIMIT 1 query with its arguments, so the advisory probe and the guarded
// write can never drift apart on what counts as a covering successor.
func (s *webhookEventStore) coveringSuccessorQuery(provider string, event *storage.WebhookEvent) (string, []any) {
	autoPlanIn := placeholders(len(storage.AutoPlanPullRequestActions))
	args := []any{provider, event.Repository, event.PullRequest, event.ReceivedAt}
	args = append(args, stringArgs(storage.AutoPlanPullRequestActions)...)
	args = append(args,
		storage.PullRequestClosedAction,
		storage.WebhookEventPending, storage.WebhookEventCompleted,
		storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts,
		storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts)
	query := `
		SELECT 1
		FROM webhook_events
		WHERE provider = ? AND repository = ? AND pull_request = ?
			AND received_at > ?
			AND event = 'pull_request'
			AND (action IN (` + autoPlanIn + `) OR action = ?)
			AND (
				state IN (?, ?)
				OR (state = ? AND attempts < ?)
				OR (state = ? AND (lease_expires_at > ` + s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond) + ` OR attempts < ?))
			)
		LIMIT 1`
	return query, args
}

// HasCoveringSuccessor is the read-only advisory probe for the covering
// predicate SupersedeIfCovered enforces: it reports whether a covering
// successor currently exists, without touching the claimed row or its lease.
// The dispatcher uses it to decide whether verifying the claimed head against
// the live PR is worth a GitHub call at all — the common no-successor claim
// stays storage-only.
func (s *webhookEventStore) HasCoveringSuccessor(ctx context.Context, event *storage.WebhookEvent) (bool, error) {
	if event.Repository == "" || event.PullRequest == 0 {
		return false, fmt.Errorf("check covering successor for webhook event %d: repository and pull request are required for coalescing", event.ID)
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	query, args := s.coveringSuccessorQuery(provider, event)
	var one int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check covering successor for webhook event %d (repo=%s, pr=%d): %w", event.ID, event.Repository, event.PullRequest, err)
	}
	return true, nil
}

// SupersedeIfCovered marks a claimed auto-plannable pull_request event
// superseded when a strictly newer covering delivery exists for the same
// (provider, repository, pull request). One conditional UPDATE does the whole
// check-and-supersede: the lease-token and processing-state guards make it
// safe against a lost claim, and the auto-plan-only guard on the updated row
// keeps closed deliveries from ever being superseded. The covering-successor
// subquery is wrapped in a derived table because MySQL rejects a direct
// self-reference to the update target in a subquery.
//
// Newness is a strictly greater received_at, not insertion order: a Redeliver
// reopen refreshes received_at on its original row, so the reopened row is
// the PR's newest delivery and pre-existing rows cannot supersede the
// operator's explicit request. Deliveries whose received_at ties at the
// column's timestamp precision never cover each other — they simply both
// process, which is the safe direction for an optimization. received_at is
// arrival order, not push order, so a newer row can still carry an older
// head; the caller confirms against the live PR that the claimed head is no
// longer current before calling (see the interface contract). A successor
// covers only in states whose work will run, is running, or has run —
// pending, completed, retryable under the attempt cap, or processing with a
// live lease or reclaimable under the cap. A processing row at the cap with
// an expired lease is a dead driver's leftover that only terminalizes, and
// terminally failed / superseded rows never run — none of those may justify
// discarding older work.
func (s *webhookEventStore) SupersedeIfCovered(ctx context.Context, event *storage.WebhookEvent) (bool, error) {
	if event.LeaseToken == "" {
		return false, fmt.Errorf("webhook event lease token is required")
	}
	if event.Repository == "" || event.PullRequest == 0 {
		return false, fmt.Errorf("supersede webhook event %d: repository and pull request are required for coalescing", event.ID)
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	autoPlanIn := placeholders(len(storage.AutoPlanPullRequestActions))
	successorQuery, successorArgs := s.coveringSuccessorQuery(provider, event)
	args := []any{storage.WebhookEventSuperseded, event.ID, event.LeaseToken, storage.WebhookEventProcessing}
	args = append(args, stringArgs(storage.AutoPlanPullRequestActions)...)
	args = append(args, successorArgs...)
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ? AND state = ?
			AND event = 'pull_request' AND action IN (`+autoPlanIn+`)
			AND EXISTS (
				SELECT 1 FROM (`+successorQuery+`) AS covering_successor
			)
	`, args...)
	if err != nil {
		return false, fmt.Errorf("supersede webhook event %d (repo=%s, pr=%d): %w", event.ID, event.Repository, event.PullRequest, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read superseded webhook event %d rows affected: %w", event.ID, err)
	}
	if rows > 0 {
		event.State = storage.WebhookEventSuperseded
		return true, nil
	}
	// No row updated: either no covering successor exists (the claim is still
	// held, keep processing) or the claim was lost. checkWebhookEventLeaseResult
	// distinguishes the two by re-reading the lease token.
	if err := s.checkWebhookEventLeaseResult(ctx, result, event.ID, event.LeaseToken); err != nil {
		return false, err
	}
	return false, nil
}

func (s *webhookEventStore) FindNext(ctx context.Context, owner string, leaseDuration time.Duration) (*storage.WebhookEvent, error) {
	if owner == "" {
		return nil, fmt.Errorf("webhook driver owner is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("webhook lease duration must be positive")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim webhook event transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim webhook event")

	// Claim in two steps so the ordering filesort only ever handles narrow sort
	// records. No index satisfies this ordering across the claimable
	// predicate's OR-branches, so the sort packs every selected column into
	// each sort record — selecting the payload JSON here would let a single
	// oversized delivery exceed sort_buffer_size and fail every claim attempt
	// (MySQL error 1038), wedging the whole inbox behind one wide row.
	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM webhook_events
		WHERE `+s.webhookClaimablePredicate()+`
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, webhookClaimableArgs()...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable webhook event: %w", err)
	}

	// The claim above holds the row's exclusive lock for the rest of the
	// transaction, so this primary-key load sees a stable row. A missing row
	// here is an invariant violation, not an empty inbox.
	row := tx.QueryRowContext(ctx, `
		SELECT `+webhookEventColumns+`
		FROM webhook_events
		WHERE id = ?
	`, id)
	event, err := scanWebhookEventInto(row)
	if err != nil {
		return nil, fmt.Errorf("load claimed webhook event %d: %w", id, err)
	}

	leaseToken := uuid.NewString()
	now := time.Now()
	leaseExpiresAt := now.Add(leaseDuration)
	_, err = tx.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = attempts + 1, lease_owner = ?, lease_token = ?,
			lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`,
			retry_after = NULL, started_at = COALESCE(started_at, NOW()), updated_at = NOW()
		WHERE id = ?
	`, storage.WebhookEventProcessing, owner, leaseToken, leaseDuration.Microseconds(), event.ID)
	if err != nil {
		return nil, fmt.Errorf("claim webhook event %d: %w", event.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim webhook event %d: %w", event.ID, err)
	}

	// Reflect the committed claim on the scanned row instead of reloading it,
	// avoiding a second round trip that would re-transfer the full payload. The
	// row was held under FOR UPDATE for the whole transaction, so no other writer
	// could have changed it. lease_expires_at is the application-clock estimate of
	// the database's NOW(6)+leaseDuration; callers schedule heartbeats from
	// leaseDuration, not from this absolute value.
	event.State = storage.WebhookEventProcessing
	event.Attempts++
	event.LeaseOwner = owner
	event.LeaseToken = leaseToken
	event.LeaseExpiresAt = &leaseExpiresAt
	event.ClaimableSince = event.ReceivedAt
	if event.RetryAfter != nil && event.RetryAfter.After(event.ClaimableSince) {
		event.ClaimableSince = *event.RetryAfter
	}
	event.RetryAfter = nil
	if event.StartedAt == nil {
		event.StartedAt = &now
	}
	event.UpdatedAt = now

	return event, nil
}

func (s *webhookEventStore) Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error {
	if leaseToken == "" {
		return fmt.Errorf("webhook event lease token is required")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("webhook lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET lease_expires_at = `+s.dialect.RelativeTime(TimestampPrecisionMicrosecond, AfterCurrentTime, ParameterIntervalAmount(), IntervalMicrosecond)+`, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, leaseDuration.Microseconds(), id, leaseToken)
	if err != nil {
		return fmt.Errorf("heartbeat webhook event %d: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// MarkCompleted marks a claimed event terminal-successful.
//
// Idempotent: the lease token is retained (not cleared) and completed_at is
// COALESCE-preserved, so a retry after a committed-but-unacknowledged first
// attempt still matches the row and is a no-op that returns nil, rather than
// misreporting the completion as a lost lease. A genuine reclaim rotates the
// token, so a write with a stale token still returns ErrWebhookEventLeaseLost.
func (s *webhookEventStore) MarkCompleted(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventCompleted, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d completed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// MarkFailed marks a claimed event failed. A non-nil retryAfter keeps it
// retryable after that time; nil makes the failure terminal.
//
// Idempotent for the same lease token, on the same rationale as MarkCompleted:
// the token is retained and completed_at is COALESCE-preserved for terminal
// failures. A retryable failure keeps completed_at NULL; the row becomes
// claimable again via retry_after, and FindNext rotates a fresh token on the
// next claim so the retained token cannot be reused to claim.
func (s *webhookEventStore) MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	state := storage.WebhookEventFailed
	if retryAfter != nil {
		state = storage.WebhookEventFailedRetryable
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, retry_after = ?,
			completed_at = CASE WHEN ? THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, state, nullString(errMsg), nullTimePtr(retryAfter), retryAfter == nil, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d failed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// MarkFailedPermanent dead-letters a claimed event whose failure the driver
// proved deterministic for its head: the row leaves every retry and
// reconciler-synthesis path (HasEventForHead reports the head covered) and is
// revived only by an explicit GitHub Redeliver through Create's
// duplicate-GUID reopen.
//
// Idempotent for the same lease token, on the same rationale as MarkCompleted:
// the token is retained and completed_at is COALESCE-preserved.
func (s *webhookEventStore) MarkFailedPermanent(ctx context.Context, id int64, leaseToken string, errMsg string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, retry_after = NULL,
			completed_at = COALESCE(completed_at, NOW()),
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventFailedPermanent, nullString(errMsg), id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d permanently failed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// Release re-queues a claimed event as pending and refunds the attempt the
// claim consumed. When this undoes the first claim (attempts == 1), started_at
// is cleared so a later claim re-derives it via COALESCE(started_at, NOW()) —
// otherwise an interrupted first claim would permanently pin started_at to the
// cancelled attempt's time and misreport when processing actually began. The
// started_at reset is ordered before the attempts decrement so the CASE reads
// the pre-decrement value.
func (s *webhookEventStore) Release(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?,
			started_at = CASE WHEN attempts <= 1 THEN NULL ELSE started_at END,
			attempts = GREATEST(attempts - 1, 0), lease_owner = NULL,
			lease_token = NULL, lease_expires_at = NULL, retry_after = NULL, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventPending, id, leaseToken)
	if err != nil {
		return fmt.Errorf("release webhook event %d: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// TerminateStuckProcessing marks failed every processing row whose lease has
// expired and whose attempts have reached the cap. FindNext stops reclaiming a
// processing row once attempts == MaxWebhookEventAttempts, so a driver
// hard-killed on its final attempt leaves the row stuck in processing until it
// is either terminalized here or reopened by a GitHub Redeliver (see
// reopenTerminalWebhookEvent). This sweep is the automatic complement to
// redelivery: it clears the lease (preserving completed_at) and emits the row
// as a durable failure so it surfaces in metrics/alerting and drains the
// stuck-processing gauge without operator action. Unlike MarkFailed it is
// lease-token-agnostic: the owning driver is gone, so there is no token to
// present. Returns the number of rows terminated.
func (s *webhookEventStore) TerminateStuckProcessing(ctx context.Context, reason string) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, completed_at = COALESCE(completed_at, NOW()),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, updated_at = NOW()
		WHERE state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+` AND attempts >= ?
	`, storage.WebhookEventFailed, nullString(reason),
		storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts)
	if err != nil {
		return 0, fmt.Errorf("terminate stuck processing webhook events: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read terminate stuck processing rows affected: %w", err)
	}
	return rows, nil
}

func scanWebhookEvent(row *sql.Row) (*storage.WebhookEvent, error) {
	event, err := scanWebhookEventInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return event, err
}

func scanWebhookEventInto(row scanner) (*storage.WebhookEvent, error) {
	var event storage.WebhookEvent
	var leaseOwner, leaseToken, lastError sql.NullString
	var leaseExpiresAt, retryAfter, startedAt, completedAt sql.NullTime
	err := row.Scan(
		&event.ID, &event.Provider, &event.DeliveryID, &event.Event, &event.Action, &event.Repository, &event.PullRequest,
		&event.HeadSHA, &event.TenantID, &event.Payload, &event.State, &event.Attempts,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &retryAfter, &lastError,
		&event.ReceivedAt, &startedAt, &completedAt, &event.CreatedAt, &event.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	event.LeaseOwner = leaseOwner.String
	event.LeaseToken = leaseToken.String
	event.LastError = lastError.String
	if leaseExpiresAt.Valid {
		event.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if retryAfter.Valid {
		event.RetryAfter = &retryAfter.Time
	}
	if startedAt.Valid {
		event.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		event.CompletedAt = &completedAt.Time
	}
	return &event, nil
}

// InboxStats returns a read-only snapshot of the inbox for observability. It
// runs three cheap aggregates rather than one query: heterogeneous aggregates
// (grouped counts, a MIN age over a filtered subset, and a filtered count) do
// not combine into a single index-friendly statement, and each is served by an
// existing index.
func (s *webhookEventStore) InboxStats(ctx context.Context) (*storage.WebhookInboxStats, error) {
	stats := &storage.WebhookInboxStats{CountsByState: make(map[string]int64, len(storage.WebhookEventStatesAll))}
	for _, state := range storage.WebhookEventStatesAll {
		stats.CountsByState[state] = 0
	}

	rows, err := s.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM webhook_events GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count webhook inbox rows by state: %w", err)
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan webhook inbox state count: %w", err)
		}
		stats.CountsByState[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook inbox state counts: %w", err)
	}

	// Oldest ready-to-claim row, using the exact predicate a driver claims by
	// (webhookClaimablePredicate) so the backlog gauge and dispatch agree on
	// what is claimable: a cap-exhausted retryable row is not counted (a driver
	// won't take it, so it isn't backlog), and an expired-lease processing row a
	// driver would reclaim is counted (real backlog when its driver crashed).
	// Age is measured from when the row became claimable — the later of
	// receipt and its retry_after — so a row that spent time deliberately
	// deferred (or waiting out a retry window) counts only its time spent
	// claimable, not its grace period, as backlog. The COALESCE keeps GREATEST
	// dialect-safe: MySQL's GREATEST returns NULL when any argument is NULL
	// while PostgreSQL's ignores NULLs. NULL (nothing waiting) scans into a
	// zero age.
	var oldestClaimableAt, databaseNow sql.NullTime
	err = s.db.QueryRowContext(ctx, `
		SELECT MIN(GREATEST(received_at, COALESCE(retry_after, received_at))), `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+`
		FROM webhook_events
		WHERE `+s.webhookClaimablePredicate()+`
	`, webhookClaimableArgs()...).Scan(&oldestClaimableAt, &databaseNow)
	if err != nil {
		return nil, fmt.Errorf("measure oldest claimable webhook inbox row: %w", err)
	}
	if oldestClaimableAt.Valid && databaseNow.Valid && databaseNow.Time.After(oldestClaimableAt.Time) {
		stats.OldestClaimableAge = databaseNow.Time.Sub(oldestClaimableAt.Time)
	}

	err = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM webhook_events
		WHERE state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+` AND attempts >= ?
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts).Scan(&stats.StuckProcessing)
	if err != nil {
		return nil, fmt.Errorf("count stuck processing webhook inbox rows: %w", err)
	}

	return stats, nil
}

func (s *webhookEventStore) checkWebhookEventLeaseResult(ctx context.Context, result sql.Result, id int64, leaseToken string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read webhook event %d lease write rows affected: %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	var currentToken sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT lease_token FROM webhook_events WHERE id = ?`, id).Scan(&currentToken)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrWebhookEventNotFound
	}
	if err != nil {
		return fmt.Errorf("verify webhook event %d lease token: %w", id, err)
	}
	if currentToken.Valid && currentToken.String == leaseToken {
		return nil
	}
	return storage.ErrWebhookEventLeaseLost
}
