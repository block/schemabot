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
	// hooks lets a test act between two statements that do not run in one
	// transaction, to pin the guard the second one carries. Both are nil
	// outside tests.
	hooks webhookEventHooks
}

type webhookEventHooks struct {
	afterDuplicateInsert func()
	afterPurgeSelect     func()
}

// maxCreateAttempts bounds how often Create re-inserts a delivery whose row a
// purge deleted between the duplicate-key insert and the reopen.
const maxCreateAttempts = 3

func (s *webhookEventStore) Create(ctx context.Context, event *storage.WebhookEvent) (bool, error) {
	if event.DeliveryID == "" {
		return false, fmt.Errorf("webhook delivery ID is required")
	}
	if event.Event == "" {
		return false, fmt.Errorf("webhook event type is required")
	}
	event.Repository = storage.CanonicalKey(event.Repository)

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
	// Timestamp columns store UTC wall clocks, and not every SQL driver
	// converts a zoned time.Time before binding it, so normalize here.
	receivedAt := event.ReceivedAt.UTC()
	if event.ReceivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	// A caller-set RetryAfter is a not-before time: the row is durable
	// immediately but stays invisible to FindNext until the time passes.
	// Producers of deferred work — a redundant convergence signal that should
	// lose the race to the primary delivery — use it to schedule dispatch
	// without holding the delivery outside the inbox.
	//
	// A duplicate GUID means the row exists, and the reopen decides whether to
	// run it again. Retention can delete a terminal row between the failed
	// insert and the reopen, so a reopen that matches nothing checks that the
	// row is still there and inserts again if it is gone: a delivery that is
	// acknowledged always has a row (AV-7).
	for attempt := 1; ; attempt++ {
		id, err := s.identity.InsertID(ctx, s.db, `
			INSERT INTO webhook_events (
				provider, delivery_id, event, action, repository, pull_request, head_sha, tenant_id,
				payload, state, attempts, retry_after, received_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, provider, event.DeliveryID, event.Event, event.Action, event.Repository, event.PullRequest, event.HeadSHA, event.TenantID,
			payload, state, event.Attempts, nullTimePtr(event.RetryAfter), receivedAt)
		if err == nil {
			event.ID = id
			event.Provider = provider
			event.State = state
			event.Payload = []byte(payload)
			event.ReceivedAt = receivedAt
			return true, nil
		}
		if !s.classifier.IsDuplicateKey(err) {
			return false, fmt.Errorf("insert webhook event (provider=%s, delivery_id=%s): %w", provider, event.DeliveryID, err)
		}
		if s.hooks.afterDuplicateInsert != nil {
			s.hooks.afterDuplicateInsert()
		}
		reopened, err := s.reopenTerminalWebhookEvent(ctx, provider, event.DeliveryID, payload, receivedAt)
		if err != nil {
			return false, err
		}
		if reopened {
			return true, nil
		}
		existing, err := s.GetByDeliveryID(ctx, provider, event.DeliveryID)
		if err != nil {
			return false, fmt.Errorf("check webhook delivery after its reopen matched no row (provider=%s, delivery_id=%s): %w", provider, event.DeliveryID, err)
		}
		if existing != nil {
			// The row is in flight, so this delivery is a duplicate.
			return false, nil
		}
		if attempt == maxCreateAttempts {
			return false, fmt.Errorf("create webhook event (provider=%s, delivery_id=%s): its row was deleted between the insert and the reopen %d times", provider, event.DeliveryID, attempt)
		}
	}
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
			AND (state IN (`+placeholders(len(storage.WebhookEventStatesTerminal))+`) OR (state = ? AND lease_expires_at <= `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+`))
	`, append(append([]any{storage.WebhookEventPending, payload, receivedAt, provider, deliveryID},
		stringArgs(storage.WebhookEventStatesTerminal)...), storage.WebhookEventProcessing)...)
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
// FindNext and the InboxStats claimable aggregate derive from this single source
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

// failedOrganicRowInCoolOff matches a failed row that still covers its head:
// an organic delivery (not the SynthesizedWebhookDeliveryIDPrefix GUID form)
// that failed less than WebhookEventFailedCoverageCoolOff ago. The cutoff is
// measured on the database clock, the same clock that stamped completed_at.
// Every terminal failure write stamps completed_at; updated_at, which every
// write stamps, stands in if it is ever unset. columnPrefix qualifies each
// column (for example "c."), or is empty for an unaliased table. Bind one
// placeholder: the synthesized-GUID LIKE pattern.
func (s *webhookEventStore) failedOrganicRowInCoolOff(columnPrefix string) string {
	cutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(storage.WebhookEventFailedCoverageCoolOff.Microseconds())), IntervalMicrosecond)
	return `(` + columnPrefix + `delivery_id NOT LIKE ? AND COALESCE(` + columnPrefix + `completed_at, ` + columnPrefix + `updated_at) > ` + cutoff + `)`
}

// webhookCoveringRow matches a row that counts as coverage for its head, the
// rule HasEventForHead applies to the row for a given head and the purge
// applies to decide which rows it must keep. Both derive from this one
// predicate so what the reconciler treats as planned and what retention
// protects cannot drift apart. alias is the table alias to qualify columns
// with, or empty for an unaliased table. Bind the arguments with
// webhookCoveringRowArgs.
//
// Coverage means an auto-plannable pull_request row that is not superseded, and
// that is not a failed row outside its cool-off or a failed synthesized row.
// See HasEventForHead for why each of those cases does or does not count.
func (s *webhookEventStore) webhookCoveringRow(alias string) string {
	columnPrefix := ""
	if alias != "" {
		columnPrefix = alias + "."
	}
	return `(` + columnPrefix + `event = 'pull_request' AND ` + columnPrefix + `action IN (` + placeholders(len(storage.AutoPlanPullRequestActions)) + `)
				AND ` + columnPrefix + `state <> ?
				AND (` + columnPrefix + `state <> ? OR ` + s.failedOrganicRowInCoolOff(columnPrefix) + `))`
}

// webhookCoveringRowArgs returns the placeholder bindings for
// webhookCoveringRow, in order.
func webhookCoveringRowArgs() []any {
	args := stringArgs(storage.AutoPlanPullRequestActions)
	return append(args, storage.WebhookEventSuperseded, storage.WebhookEventFailed, storage.SynthesizedWebhookDeliveryIDPrefix+"%")
}

func (s *webhookEventStore) HasEventForHead(ctx context.Context, provider, repository string, pullRequest int, headSHA string) (bool, error) {
	repository = storage.CanonicalKey(repository)
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
	// the very loss the reconciler exists to recover. A plain failed row
	// exhausted its transient retries, so a later attempt can still plan the
	// head. An organic one covers its head only for
	// WebhookEventFailedCoverageCoolOff after it failed: the window leaves
	// room for the operator's GitHub Redeliver, and once it passes the
	// reconciler synthesizes a recovery delivery so the PR is planned even
	// when nobody noticed the failure. A failed synthesized row (the
	// SynthesizedWebhookDeliveryIDPrefix GUID form) never covers — there is
	// no Redeliver lever for a synthesized GUID, so the reconciler must be
	// able to synthesize a fresh recovery delivery, which lands in Create's
	// duplicate-GUID branch and reopens the failed row. Either way the
	// reconciler's pass interval paces the retries. A permanently failed
	// (dead-lettered) row covers its head in either GUID form: the driver
	// proved no retry can plan that head, so resurrecting it would replay
	// the same deterministic failure on every reconcile pass. A superseded
	// row never covers: its work was discarded on the promise that a
	// covering successor performs it, so it cannot itself attest coverage —
	// when the successor's coverage lapses (e.g. it targeted a different
	// head and the PR reopened on this one), the reconciler must be free to
	// synthesize.
	args := append([]any{provider, repository, pullRequest, headSHA}, webhookCoveringRowArgs()...)
	var one int
	err := s.db.QueryRowContext(ctx, `
		SELECT 1
		FROM webhook_events
		WHERE provider = ? AND repository = ? AND pull_request = ? AND head_sha = ?
			AND `+s.webhookCoveringRow("")+`
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
	queryEvent := *event
	queryEvent.Repository = storage.CanonicalKey(queryEvent.Repository)
	if queryEvent.Repository == "" || queryEvent.PullRequest == 0 {
		return false, fmt.Errorf("check covering successor for webhook event %d: repository and pull request are required for coalescing", event.ID)
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	query, args := s.coveringSuccessorQuery(provider, &queryEvent)
	var one int
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check covering successor for webhook event %d (repo=%s, pr=%d): %w", event.ID, queryEvent.Repository, event.PullRequest, err)
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
	repository := storage.CanonicalKey(event.Repository)
	if event.LeaseToken == "" {
		return false, fmt.Errorf("webhook event lease token is required")
	}
	if repository == "" || event.PullRequest == 0 {
		return false, fmt.Errorf("supersede webhook event %d: repository and pull request are required for coalescing", event.ID)
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	autoPlanIn := placeholders(len(storage.AutoPlanPullRequestActions))
	queryEvent := *event
	queryEvent.Repository = repository
	successorQuery, successorArgs := s.coveringSuccessorQuery(provider, &queryEvent)
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
		return false, fmt.Errorf("supersede webhook event %d (repo=%s, pr=%d): %w", event.ID, repository, event.PullRequest, err)
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

	// The ORDER BY must be able to walk an index on (created_at, id) rather
	// than sort. The claimable predicate ORs across several states, so no
	// state-prefixed index serves the ordering; without one on the ordering
	// pair the planner collects every claimable row and sorts it. On InnoDB
	// that sort runs under FOR UPDATE and locks the whole candidate set before
	// LIMIT 1 applies, turning SKIP LOCKED into a serializer — each driver
	// skips everything a peer locked instead of taking the next free row.
	// PostgreSQL locks rows only after the sort, so a single row is ever
	// locked; there the index spares the sort itself. Each dialect's schema
	// file names that index by its own convention.
	//
	// The index bounds locking, not reads. The walk starts at the oldest row
	// and evaluates the claimable predicate per row, and in a healthy inbox
	// the oldest rows are terminal ones — so a claim reads across all retained
	// terminal history before reaching the first claimable row. Leading the
	// index with state instead would bound those reads but lose the ordering
	// (the predicate ORs across states), reinstating the sort. Bounding the
	// reads is a retention job: only purging terminal rows keeps the history
	// the walk crosses finite.
	//
	// Claiming stays two-step — id here, then a primary-key load — so a plan
	// that does sort (the planner may prefer another index, and an
	// already-bootstrapped PostgreSQL database gains this index only by hand)
	// handles narrow sort records. Packing the payload JSON into each sort
	// record would let a single oversized delivery exceed sort_buffer_size
	// and fail every claim attempt (MySQL error 1038), wedging the whole
	// inbox behind one wide row.
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

// MarkFailedUncharged marks a claimed event retryable after retryAfter and
// refunds the attempt the claim consumed. The write is limited to a row still
// in processing, so a repeat with the same lease token matches nothing, and
// checkWebhookEventLeaseResult reports it as success because the token is
// retained: the attempt is refunded once. started_at is kept, since
// processing did begin.
func (s *webhookEventStore) MarkFailedUncharged(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, retry_after = ?,
			attempts = GREATEST(attempts - 1, 0), updated_at = NOW()
		WHERE id = ? AND lease_token = ? AND state = ?
	`, storage.WebhookEventFailedRetryable, nullString(errMsg), retryAfter, id, leaseToken, storage.WebhookEventProcessing)
	if err != nil {
		return fmt.Errorf("mark webhook event %d failed without charging an attempt: %w", id, err)
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

// webhookPurgeCandidate matches the rows a purge may delete, for the
// webhook_events table reached through alias: a terminal row received before
// the cutoff that the purge is not required to keep. DeleteTerminalOlderThan
// (page select and delete re-check) and CountTerminalOlderThan all use it, so
// the count equals what a purge may remove.
//
// A row that counts as coverage for its head (webhookCoveringRow, the rule
// HasEventForHead applies) is kept unless a newer row that covers permanently
// exists for the same provider, repository, and pull request. Deleting the
// only covering row of a pull request that is still active would make the
// reconciler plan its head again, and replay a dead-lettered head's failure.
// Only a completed or permanently failed row covers permanently. A pending,
// processing, or retryable row, or a failed row inside its cool-off, covers
// now but can lose that coverage (it can fail, or be superseded), so it never
// justifies deleting an older row; it is itself never deleted while it covers.
// "Newer" means a higher id: ids follow insertion order and a Redeliver reopen
// keeps a row's id while it resets received_at, so ordering by received_at
// would let a redelivery of an old head make that old head's row the newest
// and expose the current head's row. A row with no pull request or repository
// can never cover a head, so it is not protected. A row that does not count as
// coverage (failed outside its cool-off, failed synthesized, superseded) is
// purgeable on age alone. The guard probes idx_provider_repo_pr (MySQL) and
// idx_webhook_events_provider_repo_pr (PostgreSQL).
//
// Bind the arguments with webhookPurgeCandidateArgs.
func (s *webhookEventStore) webhookPurgeCandidate(alias string, cutoff string) string {
	return `(` + alias + `.received_at < ` + cutoff + `
				AND ` + alias + `.state IN (` + placeholders(len(storage.WebhookEventStatesTerminal)) + `)
				AND (NOT ` + s.webhookCoveringRow(alias) + `
					OR ` + alias + `.pull_request = 0 OR ` + alias + `.repository = ''
					OR EXISTS (
						SELECT 1 FROM webhook_events newer
						WHERE newer.provider = ` + alias + `.provider
							AND newer.repository = ` + alias + `.repository
							AND newer.pull_request = ` + alias + `.pull_request
							AND newer.id > ` + alias + `.id
							AND ` + webhookPermanentCoveringRow("newer") + `)))`
}

// webhookPermanentCoveringRow matches a row whose coverage no later write
// takes away: an auto-plannable pull_request row in a completed or permanently
// failed state, in either GUID form. It is webhookCoveringRow restricted to
// the two states that cover with no time limit. Bind the arguments with
// webhookPermanentCoveringRowArgs.
func webhookPermanentCoveringRow(alias string) string {
	return `(` + alias + `.event = 'pull_request' AND ` + alias + `.action IN (` + placeholders(len(storage.AutoPlanPullRequestActions)) + `)
				AND ` + alias + `.state IN (?, ?))`
}

// webhookPermanentCoveringRowArgs returns the placeholder bindings for
// webhookPermanentCoveringRow, in order.
func webhookPermanentCoveringRowArgs() []any {
	args := stringArgs(storage.AutoPlanPullRequestActions)
	return append(args, storage.WebhookEventCompleted, storage.WebhookEventFailedPermanent)
}

// webhookPurgeCandidateArgs returns the placeholder bindings for
// webhookPurgeCandidate, in order.
func webhookPurgeCandidateArgs() []any {
	args := stringArgs(storage.WebhookEventStatesTerminal)
	args = append(args, webhookCoveringRowArgs()...)
	return append(args, webhookPermanentCoveringRowArgs()...)
}

// DeleteTerminalOlderThan deletes at most limit terminal rows received more
// than age ago, oldest first, and returns how many it deleted. It never
// deletes a pending, processing, or retryable row, however old, because those
// are work a driver still owns. It also keeps the newest covering row of each
// pull request; see webhookPurgeCandidate.
//
// The cutoff is rendered as database-clock SQL rather than computed from the
// caller's time.Now(), so the same age cuts at the same instant on every pod.
// received_at, not completed_at, dates a row: every row has it, and it is what
// the received_at index serves. The state test and the coverage guard are
// residual filters on the rows that index returns.
//
// A batch is two statements because MySQL rejects LIMIT inside an IN subquery
// and PostgreSQL has no DELETE ... LIMIT: select a page of primary keys, then
// delete by primary key. The DELETE repeats the whole candidate predicate, so
// a row that Redeliver reopened, or that a newer covering row no longer
// justifies deleting, between the two statements is not deleted. The
// predicate sits in a derived table because MySQL rejects a DELETE whose
// subquery reads the table it deletes from.
func (s *webhookEventStore) DeleteTerminalOlderThan(ctx context.Context, age time.Duration, limit int) (int64, error) {
	if age.Microseconds() <= 0 {
		return 0, fmt.Errorf("delete terminal webhook events: age must be positive, got %s", age)
	}
	if limit <= 0 {
		return 0, fmt.Errorf("delete terminal webhook events: limit must be positive, got %d", limit)
	}
	if limit > storage.MaxWebhookEventPurgeBatch {
		return 0, fmt.Errorf("delete terminal webhook events: limit %d is above the largest batch, %d", limit, storage.MaxWebhookEventPurgeBatch)
	}

	cutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(age.Microseconds())), IntervalMicrosecond)

	selectArgs := append(webhookPurgeCandidateArgs(), limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.id
		FROM webhook_events c
		WHERE `+s.webhookPurgeCandidate("c", cutoff)+`
		ORDER BY c.received_at, c.id
		LIMIT ?
	`, selectArgs...)
	if err != nil {
		return 0, fmt.Errorf("select terminal webhook events older than %s: %w", age, err)
	}
	defer utils.CloseAndLog(rows)
	ids := make([]any, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("scan terminal webhook event id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate terminal webhook event ids: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if s.hooks.afterPurgeSelect != nil {
		s.hooks.afterPurgeSelect()
	}

	deleteArgs := make([]any, 0, len(ids)+len(webhookPurgeCandidateArgs()))
	deleteArgs = append(deleteArgs, ids...)
	deleteArgs = append(deleteArgs, webhookPurgeCandidateArgs()...)
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM webhook_events
		WHERE id IN (
			SELECT purgeable.id FROM (
				SELECT c.id AS id
				FROM webhook_events c
				WHERE c.id IN (`+placeholders(len(ids))+`)
					AND `+s.webhookPurgeCandidate("c", cutoff)+`
			) AS purgeable
		)
	`, deleteArgs...)
	if err != nil {
		return 0, fmt.Errorf("delete %d terminal webhook events older than %s: %w", len(ids), age, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read deleted terminal webhook events rows affected: %w", err)
	}
	return deleted, nil
}

// CountTerminalOlderThan counts the rows DeleteTerminalOlderThan may delete for
// the same age, using the same candidate predicate (cutoff, states, and the
// newest-covering-row guard) so a dry run reports what a real purge would
// remove.
func (s *webhookEventStore) CountTerminalOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	if age.Microseconds() <= 0 {
		return 0, fmt.Errorf("count terminal webhook events: age must be positive, got %s", age)
	}

	cutoff := s.dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(age.Microseconds())), IntervalMicrosecond)
	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM webhook_events c
		WHERE `+s.webhookPurgeCandidate("c", cutoff)+`
	`, webhookPurgeCandidateArgs()...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count terminal webhook events older than %s: %w", age, err)
	}
	return count, nil
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
// (grouped counts, claimable count/age, and stuck-processing count) do not combine into a single index-friendly statement, and each is served by an
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

	// Count and oldest age share the exact predicate a driver claims by
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
		SELECT COUNT(*), MIN(GREATEST(received_at, COALESCE(retry_after, received_at))), `+s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)+`
		FROM webhook_events
		WHERE `+s.webhookClaimablePredicate()+`
	`, webhookClaimableArgs()...).Scan(&stats.ClaimableDepth, &oldestClaimableAt, &databaseNow)
	if err != nil {
		return nil, fmt.Errorf("measure claimable webhook inbox rows: %w", err)
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
