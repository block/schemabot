//go:build integration

package sqlstore

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/utils"
)

type postgresHarness struct {
	db  *sql.DB
	dsn string
}

func (h postgresHarness) NewStorage(t *testing.T) storage.Storage {
	t.Helper()
	clearPostgresTables(t, h.db)
	return NewPostgres(h.db)
}

func (h postgresHarness) NewUnreachableStorage(t *testing.T) storage.Storage {
	t.Helper()
	db, err := sql.Open("pgx", h.dsn)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return NewPostgres(db)
}

func TestPostgresStorageParity(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_parity")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	storagetest.Run(t, h)
	t.Run("SettingsUpdatedAtAdvances", func(t *testing.T) { testPostgresSettingsUpdatedAtAdvances(t, h) })
	t.Run("LeaseGuardedApplyLogAppend", func(t *testing.T) { testPostgresLeaseGuardedApplyLogAppend(t, h) })
	t.Run("MarkMinimizedPreservesStamp", func(t *testing.T) { testPostgresMarkMinimizedPreservesStamp(t, h) })
	t.Run("MarkDeletedPreservesStamp", func(t *testing.T) { testPostgresMarkDeletedPreservesStamp(t, h) })
	t.Run("LockUpdatedAtAdvances", func(t *testing.T) { testPostgresLockUpdatedAtAdvances(t, h) })
	t.Run("LockAcquireSameOwnerConcurrent", func(t *testing.T) { testPostgresLockAcquireSameOwnerConcurrent(t, h) })
	t.Run("ApplyCommentReclaimStaleSummaryClaim", func(t *testing.T) { testPostgresApplyCommentReclaimStaleSummaryClaim(t, h) })
	t.Run("ApplyCommentMutationsStampUpdatedAt", func(t *testing.T) { testPostgresApplyCommentMutationsStampUpdatedAt(t, h) })
	t.Run("ApplyCommentClaimConversionRestartsStaleWindow", func(t *testing.T) { testPostgresApplyCommentClaimConversionRestartsStaleWindow(t, h) })
	t.Run("ApplyCommentProgressAuthorityStaleTakeover", func(t *testing.T) { testPostgresApplyCommentProgressAuthorityStaleTakeover(t, h) })
}

// backdatePostgresProgressObserverHeartbeat pushes an apply's progress-comment
// authority heartbeat past the stale window, simulating an observer that
// stopped renewing (crashed pod or cleared observer).
func backdatePostgresProgressObserverHeartbeat(t *testing.T, db *sql.DB, applyID int64) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `
		UPDATE apply_comments SET observer_heartbeat_at = now() - make_interval(secs => $1)
		WHERE apply_id = $2 AND comment_state = $3
	`, int64(storage.ProgressCommentAuthorityStaleAfter.Seconds())+1, applyID, state.Comment.Progress)
	require.NoError(t, err)
}

// testPostgresApplyCommentProgressAuthorityStaleTakeover verifies the
// crashed-holder handover of the progress-comment authority on PostgreSQL: a
// recorded owner whose heartbeat is older than the stale window transfers to
// the next claimant, exactly once — the takeover stamps a fresh heartbeat, so
// a third claimant loses again. Aging the heartbeat requires backdating
// observer_heartbeat_at with raw SQL, which the storage interface cannot
// express, so the scenario lives in each dialect suite; the fresh-row claim
// decisions run on both dialects through the parity suite.
func testPostgresApplyCommentProgressAuthorityStaleTakeover(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "comment_authority_stale_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApply(t, store, lock, "apply_comment_authority_stale", 723)

	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 555,
	}))

	held, err := store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-a/1/comment-observer")
	require.NoError(t, err)
	require.True(t, held, "first claim on an unowned progress comment must win")

	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-b/2/comment-observer")
	require.NoError(t, err)
	require.False(t, held, "a second owner must lose while the holder's heartbeat is fresh")

	backdatePostgresProgressObserverHeartbeat(t, h.db, apply.ID)

	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-b/2/comment-observer")
	require.NoError(t, err)
	assert.True(t, held, "a stale authority transfers to the next claimant")

	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-c/3/comment-observer")
	require.NoError(t, err)
	assert.False(t, held, "a just-transferred authority is fresh again; a third owner loses")
}

// backdatePostgresSummaryClaim pushes an apply's summary marker updated_at
// past the stale-claim window, simulating a publisher that crashed after
// claiming.
func backdatePostgresSummaryClaim(t *testing.T, db *sql.DB, applyID int64) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `
		UPDATE apply_comments SET updated_at = now() - make_interval(secs => $1)
		WHERE apply_id = $2 AND comment_state = $3
	`, int64(storage.SummaryClaimStaleAfter.Seconds())+1, applyID, state.Comment.Summary)
	require.NoError(t, err)
}

// testPostgresApplyCommentReclaimStaleSummaryClaim verifies crashed-publisher
// takeover on PostgreSQL: a claim sentinel older than the stale window
// transfers to the reclaimer (bumping updated_at so a second reclaimer
// loses), while a fresh sentinel, a missing marker, and a recorded real
// comment are all not reclaimable. Aging the sentinel requires backdating
// updated_at with raw SQL, which the storage interface cannot express, so
// the scenario lives in each dialect suite.
func testPostgresApplyCommentReclaimStaleSummaryClaim(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "comment_reclaim_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApply(t, store, lock, "apply_comment_reclaim", 720)

	reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "missing marker is not reclaimable")

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "fresh sentinel is an in-flight publish, not reclaimable")

	// Backdate the sentinel past the stale window to simulate a publisher that
	// crashed between claiming and posting.
	backdatePostgresSummaryClaim(t, h.db, apply.ID)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, reclaimed, "stale sentinel transfers to the reclaimer")

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a just-reclaimed sentinel is fresh again; a second reclaimer loses")

	// A recorded real comment is never reclaimable, however old.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	backdatePostgresSummaryClaim(t, h.db, apply.ID)
	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a posted summary must never be reclaimed")
}

// testPostgresApplyCommentMutationsStampUpdatedAt verifies that every comment
// mutation renews the row's updated_at on PostgreSQL. The summary-claim
// machinery reads the column as its freshness signal, and stamping it is the
// application's responsibility on every dialect — the PostgreSQL schema has
// no automatic renewal, so a dropped stamp would leave the backdated value in
// place. An upsert replay that writes identical values still counts as
// publisher activity and renews the timestamp.
func testPostgresApplyCommentMutationsStampUpdatedAt(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "comment_stamp_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApply(t, store, lock, "apply_comment_updated_at_stamp", 721)

	frozen := int64(900)
	comment := &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        1001,
		PendingFreezeCommentID: &frozen,
	}
	require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

	backdate := func(t *testing.T) time.Time {
		t.Helper()
		_, err := h.db.ExecContext(t.Context(), `
			UPDATE apply_comments SET updated_at = now() - interval '1 hour'
			WHERE apply_id = $1 AND comment_state = $2
		`, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		stale, err := store.ApplyComments().Get(t.Context(), apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, stale)
		return stale.UpdatedAt
	}

	// The subtests mutate one shared comment row and depend on this order:
	// "clear pending freeze" only matches because the preceding upserts re-set
	// the freeze marker, and "supersede" must run last because it retires the
	// row from further mutation.
	mutations := []struct {
		name   string
		mutate func(t *testing.T)
	}{
		{"upsert conflict update", func(t *testing.T) {
			comment.GitHubCommentID = 1002
			require.NoError(t, store.ApplyComments().Upsert(ctx, comment))
		}},
		{"identical-value upsert replay", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().Upsert(ctx, comment))
		}},
		{"increment edit count", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().IncrementEditCount(ctx, apply.ID, state.Comment.Progress))
		}},
		{"clear pending freeze", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
		}},
		{"supersede", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Progress))
		}},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			stale := backdate(t)
			m.mutate(t)
			got, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.True(t, got.UpdatedAt.After(stale),
				"mutation must renew updated_at (stale=%s got=%s)", stale, got.UpdatedAt)
		})
	}
}

// testPostgresApplyCommentClaimConversionRestartsStaleWindow verifies on
// PostgreSQL that converting a superseded posted summary marker back into a
// claim sentinel restarts the stale-claim window. The conversion is a
// brand-new claim, so ReclaimStaleSummaryClaim must not immediately hand the
// same claim to a second publisher.
func testPostgresApplyCommentClaimConversionRestartsStaleWindow(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "comment_claim_fresh_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApply(t, store, lock, "apply_comment_claim_fresh", 722)

	// A stop's summary was posted and later consumed by a resume rotation,
	// then the row sat idle long past the stale window.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))
	backdatePostgresSummaryClaim(t, h.db, apply.ID)

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won, "the superseded posted marker converts back into a claim sentinel")

	reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a just-converted sentinel is a fresh in-flight publish, not reclaimable")
}

// testPostgresMarkMinimizedPreservesStamp reads the raw minimized_at column to
// prove a repeat mark does not move the original stamp. The row is backdated
// between marks so the assertion cannot pass on write-clock proximity alone.
func testPostgresMarkMinimizedPreservesStamp(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	comment := storagetest.InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)

	require.NoError(t, store.PlanComments().MarkMinimized(ctx, comment.ID))

	var minimizedAt *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT minimized_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&minimizedAt))
	require.NotNil(t, minimizedAt, "the row is stamped, not deleted")

	_, err := h.db.ExecContext(ctx,
		`UPDATE plan_comments SET minimized_at = now() - interval '1 hour' WHERE id = $1`, comment.ID)
	require.NoError(t, err)
	var backdated *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT minimized_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&backdated))
	require.NotNil(t, backdated)

	require.NoError(t, store.PlanComments().MarkMinimized(ctx, comment.ID))

	var afterRepeat *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT minimized_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&afterRepeat))
	require.NotNil(t, afterRepeat)
	assert.Equal(t, *backdated, *afterRepeat, "a repeat mark must not move the stamp")
}

// testPostgresMarkDeletedPreservesStamp reads the raw deleted_at column to
// prove a repeat mark does not move the original stamp. The row is backdated
// between marks so the assertion cannot pass on write-clock proximity alone.
func testPostgresMarkDeletedPreservesStamp(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	comment := storagetest.InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)

	require.NoError(t, store.PlanComments().MarkDeleted(ctx, comment.ID))

	var deletedAt *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT deleted_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&deletedAt))
	require.NotNil(t, deletedAt, "the row is stamped, not removed")

	_, err := h.db.ExecContext(ctx,
		`UPDATE plan_comments SET deleted_at = now() - interval '1 hour' WHERE id = $1`, comment.ID)
	require.NoError(t, err)
	var backdated *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT deleted_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&backdated))
	require.NotNil(t, backdated)

	require.NoError(t, store.PlanComments().MarkDeleted(ctx, comment.ID))

	var afterRepeat *time.Time
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT deleted_at FROM plan_comments WHERE id = $1`, comment.ID).Scan(&afterRepeat))
	require.NotNil(t, afterRepeat)
	assert.Equal(t, *backdated, *afterRepeat, "a repeat mark must not move the stamp")
}

// testPostgresSettingsUpdatedAtAdvances proves that a second Set renews
// updated_at through the upsert's explicit stamp. The row is backdated between
// writes so the assertion cannot pass on write-clock proximity alone; the
// PostgreSQL schema has no automatic renewal, so a dropped stamp would leave
// the backdated value in place.
func testPostgresSettingsUpdatedAtAdvances(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	require.NoError(t, store.Settings().Set(ctx, "stamp_key", "v1"))
	_, err := h.db.ExecContext(ctx,
		`UPDATE settings SET updated_at = now() - interval '1 hour' WHERE setting_key = $1`, "stamp_key")
	require.NoError(t, err)
	backdated, err := store.Settings().Get(ctx, "stamp_key")
	require.NoError(t, err)
	require.NotNil(t, backdated)

	require.NoError(t, store.Settings().Set(ctx, "stamp_key", "v2"))
	updated, err := store.Settings().Get(ctx, "stamp_key")
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, "v2", updated.Value)
	assert.True(t, updated.UpdatedAt.After(backdated.UpdatedAt),
		"second Set must advance updated_at (got %s, was %s)", updated.UpdatedAt, backdated.UpdatedAt)
	assert.True(t, updated.CreatedAt.Equal(backdated.CreatedAt), "Set must not rewrite created_at")
}

// testPostgresLeaseGuardedApplyLogAppend pins the guarded-identity insert
// contract against a real PostgreSQL server: a guarded INSERT ... SELECT whose
// lease predicate matches must insert and return the generated id, while a
// stale lease must surface as the lease-lost error rather than a hard failure —
// the RETURNING row's absence is the only signal distinguishing the two.
func testPostgresLeaseGuardedApplyLogAppend(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "guarded_logs_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_guarded_logs", 700, state.Apply.Running, "staging")

	_, err := h.db.ExecContext(ctx,
		`UPDATE applies SET lease_owner = $1, lease_token = $2, lease_acquired_at = now() WHERE id = $3`,
		"driver-a", "owned-token", apply.ID)
	require.NoError(t, err)

	ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-a", Token: "owned-token"})
	owned := &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   "owned driver log",
	}
	require.NoError(t, store.ApplyLogs().Append(ownedCtx, owned))
	assert.Positive(t, owned.ID, "guarded insert must return the generated id")

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-old", Token: "stale-token"})
	require.ErrorIs(t, store.ApplyLogs().Append(staleCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   "stale driver log",
	}), storage.ErrApplyLeaseLost)

	logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "owned driver log", logs[0].Message)
}

// testPostgresLockUpdatedAtAdvances proves that the liveness touch and the
// pending-plan refresh each renew updated_at through their explicit stamps.
// The row is backdated between writes so the assertion cannot pass on
// write-clock proximity alone; the PostgreSQL schema has no automatic renewal,
// so a dropped stamp would leave the backdated value in place.
func testPostgresLockUpdatedAtAdvances(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "stamp_db", storage.DatabaseTypeMySQL)

	backdate := func(t *testing.T) *storage.Lock {
		t.Helper()
		_, err := h.db.ExecContext(ctx,
			`UPDATE locks SET updated_at = now() - interval '1 hour' WHERE database_name = $1 AND database_type = $2`,
			lock.DatabaseName, lock.DatabaseType)
		require.NoError(t, err)
		got, err := store.Locks().Get(ctx, lock.DatabaseName, lock.DatabaseType)
		require.NoError(t, err)
		require.NotNil(t, got)
		return got
	}

	backdated := backdate(t)
	require.NoError(t, store.Locks().Update(ctx, lock))
	touched, err := store.Locks().Get(ctx, lock.DatabaseName, lock.DatabaseType)
	require.NoError(t, err)
	require.NotNil(t, touched)
	assert.True(t, touched.UpdatedAt.After(backdated.UpdatedAt),
		"Update must advance updated_at (got %s, was %s)", touched.UpdatedAt, backdated.UpdatedAt)

	backdated = backdate(t)
	refresh := &storage.Lock{
		DatabaseName:  lock.DatabaseName,
		DatabaseType:  lock.DatabaseType,
		Repository:    lock.Repository,
		PullRequest:   lock.PullRequest,
		Owner:         lock.Owner,
		PendingPlanID: "plan-refresh",
	}
	require.NoError(t, store.Locks().Acquire(ctx, refresh))
	refreshed, err := store.Locks().Get(ctx, lock.DatabaseName, lock.DatabaseType)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, "plan-refresh", refreshed.PendingPlanID)
	assert.True(t, refreshed.UpdatedAt.After(backdated.UpdatedAt),
		"pending-plan refresh must advance updated_at (got %s, was %s)", refreshed.UpdatedAt, backdated.UpdatedAt)
	assert.True(t, refreshed.CreatedAt.Equal(backdated.CreatedAt), "refresh must not rewrite created_at")
}

// testPostgresLockAcquireSameOwnerConcurrent forces the lock INSERT race on a
// real PostgreSQL server. Concurrent same-owner Acquire calls collide on the
// UNIQUE(database_name, database_type) constraint; every loser's duplicate-key
// error must be recognized by the classifier and resolved as a same-owner
// success rather than surfacing a hard error.
func testPostgresLockAcquireSameOwnerConcurrent(t *testing.T, h postgresHarness) {
	clearPostgresTables(t, h.db)
	ctx := t.Context()

	const drivers = 16
	stores := make([]*Storage, drivers)
	for i := range drivers {
		db, openErr := sql.Open("pgx", h.dsn)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		require.NoError(t, db.PingContext(ctx))
		stores[i] = NewPostgres(db)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for i := range drivers {
		driverStore := stores[i]
		planID := fmt.Sprintf("plan-%d", i)
		wg.Go(func() {
			<-start
			err := driverStore.Locks().Acquire(ctx, &storage.Lock{
				DatabaseName:  "concurrent_db",
				DatabaseType:  storage.DatabaseTypeMySQL,
				Repository:    "org/repo",
				PullRequest:   123,
				Owner:         "org/repo#123",
				PendingPlanID: planID,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
		})
	}

	close(start)
	wg.Wait()

	require.Empty(t, errs, "all same-owner Acquire calls should succeed")

	lock, err := stores[0].Locks().Get(ctx, "concurrent_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "org/repo#123", lock.Owner)
	assert.Contains(t, lock.PendingPlanID, "plan-", "stored plan should be one of the concurrent attempts")
}

func TestPGXStdlibValueContracts(t *testing.T) {
	_, db := testutil.StartPostgres(t, "sqlstore_values")
	_, err := db.ExecContext(t.Context(), `CREATE TABLE value_contracts (
		id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		flag boolean NOT NULL,
		payload jsonb NOT NULL,
		observed_at timestamptz NOT NULL,
		recorded_at timestamp NOT NULL,
		label text NOT NULL
	)`)
	require.NoError(t, err)

	wantTime := time.Date(2026, time.August, 11, 12, 34, 56, 123456000, time.FixedZone("test", 2*60*60))
	_, err = db.ExecContext(t.Context(),
		`INSERT INTO value_contracts (flag, payload, observed_at, recorded_at, label) VALUES ($1, $2, $3, $4, $5)`,
		true, []byte(`{"enabled":true}`), wantTime, wantTime.UTC(), "same")
	require.NoError(t, err)

	var gotBool bool
	var gotJSON []byte
	var gotTime time.Time
	var gotPlain time.Time
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT flag, payload, observed_at, recorded_at FROM value_contracts WHERE id = 1`,
	).Scan(&gotBool, &gotJSON, &gotTime, &gotPlain))
	assert.True(t, gotBool)
	assert.JSONEq(t, `{"enabled":true}`, string(gotJSON))
	assert.Equal(t, wantTime.UTC(), gotTime.UTC())
	assert.Equal(t, wantTime.Nanosecond(), gotTime.Nanosecond(), "timestamptz retains microsecond precision")
	// Every datetime column in the PostgreSQL schema is a plain timestamp
	// (without time zone), which stores a wall-clock reading with no instant
	// semantics. The stores' portability contract is therefore UTC-in/UTC-out:
	// a UTC value must round-trip byte-exact, so predicates comparing stored
	// values against server-side now() (lease expiry, retry windows) hold as
	// long as writers hand the driver UTC times.
	assert.Equal(t, wantTime.UTC(), gotPlain.UTC(), "plain timestamp round-trips a UTC write unchanged")
	assert.Equal(t, wantTime.Nanosecond(), gotPlain.Nanosecond(), "plain timestamp retains microsecond precision")

	result, err := db.ExecContext(t.Context(), `UPDATE value_contracts SET label = $1 WHERE id = $2`, "same", 1)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected, "PostgreSQL reports matched rows even when values are unchanged")
}

// TestPostgresApplyOperationLeaseGuards covers PostgreSQL-specific details the
// cross-dialect suite cannot express: operation-lease heartbeat timestamps,
// joined deletes, and a lease steal committed concurrently with a guarded
// write. The lease-token fence must lock the joined applies row, wait out the
// steal, and re-check the token against the stolen value.
func TestPostgresApplyOperationLeaseGuards(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_op_guards")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	store := h.NewStorage(t)
	ops := store.ApplyOperations()

	seedApply := func(t *testing.T, identifier, leaseToken string) int64 {
		t.Helper()
		var id int64
		require.NoError(t, db.QueryRowContext(t.Context(), `
			INSERT INTO applies (apply_identifier, lock_id, plan_id, database_name, database_type,
				repository, pull_request, environment, engine, state, options, lease_owner, lease_token)
			VALUES ($1, 1, 1, 'testdb', 'mysql', 'org/repo', 7, 'staging', 'spirit', 'running', '{}', 'owner', $2)
			RETURNING id`, identifier, leaseToken).Scan(&id))
		return id
	}
	seedOperation := func(t *testing.T, applyID int64, key, opState, leaseToken string) int64 {
		t.Helper()
		var id int64
		require.NoError(t, db.QueryRowContext(t.Context(), `
			INSERT INTO apply_operations (apply_id, deployment, operation_key, state, lease_owner, lease_token)
			VALUES ($1, 'dep-1', $2, $3, 'owner', $4)
			RETURNING id`, applyID, key, opState, leaseToken).Scan(&id))
		return id
	}
	operationState := func(t *testing.T, id int64) string {
		t.Helper()
		var got string
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT state FROM apply_operations WHERE id = $1`, id).Scan(&got))
		return got
	}

	t.Run("operation lease guards the single-table update", func(t *testing.T) {
		applyID := seedApply(t, "apply-guard-op", "tok-apply")
		opID := seedOperation(t, applyID, "op-1", "running", "tok-op")
		_, err := db.ExecContext(t.Context(),
			`UPDATE apply_operations SET updated_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, opID)
		require.NoError(t, err)
		var before time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT updated_at FROM apply_operations WHERE id = $1`, opID).Scan(&before))

		staleCtx := storage.WithOperationLease(t.Context(), storage.OperationLease{ApplyID: applyID, OperationID: opID, Owner: "owner", Token: "stale"})
		err = ops.Heartbeat(staleCtx, opID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

		ownerCtx := storage.WithOperationLease(t.Context(), storage.OperationLease{ApplyID: applyID, OperationID: opID, Owner: "owner", Token: "tok-op"})
		require.NoError(t, ops.Heartbeat(ownerCtx, opID))
		var after time.Time
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT updated_at FROM apply_operations WHERE id = $1`, opID).Scan(&after))
		assert.True(t, after.After(before), "heartbeat must advance updated_at (before=%v after=%v)", before, after)
	})

	t.Run("apply lease guards the joined delete", func(t *testing.T) {
		applyID := seedApply(t, "apply-guard-delete", "tok-apply")
		opID := seedOperation(t, applyID, "op-1", "pending", "")

		staleCtx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: applyID, Owner: "owner", Token: "stale"})
		err := ops.DeleteByApply(staleCtx, applyID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		assert.Equal(t, "pending", operationState(t, opID))

		ownerCtx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: applyID, Owner: "owner", Token: "tok-apply"})
		require.NoError(t, ops.DeleteByApply(ownerCtx, applyID))
		var remaining int
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM apply_operations WHERE apply_id = $1`, applyID).Scan(&remaining))
		assert.Zero(t, remaining)
	})

	t.Run("apply lease fence fails closed against a concurrent steal", func(t *testing.T) {
		applyID := seedApply(t, "apply-guard-steal", "tok-apply")
		opID := seedOperation(t, applyID, "op-1", "pending", "")

		// A competing driver steals the lease in a transaction that stays open
		// past the guarded write's start, so the guarded write's statement
		// snapshot still holds the pre-steal token.
		steal, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		committed := false
		t.Cleanup(func() {
			if !committed {
				_ = steal.Rollback()
			}
		})
		_, err = steal.ExecContext(t.Context(),
			`UPDATE applies SET lease_token = 'tok-thief' WHERE id = $1`, applyID)
		require.NoError(t, err)

		// The displaced driver's guarded write must block on the fence's row
		// lock instead of passing its token check against the stale snapshot.
		ownerCtx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: applyID, Owner: "owner", Token: "tok-apply"})
		result := make(chan error, 1)
		go func() { result <- ops.MarkStarted(ownerCtx, opID) }()

		require.Eventually(t, func() bool {
			var waiting int
			if err := db.QueryRowContext(t.Context(), `
				SELECT COUNT(*) FROM pg_stat_activity
				WHERE wait_event_type = 'Lock' AND query LIKE 'UPDATE apply_operations%'`).Scan(&waiting); err != nil {
				return false
			}
			return waiting > 0
		}, 15*time.Second, 25*time.Millisecond, "guarded write must block on the stolen row's lock")

		require.NoError(t, steal.Commit())
		committed = true

		select {
		case err := <-result:
			require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		case <-time.After(15 * time.Second):
			t.Fatal("guarded write did not return after the steal committed")
		}
		assert.Equal(t, "pending", operationState(t, opID))
	})
}

func applyPostgresTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := schema.PostgresFS.ReadDir("postgres")
	require.NoError(t, err)
	for _, entry := range entries {
		content, readErr := schema.PostgresFS.ReadFile("postgres/" + entry.Name())
		require.NoError(t, readErr)
		_, execErr := db.ExecContext(t.Context(), string(content))
		require.NoError(t, execErr, "execute %s", entry.Name())
	}
}

func clearPostgresTables(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	for _, table := range tables {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf(`TRUNCATE TABLE %q RESTART IDENTITY CASCADE`, table))
		require.NoError(t, err)
	}
}

// The stranded-active sweep defers to the operation lease on PostgreSQL exactly
// as it does on MySQL. The gate is rendered SQL rather than Go, so it has to be
// exercised on each dialect: the row here is stale by every window the sweep
// applies, leaving the lease as the only thing that can keep it.
func TestPostgresReapStrandedActiveDefersToTheOperationLease(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_reap_lease")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	store := h.NewStorage(t)

	// A settled parent, quiet long enough that no window on it protects anything.
	var applyID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO applies (apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, engine, state, options, error_message, updated_at)
		VALUES ('apply-reap-lease', 1, 1, 'testdb', 'mysql', 'org/repo', 7, 'staging', 'spirit', $1, '{}',
			'target rejected the schema change', NOW() - make_interval(secs => $2))
		RETURNING id`, state.Apply.Failed,
		int64((strandedActiveParentQuiescence+time.Minute).Seconds())).Scan(&applyID))

	// The operation a driver is holding, heartbeated as of now.
	var opID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO apply_operations (apply_id, deployment, operation_key, state, lease_owner, lease_token)
		VALUES ($1, 'region-a', 'op-1', $2, 'driver', 'op-token')
		RETURNING id`, applyID, state.ApplyOperation.Running).Scan(&opID))

	_, err = db.ExecContext(t.Context(), `
		INSERT INTO tasks (task_identifier, apply_id, apply_operation_id, plan_id, database_name,
			database_type, engine, repository, pull_request, environment, state, table_name, ddl,
			ddl_action, options, updated_at)
		VALUES ('task-reap-lease', $1, $2, 1, 'testdb', 'mysql', 'spirit', 'org/repo', 7, 'staging', $3,
			'users', 'ALTER TABLE users ADD COLUMN email varchar(255)', 'ALTER', '{}',
			NOW() - make_interval(secs => $4))`,
		applyID, opID, state.Task.Running,
		int64((strandedActiveTaskQuiescence + time.Minute).Seconds()))
	require.NoError(t, err)

	taskState := func(t *testing.T) string {
		t.Helper()
		var got string
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT state FROM tasks WHERE task_identifier = 'task-reap-lease'`).Scan(&got))
		return got
	}

	reaped, err := store.Tasks().ReapStrandedActive(t.Context(), 10)
	require.NoError(t, err)
	assert.Empty(t, reaped, "a driver holds the operation, so the row is not the reaper's to write")
	assert.Equal(t, state.Task.Running, taskState(t))

	// The driver goes away without releasing: its heartbeat ages past the point
	// where a peer would take the operation from it.
	_, err = db.ExecContext(t.Context(),
		`UPDATE apply_operations SET updated_at = NOW() - make_interval(secs => $1) WHERE id = $2`,
		int64((storage.ApplyLeaseStaleAfter + time.Minute).Seconds()), opID)
	require.NoError(t, err)

	reaped, err = store.Tasks().ReapStrandedActive(t.Context(), 10)
	require.NoError(t, err)
	require.Len(t, reaped, 1, "a lease a peer could reclaim no longer speaks for the row")
	assert.Equal(t, state.Task.Failed, taskState(t))
}

// The stranded-retryable sweep renders the same lease gate, and its own SQL is
// rendered separately from the active sweep's, so PostgreSQL has to execute it
// too. A failed_retryable row under a long-settled parent is the reaper's only
// once no driver holds the operation the retry would run under.
func TestPostgresReapStrandedRetryableDefersToTheOperationLease(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_reap_retry_lease")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	store := h.NewStorage(t)

	var applyID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO applies (apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, engine, state, options, error_message, updated_at)
		VALUES ('apply-reap-retry-lease', 1, 1, 'testdb', 'mysql', 'org/repo', 7, 'staging', 'spirit', $1, '{}',
			'', NOW() - make_interval(secs => $2))
		RETURNING id`, state.Apply.Completed,
		int64((strandedRetryableQuiescence+time.Minute).Seconds())).Scan(&applyID))

	// The operation a driver is holding, heartbeated as of now.
	var opID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO apply_operations (apply_id, deployment, operation_key, state, lease_owner, lease_token)
		VALUES ($1, 'region-a', 'op-1', $2, 'driver', 'op-token')
		RETURNING id`, applyID, state.ApplyOperation.Running).Scan(&opID))

	_, err = db.ExecContext(t.Context(), `
		INSERT INTO tasks (task_identifier, apply_id, apply_operation_id, plan_id, database_name,
			database_type, engine, repository, pull_request, environment, state, table_name, ddl,
			ddl_action, options, error_message)
		VALUES ('task-reap-retry-lease', $1, $2, 1, 'testdb', 'mysql', 'spirit', 'org/repo', 7, 'staging', $3,
			'users', 'ALTER TABLE users ADD COLUMN email varchar(255)', 'ALTER', '{}', 'copy failed')`,
		applyID, opID, state.Task.FailedRetryable)
	require.NoError(t, err)

	taskState := func(t *testing.T) string {
		t.Helper()
		var got string
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT state FROM tasks WHERE task_identifier = 'task-reap-retry-lease'`).Scan(&got))
		return got
	}

	reaped, err := store.Tasks().ReapStrandedRetryable(t.Context(), 10)
	require.NoError(t, err)
	assert.Empty(t, reaped, "a driver holds the operation, so the retry promise is not the reaper's to retire")
	assert.Equal(t, state.Task.FailedRetryable, taskState(t))

	_, err = db.ExecContext(t.Context(),
		`UPDATE apply_operations SET updated_at = NOW() - make_interval(secs => $1) WHERE id = $2`,
		int64((storage.ApplyLeaseStaleAfter + time.Minute).Seconds()), opID)
	require.NoError(t, err)

	reaped, err = store.Tasks().ReapStrandedRetryable(t.Context(), 10)
	require.NoError(t, err)
	require.Len(t, reaped, 1, "a lease a peer could reclaim no longer speaks for the row")
	assert.Equal(t, state.Task.Failed, taskState(t))
}

// Recovering a crashed retry admits one driver per staleness window on
// PostgreSQL as it does on MySQL. The gate is a rendered interval comparison,
// not Go, so each dialect has to execute it. Leasing the operation and claiming
// its parent apply are separate transactions, and the parent stays
// active-and-stale for the whole gap between them: without the operation's own
// heartbeat in the predicate, every peer polling inside that gap re-leases the
// same row and rotates its token. The row must still hand off promptly, so a
// driver that releases the claim leaves it takeable on the very next poll
// rather than after another window.
func TestPostgresFindNextApplyOperationCrashRecoveryAdmitsOneDriverPerWindow(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_crash_recovery_window")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	store := h.NewStorage(t)
	ops := store.ApplyOperations()

	staleSeconds := int64((storage.ApplyLeaseStaleAfter + time.Minute).Seconds())

	// A parent apply claimed for a retry, then crashed: active, budget
	// remaining, and no longer heartbeating.
	var applyID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO applies (apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, engine, state, attempt, options, updated_at)
		VALUES ('apply-crash-recovery-window', 1, 1, 'testdb', 'mysql', 'org/repo', 7, 'staging', 'spirit',
			$1, $2, '{}', NOW() - make_interval(secs => $3))
		RETURNING id`, state.Apply.Running, maxRecoveryAttempts-1, staleSeconds).Scan(&applyID))

	// The crashed driver stopped heartbeating the operation too.
	var opID int64
	require.NoError(t, db.QueryRowContext(t.Context(), `
		INSERT INTO apply_operations (apply_id, deployment, operation_key, state)
		VALUES ($1, 'region-a', 'op-1', $2)
		RETURNING id`, applyID, state.ApplyOperation.FailedRetryable).Scan(&opID))
	_, err = db.ExecContext(t.Context(),
		`UPDATE apply_operations SET updated_at = NOW() - make_interval(secs => $1) WHERE id = $2`,
		staleSeconds, opID)
	require.NoError(t, err)

	first, err := ops.FindNextApplyOperation(t.Context(), "driver-a")
	require.NoError(t, err)
	require.NotNil(t, first, "the first driver must recover the crashed retry")
	require.Equal(t, opID, first.ID)

	// driver-a holds the operation lease but has not reached ClaimApplyByID yet,
	// so the parent apply is still active and stale.
	second, err := ops.FindNextApplyOperation(t.Context(), "driver-b")
	require.NoError(t, err)
	assert.Nil(t, second, "a peer polling between the operation claim and the parent claim must not re-lease the recovering operation")

	persisted, err := ops.Get(t.Context(), opID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, "driver-a", persisted.LeaseOwner, "the recovering driver keeps the operation lease")
	assert.Equal(t, first.LeaseToken, persisted.LeaseToken, "the recovering driver's lease token must not be rotated out from under it")

	// The driver that cannot acquire the parent apply hands the operation back,
	// and the peer holding the parent takes it without waiting out a window.
	released, err := ops.ReleaseClaim(t.Context(), first.Lease())
	require.NoError(t, err)
	require.True(t, released)

	reclaimed, err := ops.FindNextApplyOperation(t.Context(), "parent-holder")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "a released crash-recovery row must be claimable on the next poll without aging")
	assert.Equal(t, opID, reclaimed.ID)
	assert.Equal(t, "parent-holder", reclaimed.LeaseOwner)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, reclaimed.State, "the handoff must not change the row's state")
}
