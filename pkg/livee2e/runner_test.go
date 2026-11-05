package livee2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// fakeSchemaBot plays GitHub and SchemaBot together: it stores the test pull
// request and answers each PR command with the comments SchemaBot posts,
// rendered from the real templates, while changing fakeTarget the way the
// apply would.
type fakeSchemaBot struct {
	mu       sync.Mutex
	target   *fakeTarget
	nextID   int64
	heads    map[string]string // branch → head SHA
	files    map[string]string // branch → test table file content
	prs      map[int]*fakePR
	comments map[int][]Comment
	checks   map[string]CheckRun // SHA → Check Run
	applies  map[string]string   // apply ID → table

	// copyOutrunsStop finishes a copying apply as soon as it starts, so no
	// stop can land.
	copyOutrunsStop bool
	// deferredCutsOverAlone finishes a deferred apply without a cutover
	// command.
	deferredCutsOverAlone bool
	// cancelRefusedAsFinished refuses every cancel because the apply already
	// finished, without updating its status comment.
	cancelRefusedAsFinished bool
	// startFinishesWithOnlyASummary finishes the copy on start and posts only
	// its summary, leaving the status comment at Stopped.
	startFinishesWithOnlyASummary bool
	// deleteBranchFails fails every branch delete.
	deleteBranchFails bool
	// deleteBranchErr, when set, is the error every branch delete fails with.
	deleteBranchErr error
	// applyRefusal, when set, is SchemaBot's reply to every apply command,
	// which then starts nothing.
	applyRefusal string
	// stopRefusedAsFinished refuses every stop because the apply already
	// finished.
	stopRefusedAsFinished bool
	// planReads counts the reads of each commit's Check Run; the first
	// planInProgressReads of them show the plan still running.
	planReads           map[string]int
	planInProgressReads int
	// planFails fails the plan of the commit that alters the table.
	planFails bool
	// finalCheckFails leaves the Check Run failed after the cutover.
	finalCheckFails bool
	// commentsFailures fails that many comment reads with a GitHub rate
	// limit before they succeed again.
	commentsFailures int
	// commentErr, when set, fails every comment the runner posts.
	commentErr error
	// onCommand, when set, sees every PR command once SchemaBot has
	// answered it.
	onCommand func(body string)
	// cancelUnanswered leaves every cancel without a reply.
	cancelUnanswered bool
	// commands records every PR command, in order.
	commands []string
}

type fakePR struct {
	branch string
	open   bool
}

func newFakeSchemaBot(target *fakeTarget) *fakeSchemaBot {
	return &fakeSchemaBot{
		target:    target,
		nextID:    100,
		heads:     map[string]string{"main": "sha-main"},
		files:     map[string]string{},
		prs:       map[int]*fakePR{},
		comments:  map[int][]Comment{},
		checks:    map[string]CheckRun{},
		applies:   map[string]string{},
		planReads: map[string]int{},
	}
}

func (f *fakeSchemaBot) id() int64 {
	f.nextID++
	return f.nextID
}

func (f *fakeSchemaBot) BranchHead(_ context.Context, branch string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sha, ok := f.heads[branch]
	if !ok {
		return "", fmt.Errorf("no branch %s", branch)
	}
	return sha, nil
}

func (f *fakeSchemaBot) CreateBranch(_ context.Context, branch, sha string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.heads[branch]; ok {
		return fmt.Errorf("create branch %s: Reference already exists", branch)
	}
	f.heads[branch] = sha
	return nil
}

func (f *fakeSchemaBot) DeleteBranch(_ context.Context, branch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteBranchErr != nil {
		return fmt.Errorf("delete branch %s: %w", branch, f.deleteBranchErr)
	}
	if f.deleteBranchFails {
		return fmt.Errorf("delete branch %s: GitHub is unavailable", branch)
	}
	delete(f.heads, branch)
	// GitHub closes a pull request whose head branch is deleted.
	for _, pr := range f.prs {
		if pr.branch == branch {
			pr.open = false
		}
	}
	return nil
}

func (f *fakeSchemaBot) PutFile(_ context.Context, branch, _, _ string, content []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sha := fmt.Sprintf("sha-%d", f.id())
	f.heads[branch] = sha
	f.files[branch] = string(content)
	f.checks[sha] = CheckRun{Status: "completed", Conclusion: "action_required", Title: "Schema change pending"}
	if f.planFails && strings.Contains(string(content), columnAfter) {
		f.checks[sha] = CheckRun{Status: "completed", Conclusion: "failure", Title: "Plan failed: unsafe change"}
	}
	return sha, nil
}

func (f *fakeSchemaBot) OpenPullRequest(_ context.Context, head, _, _, _ string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	number := len(f.prs) + 1
	f.prs[number] = &fakePR{branch: head, open: true}
	return number, nil
}

func (f *fakeSchemaBot) ClosePullRequest(_ context.Context, number int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prs[number].open = false
	return nil
}

func (f *fakeSchemaBot) OpenPullRequests(_ context.Context, prefix string) ([]PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []PullRequest
	for number, pr := range f.prs {
		if pr.open && strings.HasPrefix(pr.branch, prefix) {
			out = append(out, PullRequest{Number: number, Branch: pr.branch})
		}
	}
	return out, nil
}

func (f *fakeSchemaBot) PullRequestsForBranch(_ context.Context, branch string) ([]PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []PullRequest
	for number, pr := range f.prs {
		if pr.branch == branch {
			out = append(out, PullRequest{Number: number, Branch: pr.branch})
		}
	}
	return out, nil
}

func (f *fakeSchemaBot) Comments(ctx context.Context, number int) ([]Comment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		// Like a real client, a cancelled read fails.
		return nil, fmt.Errorf("list comments on pull request #%d: %w", number, err)
	}
	if f.commentsFailures > 0 {
		f.commentsFailures--
		return nil, fmt.Errorf("list comments on pull request #%d: %w", number,
			secondaryRateLimit(http.MethodGet, "https://api.github.com/repos/octo/schema/issues/1/comments"))
	}
	return append([]Comment(nil), f.comments[number]...), nil
}

func (f *fakeSchemaBot) CheckRun(_ context.Context, sha, _, _ string) (*CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.checks[sha]
	if !ok {
		return nil, nil
	}
	f.planReads[sha]++
	if f.planReads[sha] <= f.planInProgressReads {
		return &CheckRun{Status: "in_progress"}, nil
	}
	return &run, nil
}

func (f *fakeSchemaBot) CreateComment(_ context.Context, number int, body string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commentErr != nil {
		return 0, fmt.Errorf("comment on pull request #%d: %w", number, f.commentErr)
	}
	id := f.id()
	f.comments[number] = append(f.comments[number], Comment{ID: id, Author: "runner", Body: body})
	f.commands = append(f.commands, body)
	f.respond(number, strings.Fields(body))
	if f.onCommand != nil {
		f.onCommand(body)
	}
	return id, nil
}

// post adds a SchemaBot status comment for applyID in applyState.
func (f *fakeSchemaBot) post(number int, applyID, applyState string) {
	f.comments[number] = append(f.comments[number], Comment{
		ID:     f.id(),
		Author: "schemabot[bot]",
		IsBot:  true,
		Body:   renderStatus(applyID, applyState),
	})
}

func (f *fakeSchemaBot) respond(number int, args []string) {
	branch := f.prs[number].branch
	content := f.files[branch]
	table := strings.TrimPrefix(branch, BranchPrefix)
	switch args[1] {
	case "apply":
		if f.applyRefusal != "" {
			f.comments[number] = append(f.comments[number], Comment{
				ID: f.id(), Author: "schemabot[bot]", IsBot: true, Body: f.applyRefusal,
			})
			return
		}
		applyID := fmt.Sprintf("apply-%016x", f.id())
		f.applies[applyID] = table
		deferred := strings.Contains(strings.Join(args, " "), "--defer-cutover")
		switch {
		case !strings.Contains(content, columnAfter):
			f.target.create(table)
			f.post(number, applyID, state.Apply.Completed)
		case deferred && !f.deferredCutsOverAlone:
			f.post(number, applyID, state.Apply.WaitingForCutover)
		case f.copyOutrunsStop || deferred:
			f.target.alter(table)
			f.post(number, applyID, state.Apply.Completed)
		default:
			// The first progress comment names the apply before it shows a
			// status, as SchemaBot's does.
			f.comments[number] = append(f.comments[number], Comment{
				ID: f.id(), Author: "schemabot[bot]", IsBot: true,
				Body: templates.RenderApplyStarted(templates.ApplyStatusCommentData{
					ApplyID: applyID, Database: "testdb", Environment: "development",
					State: state.Apply.Running, Engine: "spirit",
				}),
			})
			f.post(number, applyID, state.Apply.Running)
		}
	case "stop":
		if f.stopRefusedAsFinished {
			f.target.alter(table)
			f.comments[number] = append(f.comments[number], Comment{
				ID: f.id(), Author: "schemabot[bot]", IsBot: true,
				Body: renderAlreadyFinished(state.Apply.Completed),
			})
			return
		}
		f.post(number, args[2], state.Apply.Stopped)
	case "start":
		if f.startFinishesWithOnlyASummary {
			f.target.alter(table)
			f.comments[number] = append(f.comments[number], Comment{
				ID: f.id(), Author: "schemabot[bot]", IsBot: true,
				Body: renderSummary(args[2], state.Apply.Completed),
			})
			return
		}
		f.post(number, args[2], state.Apply.Running)
	case "cancel":
		if f.cancelUnanswered {
			return
		}
		if f.cancelRefusedAsFinished {
			f.comments[number] = append(f.comments[number], Comment{
				ID: f.id(), Author: "schemabot[bot]", IsBot: true,
				Body: renderAlreadyFinished(state.Apply.Completed),
			})
			return
		}
		f.post(number, args[2], state.Apply.Cancelled)
	case "cutover":
		f.target.alter(f.applies[args[2]])
		f.post(number, args[2], state.Apply.Completed)
		f.checks[f.heads[branch]] = CheckRun{Status: "completed", Conclusion: "success", Title: "Schema up to date"}
		if f.finalCheckFails {
			f.checks[f.heads[branch]] = CheckRun{Status: "completed", Conclusion: "failure", Title: "Schema drift detected"}
		}
	}
}

// fakeTarget is the test database.
type fakeTarget struct {
	mu      sync.Mutex
	columns map[string]string // table → created_at type
	rows    map[string]int64
}

func newFakeTarget() *fakeTarget {
	return &fakeTarget{columns: map[string]string{}, rows: map[string]int64{}}
}

func (t *fakeTarget) create(table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.columns[table] = columnBefore
}

func (t *fakeTarget) alter(table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.columns[table] = columnAfter
}

func (t *fakeTarget) has(table string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.columns[table]
	return ok
}

func (t *fakeTarget) TableExists(_ context.Context, table string) (bool, error) {
	return t.has(table), nil
}

func (t *fakeTarget) ColumnType(_ context.Context, table, _ string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.columns[table]
	if !ok {
		return "", fmt.Errorf("no table %s", table)
	}
	return c, nil
}

func (t *fakeTarget) RowCount(_ context.Context, table string) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rows[table], nil
}

func (t *fakeTarget) Seed(_ context.Context, table, _ string, rows int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rows[table] = rows
	return nil
}

func (t *fakeTarget) LeftoverCopyTables(context.Context, string) ([]string, error) {
	return nil, nil
}

func (t *fakeTarget) RunTables(context.Context) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for table := range t.columns {
		if runTableName.MatchString(table) {
			out = append(out, table)
		}
	}
	return out, nil
}

func (t *fakeTarget) DropTable(_ context.Context, table string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.columns, table)
	delete(t.rows, table)
	return nil
}

func (t *fakeTarget) Close() error { return nil }

func testRunner(t *testing.T, bot *fakeSchemaBot, target *fakeTarget) *Runner {
	t.Helper()
	r, err := NewRunner(Config{
		Repo:          "octo/schema",
		BaseBranch:    "main",
		TableDir:      "schema/testdb",
		Database:      "testdb",
		Environment:   "development",
		CheckName:     "SchemaBot (development)",
		AppSlug:       "schemabot",
		CommandPrefix: "schemabot",
		SeedTable:     "seed",
		SeedRows:      1000,
		PollInterval:  time.Millisecond,
		WaitTimeout:   time.Second,
		RunTimeout:    time.Hour,
	}, bot, target, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	r.now = func() time.Time { return time.Date(2026, 11, 5, 9, 30, 0, 0, time.UTC) }
	return r
}

// testTable is the table a run started at the test's now names: the time it
// is over, a RunTimeout and a WaitTimeout later.
const testTable = "e2e_20261105103001"

func TestRunDrivesEveryStepAndCleansUp(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)

	require.NoError(t, testRunner(t, bot, target).Run(t.Context()))

	require.Len(t, bot.commands, 7)
	assert.Equal(t, "schemabot apply -e development", bot.commands[0])
	assert.Equal(t, "schemabot apply -e development", bot.commands[1])
	assert.Regexp(t, `^schemabot stop apply-[0-9a-f]{16} -e development$`, bot.commands[2])
	assert.Regexp(t, `^schemabot start apply-[0-9a-f]{16} -e development$`, bot.commands[3])
	assert.Regexp(t, `^schemabot cancel apply-[0-9a-f]{16} -e development$`, bot.commands[4])
	assert.Equal(t, "schemabot apply --defer-cutover -e development", bot.commands[5])
	assert.Regexp(t, `^schemabot cutover apply-[0-9a-f]{16} -e development$`, bot.commands[6])
	assert.NotEqual(t, bot.commands[4], strings.Replace(bot.commands[6], "cutover", "cancel", 1),
		"the cutover acts on the deferred apply, not the cancelled one")
	assert.False(t, bot.prs[1].open, "the run closes its pull request")
	assert.NotContains(t, bot.heads, BranchPrefix+testTable, "the run deletes its branch")
	assert.False(t, target.has(testTable), "the run drops its table")
}

func TestRunIsInconclusiveWhenTheCopyOutrunsTheStop(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.copyOutrunsStop = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.False(t, bot.prs[1].open)
	assert.False(t, target.has(testTable))
}

func TestRunFailsWhenADeferredApplyCutsOverAlone(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.deferredCutsOverAlone = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive, "a deferred apply that finishes alone is a failure, not a race")
	assert.False(t, bot.prs[1].open)
}

// An Applied summary posted while the status comment still shows Stopped
// means the copy outran the start: inconclusive, not a wait that times out.
func TestRunReadsASummaryWhileTheStatusCommentLags(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.startFinishesWithOnlyASummary = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "finished before it reached "+StatusInProgress)
}

func TestSweepCancelsAnEarlierRunBeforeClosingIt(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	bot.heads[BranchPrefix+earlier] = "sha-old"
	bot.post(1, "apply-00000000000000aa", state.Apply.Running)
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))

	assert.Equal(t, []string{"schemabot cancel apply-00000000000000aa -e development"}, bot.commands)
	assert.False(t, bot.prs[1].open)
	assert.NotContains(t, bot.heads, BranchPrefix+earlier)
	assert.False(t, target.has(earlier))
}

// A stopped apply that finished without its status comment showing it, as when
// SchemaBot loses track of a resumed apply, holds nothing: the sweep closes
// its run when SchemaBot refuses the cancel because the apply already
// finished.
func TestSweepClosesARunWhoseCancelIsRefusedAsFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.cancelRefusedAsFinished = true
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	bot.heads[BranchPrefix+earlier] = "sha-old"
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))

	assert.Equal(t, []string{"schemabot cancel apply-00000000000000aa -e development"}, bot.commands)
	assert.False(t, bot.prs[1].open)
	assert.False(t, target.has(earlier))
}

func TestSweepDoesNotCancelAnApplyWhoseSummaryShowsItFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	bot.heads[BranchPrefix+earlier] = "sha-old"
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)
	bot.comments[1] = append(bot.comments[1], Comment{
		ID: bot.id(), Author: "schemabot[bot]", IsBot: true,
		Body: renderSummary("apply-00000000000000aa", state.Apply.Completed),
	})

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))

	assert.Empty(t, bot.commands)
	assert.False(t, bot.prs[1].open)
	assert.False(t, target.has(earlier))
}

func TestSweepKeepsTablesWhileARunCannotBeClosed(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	// An apply whose cancel SchemaBot never acknowledges.
	bot.comments[1] = append(bot.comments[1], Comment{
		ID: 1, Author: "schemabot[bot]", IsBot: true,
		Body: renderStatus("apply-00000000000000bb", state.Apply.Running),
	})
	r := testRunner(t, bot, target)
	r.cfg.WaitTimeout = 20 * time.Millisecond
	r.gh = &silentBot{fakeSchemaBot: bot}

	err := r.Sweep(t.Context())

	require.Error(t, err)
	assert.True(t, bot.prs[1].open, "a run with an unfinished apply stays open")
	assert.True(t, target.has(earlier), "its table stays until the apply finishes")
	assert.NotErrorIs(t, err, ErrInconclusive)
}

// silentBot records commands but never answers them.
type silentBot struct {
	*fakeSchemaBot
}

func (s *silentBot) CreateComment(_ context.Context, number int, body string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.id()
	s.comments[number] = append(s.comments[number], Comment{ID: id, Author: "runner", Body: body})
	s.commands = append(s.commands, body)
	return id, nil
}

// Another runner may be driving a run that is not over, so the sweep leaves
// its pull request, applies, and table alone. The time in the name decides,
// not the sweeping runner's own timeouts.
func TestSweepLeavesARunThatMayStillBeInProgress(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const recent = "e2e_20261105100000" // over 30 minutes after the test's now
	const orphan = "e2e_20261105101500" // a table whose run has not opened its pull request yet
	target.create(recent)
	target.create(orphan)
	bot.prs[1] = &fakePR{branch: BranchPrefix + recent, open: true}
	bot.post(1, "apply-00000000000000aa", state.Apply.Running)

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))

	assert.Empty(t, bot.commands)
	assert.True(t, bot.prs[1].open)
	assert.True(t, target.has(recent))
	assert.True(t, target.has(orphan))
}

// SchemaBot can accept an apply without posting its progress comment, so an
// apply command with no reply may have a running apply behind it. The sweep
// keeps that run's pull request and table.
func TestSweepKeepsARunWhoseApplyCommandHasNoReply(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	bot.comments[1] = append(bot.comments[1], Comment{ID: bot.id(), Author: "runner", Body: "schemabot apply -e development"})

	err := testRunner(t, bot, target).Sweep(t.Context())

	require.ErrorContains(t, err, "no reply from SchemaBot")
	assert.Empty(t, bot.commands)
	assert.True(t, bot.prs[1].open)
	assert.True(t, target.has(earlier))
}

// The branch is deleted before the pull request is closed, so a failed
// delete leaves the pull request open for the next sweep to retry.
func TestSweepRetriesARunWhoseBranchDeleteFailed(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier, open: true}
	bot.heads[BranchPrefix+earlier] = "sha-old"
	bot.deleteBranchFails = true
	r := testRunner(t, bot, target)

	require.Error(t, r.Sweep(t.Context()))
	assert.True(t, bot.prs[1].open, "the next sweep still finds the run")
	assert.True(t, target.has(earlier))

	bot.deleteBranchFails = false
	require.NoError(t, r.Sweep(t.Context()))
	assert.False(t, bot.prs[1].open)
	assert.NotContains(t, bot.heads, BranchPrefix+earlier)
	assert.False(t, target.has(earlier))
}

// A cancel SchemaBot refuses because the copy already finished means the copy
// outran the cancel: inconclusive, not a wait that times out.
func TestRunIsInconclusiveWhenTheCancelIsRefusedAsFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.cancelRefusedAsFinished = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "finished before it reached "+StatusCancelled)
}

// A run that picked the same name as one already in progress creates no
// branch, so its cleanup must leave the other run's pull request, branch, and
// table alone.
func TestRunLeavesAnotherRunsNamesAlone(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	target.create(testTable)
	bot.heads[BranchPrefix+testTable] = "sha-other"
	bot.prs[1] = &fakePR{branch: BranchPrefix + testTable, open: true}
	bot.post(1, "apply-00000000000000aa", state.Apply.Running)

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorContains(t, err, "already exists")
	assert.Empty(t, bot.commands)
	assert.True(t, bot.prs[1].open)
	assert.Contains(t, bot.heads, BranchPrefix+testTable)
	assert.True(t, target.has(testTable))
}

// Closing a pull request leaves its unfinished apply running, so the sweep
// keeps the table of a closed run until its applies are proven finished.
func TestSweepKeepsTheTableOfAClosedRunWithAnUnfinishedApply(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier}
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)

	err := testRunner(t, bot, target).Sweep(t.Context())

	require.ErrorContains(t, err, "apply-00000000000000aa unfinished")
	assert.Empty(t, bot.commands, "the sweep cannot cancel through a closed pull request")
	assert.True(t, target.has(earlier))

	bot.comments[1] = append(bot.comments[1], Comment{
		ID: bot.id(), Author: "schemabot[bot]", IsBot: true,
		Body: renderSummary("apply-00000000000000aa", state.Apply.Cancelled),
	})
	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))
	assert.False(t, target.has(earlier))
}

// A closed run whose cancel SchemaBot refused because the apply had finished
// holds nothing, as closeRun already accepts, so its table can go.
func TestSweepDropsTheTableOfAClosedRunWhoseCancelWasRefusedAsFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier}
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)
	bot.comments[1] = append(bot.comments[1],
		Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000aa -e development"},
		Comment{ID: bot.id(), Author: "schemabot[bot]", IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)},
	)

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))
	assert.False(t, target.has(earlier))
}

// GitHub rate limiting the runner proves nothing about SchemaBot, so the run
// is inconclusive and the pipeline can retry it. Any other GitHub error fails.
func TestRunIsInconclusiveWhenGitHubRateLimitsIt(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.commentErr = secondaryRateLimit(http.MethodPost, "https://api.github.com/repos/octo/schema/issues/1/comments")

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "secondary rate limit")

	target = newFakeTarget()
	bot = newFakeSchemaBot(target)
	bot.commentErr = errors.New("Resource not accessible by integration")
	err = testRunner(t, bot, target).Run(t.Context())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive)
}

// secondaryRateLimit is the error go-github returns when GitHub's secondary
// rate limit refuses a call.
func secondaryRateLimit(method, url string) error {
	req := httptest.NewRequestWithContext(context.Background(), method, url, nil)
	return &github.AbuseRateLimitError{
		Response: &http.Response{Request: req, StatusCode: http.StatusForbidden},
		Message:  "You have exceeded a secondary rate limit",
	}
}

// A rate-limited cleanup after a failed step leaves the run failed: the step
// proved SchemaBot wrong whatever happened to the cleanup after it.
func TestRunStaysFailedWhenOnlyTheCleanupIsRateLimited(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.deferredCutsOverAlone = true
	bot.deleteBranchErr = secondaryRateLimit(http.MethodDelete, "https://api.github.com/repos/octo/schema/git/refs/heads/schemabot-e2e/run")

	err := testRunner(t, bot, target).Run(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "secondary rate limit", "the cleanup error is still reported")
	assert.NotErrorIs(t, err, ErrInconclusive)
}

func TestRunIsInconclusiveWhenAnotherPullRequestHoldsTheLock(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.applyRefusal = templates.RenderApplyBlockedByOtherPR(templates.ApplyLockConflictData{
		Database: "testdb", Environment: "development", RequestedBy: "octocat",
		LockOwner: "hubot", LockRepo: "octo/schema", LockPR: 9, LockCreated: time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC),
	})

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "holds the lock")
}

func TestRunIsInconclusiveWhileOtherChecksAreRunning(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.applyRefusal = templates.RenderApplyBlockedByInProgressChecks("development", []templates.BlockingCheck{{Name: "lint", State: "in_progress"}}, nil)

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "Apply Blocked")
}

// A refusal answers the command, so the run fails at once and names it
// rather than waiting out WaitTimeout.
func TestRunFailsFastWhenSchemaBotRefusesTheApply(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.applyRefusal = templates.RenderApplyBlockedByNonPassingChecks("development", []templates.BlockingCheck{{Name: "lint", State: "failure"}})
	r := testRunner(t, bot, target)
	r.cfg.WaitTimeout = time.Hour

	err := r.Run(t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "SchemaBot refused apply")
	assert.Contains(t, err.Error(), "Apply Blocked")
}

func TestRunIsInconclusiveWhenTheStopIsRefusedAsFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.stopRefusedAsFinished = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "finished before it reached "+StatusStopped)
}

func TestRunWaitsForAPlanStillRunning(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.planInProgressReads = 3

	require.NoError(t, testRunner(t, bot, target).Run(t.Context()))
	assert.Equal(t, "schemabot apply -e development", bot.commands[0], "the first apply waits for the plan")
}

func TestRunFailsFastWhenThePlanFails(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.planFails = true
	r := testRunner(t, bot, target)
	r.cfg.WaitTimeout = time.Hour

	err := r.Run(t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "Plan failed: unsafe change")
	assert.Len(t, bot.commands, 1, "no apply follows a failed plan")
}

func TestRunFailsWhenTheFinalCheckNeverPasses(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.finalCheckFails = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), `completed/failure "Schema drift detected"`)
	assert.Contains(t, err.Error(), "timed out after")
}

// A poll reads only, so it rides out a few rate-limited reads. A wait that
// ends while GitHub is still failing proves nothing either way.
func TestRunRetriesAPollThroughAGitHubRateLimit(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.commentsFailures = 3

	require.NoError(t, testRunner(t, bot, target).Run(t.Context()))

	target = newFakeTarget()
	bot = newFakeSchemaBot(target)
	bot.commentsFailures = 1 << 30
	r := testRunner(t, bot, target)
	r.cfg.WaitTimeout = 50 * time.Millisecond
	err := r.Run(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "while GitHub was failing")
	// The cleanup cannot read the pull request either, so it may leave an
	// apply holding the lock: that outranks the inconclusive wait.
	assert.Contains(t, err.Error(), "clean up run "+runTablePrefix)
	assert.NotErrorIs(t, err, ErrInconclusive)
}

func TestRunIsInconclusiveWhenAnotherRunTookTheName(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.heads[BranchPrefix+testTable] = "sha-other"

	err := testRunner(t, bot, target).Run(t.Context())

	require.ErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "already exists")
}

// A wait the run's own deadline cuts short says so, rather than blaming
// WaitTimeout.
func TestRunNamesTheRunDeadlineWhenItEndsAWait(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.planInProgressReads = 1 << 30
	r := testRunner(t, bot, target)
	r.cfg.RunTimeout = 50 * time.Millisecond
	r.cfg.WaitTimeout = time.Hour

	err := r.Run(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "run deadline passed")
	assert.NotContains(t, err.Error(), "timed out after")
}

// A cleanup that fails may leave an apply holding the lock, so a retry would
// come back inconclusive at that lock. The cleanup's failure decides the run.
func TestRunFailsWhenTheCleanupFailsAfterAnInconclusiveRun(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	bot.copyOutrunsStop = true
	bot.deleteBranchFails = true

	err := testRunner(t, bot, target).Run(t.Context())

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInconclusive)
	assert.Contains(t, err.Error(), "finished before it reached "+StatusInProgress, "the steps' error is still reported")
	assert.Contains(t, err.Error(), "clean up run "+testTable)
}

// A leftover the sweep cannot reap does not stop later runs. A run that
// fails names the sweep's error too.
func TestRunGoesOnWhenTheSweepFails(t *testing.T) {
	const earlier = "e2e_20261104000000"
	stuck := func() (*fakeSchemaBot, *fakeTarget) {
		target := newFakeTarget()
		bot := newFakeSchemaBot(target)
		target.create(earlier)
		bot.prs[1] = &fakePR{branch: BranchPrefix + earlier}
		bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)
		return bot, target
	}

	bot, target := stuck()
	require.NoError(t, testRunner(t, bot, target).Run(t.Context()))
	assert.True(t, target.has(earlier), "the stuck leftover stays for someone to check")

	bot, target = stuck()
	bot.deferredCutsOverAlone = true
	err := testRunner(t, bot, target).Run(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the sweep before the run also failed")
	assert.Contains(t, err.Error(), "apply-00000000000000aa unfinished")
}

// Only a pull request's apply creates a run's table, so a table with no pull
// request behind it means the lookup missed it, not that nothing ran.
func TestSweepKeepsATableWhoseRunHasNoPullRequest(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)

	err := testRunner(t, bot, target).Sweep(t.Context())

	require.ErrorContains(t, err, "found no pull request")
	assert.True(t, target.has(earlier))
}

// A sweeper whose clock runs ahead must not reach a run in its last seconds.
func TestSweepLeavesARunWithinTheGrace(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const ending = "e2e_20261105092930" // 30 seconds before the test's now
	target.create(ending)
	bot.prs[1] = &fakePR{branch: BranchPrefix + ending, open: true}

	require.NoError(t, testRunner(t, bot, target).Sweep(t.Context()))

	assert.True(t, bot.prs[1].open)
	assert.True(t, target.has(ending))
}

// Only SchemaBot's first reply to a cancel answers it, so a refusal of a
// later command for another apply cannot prove this apply finished.
func TestSweepReadsOnlyTheFirstReplyToACancel(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier}
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)
	bot.comments[1] = append(bot.comments[1],
		Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000aa -e development"},
		Comment{ID: bot.id(), Author: "schemabot[bot]", IsBot: true, Body: renderStartRefused("the cancel could not reach the engine")},
		Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000bb -e development"},
		Comment{ID: bot.id(), Author: "schemabot[bot]", IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)},
	)

	err := testRunner(t, bot, target).Sweep(t.Context())

	require.ErrorContains(t, err, "apply-00000000000000aa unfinished")
	assert.True(t, target.has(earlier))
}

// A run cancelled part way, as a pipeline cancels a superseded gate, still
// cleans up after itself.
func TestRunCleansUpAfterItIsCancelled(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bot.onCommand = func(body string) {
		if strings.Contains(body, "--defer-cutover") {
			cancel()
		}
	}

	err := testRunner(t, bot, target).Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "run cancelled")
	assert.Regexp(t, `^schemabot cancel apply-[0-9a-f]{16} -e development$`, bot.commands[len(bot.commands)-1],
		"the cleanup cancels the deferred apply")
	assert.False(t, bot.prs[1].open)
	assert.NotContains(t, bot.heads, BranchPrefix+testTable)
	assert.False(t, target.has(testTable))
}

// Once someone posts another command, SchemaBot's next reply may answer that
// one, so cleanup must not read it as proof that its own cancel's apply
// finished.
func TestRunKeepsItsTableWhenOnlyAnotherCancelIsRefusedAsFinished(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bot.onCommand = func(body string) {
		switch {
		case strings.Contains(body, "--defer-cutover"):
			// Stop the run with the deferred apply waiting, and leave the
			// cleanup's cancel of it unanswered.
			cancel()
			bot.cancelUnanswered = true
		case bot.cancelUnanswered && strings.Contains(body, " cancel "):
			bot.comments[1] = append(bot.comments[1],
				Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000bb -e development"},
				Comment{ID: bot.id(), Author: "schemabot[bot]", IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)},
			)
		}
	}
	err := testRunner(t, bot, target).Run(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "clean up run "+testTable)
	assert.Contains(t, bot.heads, BranchPrefix+testTable, "the branch, and with it the pull request, stays for the next sweep")
	assert.True(t, bot.prs[1].open)
	assert.True(t, target.has(testTable))
}

// The sweep reads a cancel's answer the same way.
func TestSweepIgnoresAReplyToALaterCancel(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const earlier = "e2e_20261104000000"
	target.create(earlier)
	bot.prs[1] = &fakePR{branch: BranchPrefix + earlier}
	bot.post(1, "apply-00000000000000aa", state.Apply.Stopped)
	bot.comments[1] = append(bot.comments[1],
		Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000aa -e development"},
		Comment{ID: bot.id(), Author: "octocat", Body: "schemabot cancel apply-00000000000000bb -e development"},
		Comment{ID: bot.id(), Author: "schemabot[bot]", IsBot: true, Body: renderAlreadyFinished(state.Apply.Completed)},
	)

	err := testRunner(t, bot, target).Sweep(t.Context())

	require.ErrorContains(t, err, "apply-00000000000000aa unfinished")
	assert.True(t, target.has(earlier))
}
