package livee2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

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
	// commands records every PR command, in order.
	commands []string
}

type fakePR struct {
	branch string
	open   bool
}

func newFakeSchemaBot(target *fakeTarget) *fakeSchemaBot {
	return &fakeSchemaBot{
		target:   target,
		nextID:   100,
		heads:    map[string]string{"main": "sha-main"},
		files:    map[string]string{},
		prs:      map[int]*fakePR{},
		comments: map[int][]Comment{},
		checks:   map[string]CheckRun{},
		applies:  map[string]string{},
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
	f.heads[branch] = sha
	return nil
}

func (f *fakeSchemaBot) DeleteBranch(_ context.Context, branch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeSchemaBot) Comments(_ context.Context, number int) ([]Comment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Comment(nil), f.comments[number]...), nil
}

func (f *fakeSchemaBot) CheckRun(_ context.Context, sha, _, _ string) (*CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.checks[sha]
	if !ok {
		return nil, nil
	}
	return &run, nil
}

func (f *fakeSchemaBot) CreateComment(_ context.Context, number int, body string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.id()
	f.comments[number] = append(f.comments[number], Comment{ID: id, Author: "runner", Body: body})
	f.commands = append(f.commands, body)
	f.respond(number, strings.Fields(body))
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

const testTable = "e2e_20261105093000"

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

// Another runner may be driving a recent run, so the sweep leaves its pull
// request, applies, and table alone.
func TestSweepLeavesARunThatMayStillBeInProgress(t *testing.T) {
	target := newFakeTarget()
	bot := newFakeSchemaBot(target)
	const recent = "e2e_20261105090000" // 30 minutes before the test's now
	const orphan = "e2e_20261105091500" // a table whose run has not opened its pull request yet
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
