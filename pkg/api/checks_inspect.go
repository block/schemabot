// checks_inspect.go answers "why does this pull request's SchemaBot check read
// the way it does" from the two records that decide it: the stored check state
// SchemaBot keeps, and the Check Run GitHub shows.
//
// The two disagree whenever a row was recorded for a commit the pull request
// has since moved past, which is the state an operator cannot tell apart from a
// hung apply without reading server logs. Putting both beside each other, with
// the reading of the row that the aggregate itself performs, is what makes that
// a lookup instead of an investigation.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

type ChecksInspectRequest = apitypes.ChecksInspectRequest
type ChecksInspectResponse = apitypes.ChecksInspectResponse
type InspectedCheck = apitypes.InspectedCheck
type InspectedCheckRun = apitypes.InspectedCheckRun

// handleChecksInspect answers an inspection over GET, because an inspection
// only reads: it stages nothing, writes no stored check state, and creates no
// Check Run. That keeps it on the read tier, where anyone who can already see
// a pull request's status can ask why its check reads the way it does, and it
// keeps the question repeatable — the same address, asked twice, is two reads.
func (s *Service) handleChecksInspect(w http.ResponseWriter, r *http.Request) {
	req, err := checksInspectRequestFromQuery(r.URL.Query())
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	ctx, cancel := s.extendWebhookOpsDeadline(w, r)
	defer cancel()
	response, err := executeChecksInspect(ctx, s.config, s.storage, req, s.logger)
	if err != nil {
		s.writeWebhookOpsError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, response)
}

// checksInspectRequestFromQuery reads the inspection target out of the query
// string. A pull request may be named the way the caller already holds it —
// its URL, or "owner/name#number" — as well as by a number beside a repo, so
// that reaching for the inspection never means taking an address apart first.
//
// A reference that carries a repository and an explicit repo parameter that
// disagree are refused rather than resolved by precedence: one of the two is a
// mistake, and inspecting whichever one the code happens to prefer would answer
// a question about a pull request the caller did not ask about.
func checksInspectRequestFromQuery(query url.Values) (ChecksInspectRequest, error) {
	req := ChecksInspectRequest{
		Repo:        canonicalRepo(query.Get("repo")),
		Environment: canonicalEnvironment(query.Get("environment")),
	}
	reference := strings.TrimSpace(query.Get("pull_request"))
	if reference == "" {
		return req, webhookOpsRequestErrorf("pull_request is required: its number, its URL, or owner/name#number")
	}
	if pr, err := strconv.Atoi(reference); err == nil {
		req.PullRequest = pr
		return req, nil
	}

	repo, pr, err := caller.ParsePullRequestReference(reference)
	if err != nil {
		return req, webhookOpsRequestErrorf("%s", err)
	}
	if pr == 0 {
		return req, webhookOpsRequestErrorf("%q names a repository but no pull request; give the number too", reference)
	}
	// Compared folded, because every consumer below folds: the storage lookup,
	// the checks-enabled test, and the app resolution all read a repository
	// through storage.CanonicalKey. Comparing the raw spellings would refuse
	// "Acme/Store" beside a lowercase URL as a contradiction while either
	// spelling on its own is accepted.
	if req.Repo != "" && storage.CanonicalKey(req.Repo) != storage.CanonicalKey(repo) {
		return req, webhookOpsRequestErrorf("repo is %q but pull_request names %q; pass one of the two", req.Repo, repo)
	}
	req.Repo = repo
	req.PullRequest = pr
	return req, nil
}

// checksInspectClient is the GitHub surface an inspection needs: the commit the
// pull request is gated on, and the Check Run sitting on it.
type checksInspectClient interface {
	FetchPullRequestNoCache(ctx context.Context, repo string, pr int) (*ghclient.PullRequestInfo, error)
	FindCheckRunByName(ctx context.Context, repo, headSHA, checkName string) (*ghclient.CheckRunResult, []string, error)
}

func executeChecksInspect(ctx context.Context, cfg *ServerConfig, store storage.Storage, req ChecksInspectRequest, logger *slog.Logger) (*ChecksInspectResponse, error) {
	if cfg == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("storage is not configured")
	}
	if err := requireRepoFullName(req.Repo); err != nil {
		return nil, err
	}
	if req.PullRequest <= 0 {
		return nil, webhookOpsRequestErrorf("pull_request must be positive")
	}
	// A mistyped environment would silently narrow the response to nothing and
	// read as "this pull request has no check state", which is the opposite of
	// what an operator is here to find out. On an instance that scopes nothing
	// by environment the filter would also drop the one global aggregate.
	if err := requireNarrowableEnvironment(cfg, req.Environment); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	client, _, err := resolveRepoInstallationClient(ctx, cfg, req.Repo, logger)
	if err != nil {
		return nil, err
	}
	return inspectChecks(ctx, cfg, store, client, req, logger)
}

func inspectChecks(ctx context.Context, cfg *ServerConfig, store storage.Storage, client checksInspectClient, req ChecksInspectRequest, logger *slog.Logger) (*ChecksInspectResponse, error) {
	// The head is read uncached and once: it is what every row below is read
	// against, and a second read could disagree with the first and make two
	// rows in one response answer different questions.
	prInfo, err := client.FetchPullRequestNoCache(ctx, req.Repo, req.PullRequest)
	if err != nil {
		return nil, fmt.Errorf("read head commit for %s#%d: %w", req.Repo, req.PullRequest, err)
	}

	stored, err := store.Checks().GetByPR(ctx, req.Repo, req.PullRequest)
	if err != nil {
		return nil, fmt.Errorf("read stored check state for %s#%d: %w", req.Repo, req.PullRequest, err)
	}

	response := &ChecksInspectResponse{
		Repo:          req.Repo,
		PullRequest:   req.PullRequest,
		HeadSHA:       prInfo.HeadSHA,
		PRState:       prInfo.State,
		Environment:   req.Environment,
		ChecksEnabled: cfg.AreChecksEnabled(req.Repo),
		Rows:          make([]InspectedCheck, 0, len(stored)),
	}

	for _, check := range stored {
		if req.Environment != "" && check.Environment != req.Environment {
			continue
		}
		response.Rows = append(response.Rows, inspectedCheck(ctx, store, check, prInfo.HeadSHA, logger))
	}
	// Ordered on every field that distinguishes two rows, so the answer does
	// not depend on the order storage returned them in: a database name is
	// unique only within its type, and two rows differing in type alone would
	// otherwise swap between reads of unchanged state.
	sort.Slice(response.Rows, func(i, j int) bool {
		a, b := response.Rows[i], response.Rows[j]
		if a.Environment != b.Environment {
			return a.Environment < b.Environment
		}
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		return a.DatabaseType < b.DatabaseType
	})

	runs := checkRunsOnHead(ctx, cfg, client, req.Repo, prInfo.HeadSHA, req.Environment, logger)
	response.CheckRunsOnHead = runs.found
	response.MissingCheckRunNames = runs.missing
	response.UnreadableCheckRunNames = runs.unreadable
	response.UntrustedConflictNames = runs.untrustedConflicts
	return response, nil
}

// inspectedCheck reads one stored row and names the apply holding it.
//
// The apply is resolved before the row is diagnosed, not after: whether the
// gate is waiting or owed turns on the owner's state, so a disposition derived
// without it would report a stopped apply as one in flight.
func inspectedCheck(ctx context.Context, store storage.Storage, check *storage.Check, headSHA string, logger *slog.Logger) InspectedCheck {
	apply := applyHoldingCheck(ctx, store, check, logger)
	disposition := checkstate.Diagnose(check, headSHA, apply)
	row := InspectedCheck{
		Environment:    check.Environment,
		DatabaseType:   check.DatabaseType,
		Database:       check.DatabaseName,
		Aggregate:      checkstate.IsAggregate(check),
		RecordedSHA:    check.HeadSHA,
		CoversHead:     checkstate.CoversHead(check, headSHA),
		Status:         check.Status,
		Conclusion:     check.Conclusion,
		BlockingReason: check.BlockingReason,
		CheckRunID:     check.CheckRunID,
		Reason:         disposition.Reason,
		Summary:        disposition.Summary,
		Remedy:         disposition.Remedy,
		Blocking:       disposition.Blocking,
		SelfConverging: disposition.SelfConverging,
	}
	if !check.UpdatedAt.IsZero() {
		row.UpdatedAt = check.UpdatedAt.UTC().Format(time.RFC3339)
	}
	if apply != nil {
		row.ApplyIdentifier = apply.ApplyIdentifier
		row.ApplyState = apply.State
	}
	return row
}

// applyHoldingCheck resolves the apply a stored row names, or nil when the row
// names none.
//
// An unreadable or missing apply also returns nil, and the diagnosis reports
// that row as an unknown owner rather than guessing at one. Dropping the
// inspection entirely would be worse than reporting a row without a name: the
// other rows in the same response are what the operator came for.
func applyHoldingCheck(ctx context.Context, store storage.Storage, check *storage.Check, logger *slog.Logger) *storage.Apply {
	if check.ApplyID == 0 {
		return nil
	}
	apply, err := store.Applies().Get(ctx, check.ApplyID)
	if err != nil {
		logger.Warn("check inspection reports a row without the apply holding it: reading that apply failed",
			"repo", check.Repository, "pr", check.PullRequest, "environment", check.Environment,
			"database_type", check.DatabaseType, "database", check.DatabaseName, "error", err)
		return nil
	}
	if apply == nil {
		logger.Warn("check inspection reports a row whose apply no longer exists in storage",
			"repo", check.Repository, "pr", check.PullRequest, "environment", check.Environment,
			"database_type", check.DatabaseType, "database", check.DatabaseName)
		return nil
	}
	return apply
}

// checkRunsOnHead reports every Check Run this deployment publishes that was
// found on the head, and the expected names that were not.
//
// Each name is a required check in its own right, so they are reported
// separately rather than reduced to the first one found: on a deployment
// publishing one check per environment, a present staging run says nothing
// about an absent production run, and treating it as "the" run would report
// the gate as clear while branch protection waits on the name that is gone.
//
// A Check Run that cannot be read is not an error for the inspection: the
// stored rows are the part that explains the state, and they are already in
// hand. Such a name is reported as unreadable rather than as missing, and the
// two are kept apart all the way to the reader: a missing run is recreated, an
// unreadable one is read again, and an empty result that hides a failed read
// would let a GitHub outage render as a clear gate.
//
// A deployment that publishes no checks for the repository still has its runs
// reported, since a stale one left on the head is worth seeing, but no name is
// called missing there. The absence is the configuration.
//
// A name an untrusted app also has a run under is reported as conflicted
// whether or not the trusted run exists, because branch protection picks the
// run it reads and no backfill touches the untrusted one. When the trusted run
// is absent the name is missing and conflicted at once: a backfill is still the
// action, but reporting the absence alone would leave the operator recreating a
// run and wondering why the gate did not move. When the trusted run is present
// the conflict is the whole finding, and dropping it would let this inspection
// state that nothing holds the gate while protection reads a failing duplicate.
// On a deployment that publishes no checks for the repository no name is
// conflicted either: a conflict claims another app's run competes with
// SchemaBot's, and there is none of SchemaBot's here for it to compete with.
func checkRunsOnHead(ctx context.Context, cfg *ServerConfig, client checksInspectClient, repo, headSHA, environment string, logger *slog.Logger) headCheckRuns {
	names := webhookMissingCheckNames(cfg, repo, environment, "")
	if len(names) == 0 || headSHA == "" {
		return headCheckRuns{}
	}
	var result headCheckRuns
	checksEnabled := cfg.AreChecksEnabled(repo)
	for _, name := range names {
		run, untrustedApps, err := client.FindCheckRunByName(ctx, repo, headSHA, name)
		if err != nil {
			logger.Warn("check inspection cannot say whether this Check Run is on the head: reading the run failed",
				"repo", repo, "head_sha", headSHA, "check_name", name, "error", err)
			result.unreadable = append(result.unreadable, name)
			continue
		}
		if run == nil {
			if !checksEnabled {
				logger.Debug("no Check Run under this name on the head commit, and this deployment publishes none for the repository",
					"repo", repo, "head_sha", headSHA, "check_name", name)
				continue
			}
			logger.Debug("no SchemaBot Check Run under this name on the head commit",
				"repo", repo, "head_sha", headSHA, "check_name", name)
			result.missing = append(result.missing, name)
			if len(untrustedApps) > 0 {
				logger.Warn("no SchemaBot Check Run under this name on the head commit, but an untrusted app has one",
					"repo", repo, "head_sha", headSHA, "check_name", name, "untrusted_apps", untrustedApps)
				result.untrustedConflicts = append(result.untrustedConflicts, name)
			}
			continue
		}
		// A conflict is a claim that another app's run competes with
		// SchemaBot's. With publishing turned off there is no SchemaBot run for
		// it to compete with, so what sits under the name is simply another
		// app's, and calling it contested would contradict the line that says
		// this deployment maintains none here.
		if len(untrustedApps) > 0 {
			if checksEnabled {
				logger.Warn("a SchemaBot Check Run is on the head commit under this name, and so is an untrusted app's",
					"repo", repo, "head_sha", headSHA, "check_name", name, "untrusted_apps", untrustedApps)
				result.untrustedConflicts = append(result.untrustedConflicts, name)
			} else {
				logger.Debug("an untrusted app has a Check Run under this name, and this deployment publishes none for the repository, so there is nothing for it to contest",
					"repo", repo, "head_sha", headSHA, "check_name", name, "untrusted_apps", untrustedApps)
			}
		}
		inspected := InspectedCheckRun{
			Name:       run.Name,
			CheckRunID: run.ID,
			Status:     run.Status,
			Conclusion: run.Conclusion,
		}
		inspected.StartedAt = formatCheckRunStartedAt(run.StartedAt)
		result.found = append(result.found, inspected)
	}
	return result
}

// headCheckRuns is what reading the head's Check Runs found, by disposition.
// The four are kept apart rather than folded into a present/absent pair because
// each one implies a different action, and the whole point of the inspection is
// to name that action.
type headCheckRuns struct {
	found              []InspectedCheckRun
	missing            []string
	unreadable         []string
	untrustedConflicts []string
}
