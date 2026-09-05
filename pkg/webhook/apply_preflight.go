package webhook

import (
	"context"
	"fmt"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// applyGate identifies a gate in the apply ladder. The order of the constants
// is the order the ladder enforces them in, which is what lets a blocking gate
// ask for the status of everything behind it.
type applyGate int

const (
	gateReviewApproval applyGate = iota
	gatePRChecks
	gatePriorEnvironments
	gateDatabaseLock
)

// gateName is the operator-facing label for a gate, matching how the gate's
// own rejection comment names it.
func (g applyGate) gateName() string {
	switch g {
	case gateReviewApproval:
		return "Review approval"
	case gatePRChecks:
		return "PR checks"
	case gatePriorEnvironments:
		return "Prior environments"
	default:
		return "Database lock"
	}
}

// applyPreflight reads the apply gates behind the one that blocked, so a
// rejection comment can name everything still in the way instead of only the
// first thing.
//
// It is advisory and gates nothing. Every probe is read-only — no lock is
// taken, no comment is posted, no stored state is written — and every probe
// runs single-shot, without the retry waits the enforcing gates use, because
// an operator waiting on a rejection comment is not served by a probe that
// re-reads a check for several seconds. A probe that cannot reach a verdict
// reports unknown, so uncertainty is never rendered as ready.
//
// Nothing here decides whether the apply proceeds: the gate that blocked has
// already decided that, and this changes neither the ladder's order nor any
// gate's verdict.
type applyPreflight struct {
	handler     *Handler
	client      *ghclient.InstallationClient
	repo        string
	pr          int
	schema      *ghclient.SchemaRequestResult
	environment string
}

// newApplyPreflight builds the prober for one apply command. It returns nil
// when the command has no discovered schema to probe against, which callers
// treat as "no checklist" rather than as a failure.
func (h *Handler) newApplyPreflight(client *ghclient.InstallationClient, repo string, pr int, schema *ghclient.SchemaRequestResult, environment string) *applyPreflight {
	if h == nil || client == nil || schema == nil {
		return nil
	}
	return &applyPreflight{handler: h, client: client, repo: repo, pr: pr, schema: schema, environment: environment}
}

// rowsAfter reports the status of every gate the ladder enforces after
// blockedAt. A nil receiver yields no rows, so a caller with no prober renders
// the rejection comment exactly as it would without the checklist.
func (p *applyPreflight) rowsAfter(ctx context.Context, blockedAt applyGate) []templates.PreflightRow {
	if p == nil {
		return nil
	}
	var rows []templates.PreflightRow
	for gate := blockedAt + 1; gate <= gateDatabaseLock; gate++ {
		rows = append(rows, p.probe(ctx, gate))
	}
	return rows
}

func (p *applyPreflight) probe(ctx context.Context, gate applyGate) templates.PreflightRow {
	var status templates.PreflightStatus
	var detail string
	switch gate {
	case gateReviewApproval:
		status, detail = p.probeReviewApproval(ctx)
	case gatePRChecks:
		status, detail = p.probePRChecks(ctx)
	case gatePriorEnvironments:
		status, detail = p.probePriorEnvironments(ctx)
	default:
		status, detail = p.probeDatabaseLock(ctx)
	}
	return templates.PreflightRow{Gate: gate.gateName(), Status: status, Detail: detail}
}

// unknownGate logs why a probe could not reach a verdict and renders the
// reason the operator can act on. The underlying error stays in the log: it
// can carry hosts, headers, and driver internals that must never render in PR
// markdown.
func (p *applyPreflight) unknownGate(gate applyGate, detail string, err error) (templates.PreflightStatus, string) {
	p.handler.logger.Warn("apply preflight could not read a gate; reporting it as unknown on the pull request",
		"repo", p.repo, "pr", p.pr, "gate", gate.gateName(),
		"database", p.schema.Database, "database_type", p.schema.Type,
		"environment", p.environment, "error", err)
	return templates.PreflightUnknown, detail
}

func (p *applyPreflight) probeReviewApproval(ctx context.Context) (templates.PreflightStatus, string) {
	result, err := p.handler.checkReviewGate(ctx, p.client, p.repo, p.pr, p.schema.Database, p.schema.SchemaPath)
	if err != nil {
		return p.unknownGate(gateReviewApproval, "Approval state could not be read", err)
	}
	if result == nil || result.Approved {
		return templates.PreflightReady, ""
	}
	return templates.PreflightBlocked, "Needs an approval from an authorized reviewer"
}

func (p *applyPreflight) probePRChecks(ctx context.Context) (templates.PreflightStatus, string) {
	config := p.handler.service.Config()
	if !config.ShouldRequirePassingChecks() {
		return templates.PreflightReady, ""
	}
	statuses, err := p.client.GetPRCheckStatuses(ctx, p.repo, p.schema.HeadSHA, config.RequiredChecks)
	if err != nil {
		return p.unknownGate(gatePRChecks, "Check statuses could not be read", err)
	}
	if notPassing := filterNonPassingNonSchemaBotChecks(statuses, config); len(notPassing) > 0 {
		return templates.PreflightBlocked, fmt.Sprintf("%s not passing", pluralizeChecks(len(notPassing)))
	}
	pending := len(filterInProgressNonSchemaBotChecks(statuses, config)) + len(missingRequiredChecks(statuses, config))
	if pending > 0 {
		return templates.PreflightBlocked, fmt.Sprintf("%s still to report", pluralizeChecks(pending))
	}
	return templates.PreflightReady, ""
}

// pluralizeChecks renders a check count with its noun.
func pluralizeChecks(n int) string {
	if n == 1 {
		return "1 check"
	}
	return fmt.Sprintf("%d checks", n)
}

// probePriorEnvironments reads each prior environment in the database's
// promotion order the same way the gate does — stored check state for an
// environment this deployment owns, the trusted aggregate Check Run for one a
// sibling deployment owns — but takes each reading once. The gate's retries
// exist so ordering jitter cannot block an apply; a probe that finds a check
// mid-flight is reporting something true and useful.
func (p *applyPreflight) probePriorEnvironments(ctx context.Context) (templates.PreflightStatus, string) {
	config := p.handler.service.Config()
	database := p.schema.Database
	if scopedTargetMissingFromPromotionOrder(config, database, p.environment) {
		return templates.PreflightBlocked, "This environment is absent from the configured promotion order"
	}

	environments := promotionGateEnvironments(config, database, p.environment, p.schema.Environments)
	for _, priorEnv := range environments {
		if priorEnv == p.environment {
			return templates.PreflightReady, ""
		}
		status, detail := p.probePriorEnvironment(ctx, priorEnv)
		if status != templates.PreflightReady {
			return status, detail
		}
	}
	return templates.PreflightReady, ""
}

func (p *applyPreflight) probePriorEnvironment(ctx context.Context, priorEnv string) (templates.PreflightStatus, string) {
	if p.handler.service.Config().IsEnvironmentAllowed(priorEnv) {
		check, err := p.handler.service.Storage().Checks().Get(ctx, p.repo, p.pr, priorEnv, p.schema.Type, p.schema.Database)
		if err != nil {
			return p.unknownGate(gatePriorEnvironments, "Prior environment state could not be read", err)
		}
		if check == nil {
			return templates.PreflightBlocked, "Not applied to " + priorEnv + " yet"
		}
		if check.Conclusion == checkConclusionSuccess {
			return templates.PreflightReady, ""
		}
		return templates.PreflightBlocked, priorEnv + " is not passing"
	}

	checkName := aggregateCheckNameForEnv(p.handler.promotionCheckNameForRepo(p.repo), priorEnv)
	checkResult, _, err := p.client.FindCheckRunByName(ctx, p.repo, p.schema.HeadSHA, checkName)
	if err != nil {
		return p.unknownGate(gatePriorEnvironments, "Prior environment state could not be read", err)
	}
	if checkResult == nil {
		return templates.PreflightBlocked, "Not applied to " + priorEnv + " yet"
	}
	if checkResult.Status == checkStatusCompleted && checkResult.Conclusion == checkConclusionSuccess {
		return templates.PreflightReady, ""
	}
	return templates.PreflightBlocked, priorEnv + " is not passing"
}

// probeDatabaseLock reports the two conditions the lock gate blocks on: the
// database locked by another pull request, and a schema change from this pull
// request still running against it.
func (p *applyPreflight) probeDatabaseLock(ctx context.Context) (templates.PreflightStatus, string) {
	lock, err := p.handler.service.Storage().Locks().Get(ctx, p.schema.Database, p.schema.Type)
	if err != nil {
		return p.unknownGate(gateDatabaseLock, "Lock state could not be read", err)
	}
	if lock == nil {
		return templates.PreflightReady, ""
	}
	if lock.Owner != fmt.Sprintf("%s#%d", p.repo, p.pr) {
		return templates.PreflightBlocked, fmt.Sprintf("Held by %s", lock.Owner)
	}

	applies, err := p.handler.service.Storage().Applies().GetByPR(ctx, p.repo, p.pr)
	if err != nil {
		return p.unknownGate(gateDatabaseLock, "Lock state could not be read", err)
	}
	for _, a := range applies {
		if a.Database == p.schema.Database && !state.IsTerminalApplyState(a.State) {
			return templates.PreflightBlocked, "A schema change from this pull request is still " + a.State
		}
	}
	return templates.PreflightReady, ""
}
