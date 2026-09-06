package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// webhookPayload represents the relevant fields from a GitHub webhook payload.
type webhookPayload struct {
	Action string `json:"action"`
	Issue  *struct {
		Number      int `json:"number"`
		PullRequest *struct {
			URL string `json:"url"`
		} `json:"pull_request"`
	} `json:"issue"`
	Comment *struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
		User *struct {
			Login string `json:"login"`
			Type  string `json:"type"`
		} `json:"user"`
	} `json:"comment"`
	Repository *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation *struct {
		ID int64 `json:"id"`
	} `json:"installation"`
}

type issueCommentGateBlockReason string

const (
	issueCommentGatePass                issueCommentGateBlockReason = ""
	issueCommentGateInvalidTenant       issueCommentGateBlockReason = "invalid tenant flag"
	issueCommentGateTenantNotOwned      issueCommentGateBlockReason = "tenant handled by another deployment"
	issueCommentGateTenantRequired      issueCommentGateBlockReason = "tenant target required"
	issueCommentGateInvalidEnvironment  issueCommentGateBlockReason = "invalid environment value"
	issueCommentGateMissingEnvironment  issueCommentGateBlockReason = "missing environment flag"
	issueCommentGateEnvironmentNotOwned issueCommentGateBlockReason = "environment handled by another instance"
	issueCommentGateMissingApplyID      issueCommentGateBlockReason = "missing apply ID"
	issueCommentGateCommandNotFound     issueCommentGateBlockReason = "command not found"
	issueCommentGateAutoConfirm         issueCommentGateBlockReason = "auto-confirm flag unsupported for command"
	issueCommentGateDeferCutover        issueCommentGateBlockReason = "defer-cutover flag unsupported for command"
	issueCommentGateDatabase            issueCommentGateBlockReason = "database flag unsupported for command"
)

// issueCommentGateBlock evaluates the routing and usage gates shared by the
// request path and durable driver in request-path order.
func (h *Handler) issueCommentGateBlock(repo string, result CommandResult, parser *CommandParser, commentBody string) issueCommentGateBlockReason {
	if result.TenantError {
		return issueCommentGateInvalidTenant
	}
	if h.service != nil {
		cfg := h.service.Config()
		if result.Tenant != "" && !cfg.ShouldRespondToTenant(result.Tenant) {
			return issueCommentGateTenantNotOwned
		}
		if result.Tenant == "" && cfg.Tenant != "" && commandRequiresTenantTarget(result) {
			fansOut := h.fansOutUnscopedCommand(repo) && unscopedCommandFansOut(result)
			if !fansOut {
				return issueCommentGateTenantRequired
			}
		}
	}
	if result.EnvironmentError {
		return issueCommentGateInvalidEnvironment
	}
	if result.MissingEnv {
		return issueCommentGateMissingEnvironment
	}
	if result.Found && result.Environment != "" && h.service != nil && !h.service.Config().IsEnvironmentAllowed(result.Environment) {
		return issueCommentGateEnvironmentNotOwned
	}
	if result.Found && result.Action == action.Rollback && result.ApplyID == "" {
		return issueCommentGateMissingApplyID
	}
	if !result.Found {
		return issueCommentGateCommandNotFound
	}
	if parser.HasAutoConfirmFlag(commentBody) {
		return issueCommentGateAutoConfirm
	}
	if result.Action == action.Rollback && parser.HasDeferCutoverFlag(commentBody) {
		return issueCommentGateDeferCutover
	}
	if !commandSupportsDatabaseFlag(result.Action) && parser.HasDatabaseFlag(commentBody) {
		return issueCommentGateDatabase
	}
	return issueCommentGatePass
}

// handleIssueComment processes GitHub issue comment webhooks.
func (h *Handler) handleIssueComment(ctx context.Context, metricApp string, w http.ResponseWriter, body []byte, deliveryID string) {
	var payload webhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid webhook payload")
		return
	}

	// Only process "created" comment events on PRs
	if payload.Action != "created" ||
		payload.Issue == nil ||
		payload.Issue.PullRequest == nil ||
		payload.Comment == nil ||
		payload.Repository == nil {
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "event ignored (not a PR comment creation)",
		})
		return
	}
	payload.Repository.FullName = storage.CanonicalKey(payload.Repository.FullName)

	var payloadInstallationID int64
	if payload.Installation != nil {
		payloadInstallationID = payload.Installation.ID
	}
	// Repo-level webhook deliveries carry no installation id in the payload; the
	// dispatcher resolves it and stashes it on the context.
	installationID := h.effectiveInstallationID(ctx, payloadInstallationID)

	repo := payload.Repository.FullName
	pr := payload.Issue.Number
	requestedBy := ""
	if payload.Comment.User != nil {
		requestedBy = payload.Comment.User.Login
	}

	// Ignore comments from bots to prevent infinite loops. The one exception is
	// a trusted sibling SchemaBot deployment's comment on a repo this
	// deployment leads: it is consumed as an aggregate re-fold nudge — never
	// parsed as a command — because participants comment at exactly the moments
	// their Check Runs change, and GitHub delivers check_run events only to the
	// App that created the check.
	if payload.Comment.User != nil && payload.Comment.User.Type == "Bot" {
		if h.participantCommentNudge(ctx, repo, pr, installationID, deliveryID, requestedBy) {
			h.writeJSON(w, http.StatusOK, map[string]string{
				"message": "participant comment triggered aggregate re-fold",
			})
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "event ignored (comment from bot)",
		})
		return
	}

	// Parse command
	parser := NewCommandParser()
	result := parser.ParseCommand(payload.Comment.Body)
	result.CommentID = payload.Comment.ID
	result.DeliveryID = deliveryID

	if !result.IsMention {
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "no SchemaBot command found",
		})
		return
	}

	// Reject commands from repositories not in the configured allowlist
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("webhook from unregistered repository",
			"event", "issue_comment",
			"action", payload.Action,
			"repo", repo,
			"pr", pr,
			"installation_id", installationID,
			"requested_by", requestedBy)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, metricApp, "issue_comment", payload.Action, repo)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "repository not registered",
		})
		return
	}

	// Every command response below — acknowledgment reactions, usage-error
	// comments, and dispatched work alike — needs the installation ID, so a
	// delivery without one is rejected before any command handling.
	if installationID == 0 {
		h.writeError(w, http.StatusBadRequest, "missing installation ID in webhook payload")
		return
	}

	// The ladder evaluates in request-path order, so the first-tripped reason
	// cached here is the one each branch below would have observed.
	gateReason := h.issueCommentGateBlock(repo, result, parser, payload.Comment.Body)

	if gateReason == issueCommentGateInvalidTenant {
		h.logger.Info("ignoring command with invalid tenant flag",
			"repo", repo, "pr", pr, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "invalid tenant flag",
		})
		return
	}

	// When a command names a tenant, only the matching isolated deployment should
	// react or post comments. This mirrors allowed_environments routing for -e.
	if gateReason == issueCommentGateTenantNotOwned {
		h.logger.Info("ignoring command for non-owned tenant",
			"repo", repo, "pr", pr, "tenant", result.Tenant, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "tenant handled by another instance",
		})
		return
	}
	if gateReason == issueCommentGateTenantRequired {
		h.logger.Info("ignoring work command without tenant target",
			"repo", repo, "pr", pr, "tenant", h.service.Config().Tenant, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "tenant target required",
		})
		return
	}
	if result.Tenant == "" && commandRequiresTenantTarget(result) && h.service != nil && h.service.Config().Tenant != "" {
		h.logger.Info("aggregate participant fanning out unscoped work command; acting on work it owns",
			"repo", repo, "pr", pr, "tenant", h.service.Config().Tenant, "action", result.Action)
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "aggregate_participant_fanout",
			Repository: repo,
			Status:     "success",
		})
	}

	// Handle help command
	if result.IsHelp {
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping help command (respond_to_unscoped is false)", "repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		h.logger.Info("processing help command", "repo", repo, "pr", pr)
		h.postComment(repo, pr, installationID, templates.RenderHelpComment())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "help posted"})
		return
	}

	// Reject a malformed -e value. It can never match any instance's
	// allowed_environments, so no instance would act on it. On an aggregate
	// repo participants defer the reply to the leader, which posts it exactly
	// once; otherwise the respond_to_unscoped policy picks one responder.
	if gateReason == issueCommentGateInvalidEnvironment {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping malformed environment reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping invalid environment response (respond_to_unscoped is false)",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		h.logger.Info("rejecting command with invalid environment value",
			"repo", repo, "pr", pr, "action", result.Action)
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
		h.postComment(repo, pr, installationID,
			templates.RenderInvalidEnv(result.Action, h.knownEnvironments()))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid environment value"})
		return
	}

	// Handle missing -e flag. Plan without -e is a valid multi-env request;
	// for every other command a missing -e is a usage error, answered by
	// exactly one deployment: participants defer to the leader on an aggregate
	// repo, and the respond_to_unscoped policy picks one responder otherwise.
	if gateReason == issueCommentGateMissingEnvironment {
		if result.Action == action.Plan {
			// Plan without -e: run for all configured environments. The same
			// acknowledgment split as scoped commands applies: repos without an
			// aggregate role and -t-scoped plans acknowledge at dispatch, while
			// an unscoped plan on an aggregate-role repo acknowledges at the
			// handler's act-point once discovery resolves owned schema.
			h.logger.Info("plan without -e flag", "repo", repo, "pr", pr)
			if h.service == nil || h.service.Config().AggregateRoleForRepo(repo) == "" || result.Tenant != "" {
				h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
			}
			h.goSafe(repo, pr, installationID, deliveryID, func() {
				h.handleMultiEnvPlan(repo, pr, result.Database, result.Tenant, installationID, requestedBy, false, true, result.CommentID)
			})
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "multi-env plan started"})
			return
		}
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping missing environment reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping missing environment response (respond_to_unscoped is false)",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		if result.Action == action.Rollback {
			if result.ApplyID == "" {
				h.postComment(repo, pr, installationID, templates.RenderRollbackMissingArguments())
				h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing rollback arguments"})
				return
			}
			h.postComment(repo, pr, installationID, templates.RenderRollbackMissingEnv())
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
			return
		}
		if result.Action == action.RollbackConfirm {
			h.postComment(repo, pr, installationID, templates.RenderRollbackMissingEnv())
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderMissingEnv(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
		return
	}

	// When allowed_environments is configured, commands targeting environments
	// handled by another instance are silently ignored — that instance will
	// process the command from its own webhook delivery. An environment absent
	// from this instance's configuration entirely is rejected under the
	// respond_to_unscoped policy so exactly one instance corrects the caller —
	// except on an aggregate repo, where a participant's configuration covers
	// only its own slice of the fleet's environments and a sibling deployment
	// may serve the value, so participants defer silently instead.
	if gateReason == issueCommentGateEnvironmentNotOwned {
		if !h.service.Config().IsEnvironmentKnown(result.Environment) {
			if h.silentUnknownEnvOnAggregateFanOut(repo) {
				h.logger.Info("deferring unknown environment on aggregate participant; a sibling deployment may serve it",
					"repo", repo, "pr", pr, "environment", result.Environment, "tenant", result.Tenant, "action", result.Action)
				h.writeJSON(w, http.StatusOK, map[string]string{"message": "environment deferred to sibling deployments"})
				return
			}
			if result.Tenant == "" && !h.service.Config().ShouldRespondToUnscoped() {
				h.logger.Debug("skipping unknown environment response (respond_to_unscoped is false)",
					"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
				h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
				return
			}
			h.logger.Info("rejecting command for unknown environment",
				"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
			h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
			h.postComment(repo, pr, installationID,
				templates.RenderInvalidEnv(result.Action, h.knownEnvironments()))
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unknown environment"})
			return
		}
		h.logger.Info("ignoring command for environment owned by another instance",
			"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
		h.writeJSON(w, http.StatusOK, map[string]string{
			"message": "environment handled by another instance",
		})
		return
	}

	if gateReason == issueCommentGateMissingApplyID {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping rollback missing-apply-id reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderRollbackMissingApplyID(h.deploymentTenant()))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing apply ID"})
		return
	}

	// Handle invalid command (schemabot mentioned but command not recognized)
	if gateReason == issueCommentGateCommandNotFound {
		if result.Tenant == "" && h.service != nil && !h.service.Config().ShouldRespondToUnscoped() {
			h.logger.Debug("skipping invalid command response (respond_to_unscoped is false)", "repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "unscoped command skipped"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
		return
	}

	// Reject -y/--yes: it is a CLI flag, and no comment command takes it
	if gateReason == issueCommentGateAutoConfirm {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping unsupported auto-confirm flag reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderUnsupportedAutoConfirm(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}
	if gateReason == issueCommentGateDeferCutover {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping misplaced defer-cutover flag reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		h.postCommandError(repo, pr, installationID, action.Rollback, result.Environment, requestedBy,
			"`--defer-cutover` belongs on `schemabot rollback-confirm`, after reviewing the rollback plan.")
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}

	if gateReason == issueCommentGateDatabase {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping unsupported database flag reply for unscoped fan-out; the leader posts it once",
				"repo", repo, "pr", pr, "action", result.Action)
			h.writeJSON(w, http.StatusOK, map[string]string{"message": "usage error deferred to leader"})
			return
		}
		h.postComment(repo, pr, installationID, templates.RenderUnsupportedDatabaseFlag(result.Action))
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unsupported flag"})
		return
	}

	// The branches above match the shared ladder's reasons one-for-one. A
	// reason none of them recognized means a gate was added to the ladder
	// without a request-path branch — the ladder said to block, so block:
	// dispatching here would run a command the ladder rejected.
	if gateReason != issueCommentGatePass {
		h.logger.Error("blocking command because its gate reason has no request-path branch",
			"repo", repo, "pr", pr, "action", result.Action, "reason", gateReason)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "command blocked"})
		return
	}

	// Two cases are decidable at dispatch and acknowledge immediately: a repo
	// with no aggregate role has exactly one SchemaBot (no ownership question),
	// and a -t-scoped command names its actor (every non-addressed deployment
	// was already filtered by the tenant gate above, so reaching this point
	// scoped means this deployment is the addressee). Unscoped commands on
	// aggregate-role repos defer to each handler's act-point, after the
	// fan-out silent-skip gates, where ownership is actually known.
	if h.service == nil || h.service.Config().AggregateRoleForRepo(repo) == "" || result.Tenant != "" {
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
	}

	h.logger.Info("processing command",
		"action", result.Action,
		"environment", result.Environment,
		"repo", repo,
		"pr", pr,
	)

	switch result.Action {
	case action.Plan:
		h.handlePlanCommand(w, repo, pr, result.Environment, result.Database, result.Tenant, installationID, deliveryID, requestedBy, result.CommentID)
	case action.Help:
		h.postComment(repo, pr, installationID, templates.RenderHelpComment())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "help posted"})
	case action.Apply:
		if h.durableWebhookDispatch {
			h.enqueueDurableIssueCommentCommand(ctx, w, metricApp, body, deliveryID, repo, pr, installationID, result.Action)
			return
		}
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleApplyCommand(repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "apply started"})
	case action.ApplyConfirm:
		if h.durableWebhookDispatch {
			h.enqueueDurableIssueCommentCommand(ctx, w, metricApp, body, deliveryID, repo, pr, installationID, result.Action)
			return
		}
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleApplyConfirmCommand(repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "apply-confirm started"})
	case action.Unlock:
		if h.durableWebhookDispatch {
			h.enqueueDurableIssueCommentCommand(ctx, w, metricApp, body, deliveryID, repo, pr, installationID, result.Action)
			return
		}
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleUnlockCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "unlock started"})
	case action.Rollback:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleRollbackCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback started"})
	case action.RollbackConfirm:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleRollbackConfirmCommand(repo, pr, result.Environment, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "rollback-confirm started"})
	case action.Stop:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleStopCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "stop started"})
	case action.Cancel:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleCancelCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "cancel started"})
	case action.Start:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleStartCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "start started"})
	case action.Release:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleReleaseCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "release started"})
	case action.Cutover:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleCutoverCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "cutover started"})
	case action.SkipRevert:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleSkipRevertCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "skip-revert started"})
	case action.Revert:
		h.goSafe(repo, pr, installationID, deliveryID, func() {
			h.handleRevertCommand(repo, pr, installationID, requestedBy, result)
		})
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "revert started"})
	default:
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "invalid command"})
	}
}

func commandRequiresTenantTarget(result CommandResult) bool {
	return !result.IsHelp && (result.Found || result.MissingEnv)
}

// fansOutUnscopedCommand reports whether this deployment should self-serve an
// unscoped work command (no -t tenant) for repo by acting on work it owns,
// rather than ignoring it. An aggregate participant fans out: an unscoped
// command on a shared repo reaches every participant, and each acts only on
// what it owns — its own databases for plan/apply/unlock, or the target apply
// when it is the one holding it in storage (see actionFansOutUnscoped for the
// per-action discriminators). A tenanted deployment that is not a participant
// for repo keeps ignoring unscoped work commands, since per-tenant routing
// requires an explicit -t.
func (h *Handler) fansOutUnscopedCommand(repo string) bool {
	if h.service == nil {
		return false
	}
	return h.service.Config().AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// actionFansOutUnscoped reports whether an action is one a participant can serve
// without an explicit -t, by acting only on work it owns. Every action in the
// set routes by a discriminator that resolves to exactly one deployment, so a
// fan-out still yields a single actor per unit of work:
//   - plan, apply, and apply-confirm route by environment/database — each
//     participant handles its own share of a shared PR;
//   - unlock releases only the participant's own database locks (locks are
//     keyed by database, not by apply);
//   - rollback and the lifecycle controls (stop, cancel, start, release,
//     cutover, skip-revert, revert) route by apply identifier, which
//     lives in exactly one deployment's storage — non-owners silently skip the
//     lookup miss (see silentOnUnscopedFanOut) and only the owner acts;
//   - rollback-confirm routes by the pinned pending rollback plan for the
//     PR/environment, held only by the deployment that posted the plan.
//
// An action outside the set requires an explicit -t until a single-owner
// discriminator is established for it.
func actionFansOutUnscoped(a string) bool {
	switch a {
	case action.Plan, action.Apply, action.ApplyConfirm, action.Unlock,
		action.Rollback, action.RollbackConfirm,
		action.Stop, action.Cancel, action.Start, action.Release,
		action.Cutover, action.SkipRevert, action.Revert:
		return true
	default:
		return false
	}
}

// unscopedCommandFansOut reports whether an unscoped (no -t) command is one a
// participant should actually act on when fanning out, as opposed to an error
// case it should stay silent on. Only fan-out actions qualify (see
// actionFansOutUnscoped). A complete command (Found) fans out, and a plan
// without -e fans out as a multi-env plan. A missing-env command other than
// plan does NOT fan out: otherwise every participant on a shared repo would
// post its own duplicate "missing environment" comment. The leader (which
// never hits the tenant gate) posts that error once.
func unscopedCommandFansOut(result CommandResult) bool {
	if !actionFansOutUnscoped(result.Action) {
		return false
	}
	if result.Found {
		return true
	}
	return result.MissingEnv && result.Action == action.Plan
}

// postCommentReportingError posts a comment on a PR and reports whether it
// landed. A caller that records durable state describing what a comment told
// the operator must post through this rather than postComment: a swallowed post
// failure would leave the record claiming they were shown something they never
// saw.
func (h *Handler) postCommentReportingError(repo string, pr int, installationID int64, body string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return fmt.Errorf("create GitHub client to comment on %s#%d: %w", repo, pr, err)
	}

	if _, _, err := client.CreateIssueComment(ctx, repo, pr, h.renderPRComment(body)); err != nil {
		return fmt.Errorf("post comment on %s#%d: %w", repo, pr, err)
	}
	return nil
}

// postComment posts a comment on a PR, best effort.
func (h *Handler) postComment(repo string, pr int, installationID int64, body string) {
	if err := h.postCommentReportingError(repo, pr, installationID, body); err != nil {
		h.logger.Error("failed to post comment",
			"repo", repo, "pr", pr, "installation_id", installationID, "error", err)
	}
}

// postAndTrackComment creates a PR comment and stores its ID in apply_comments.
// Progress comments record the apply's control phase at post time — derived
// here, matching the observer's variant, so no caller can post a progress
// comment that silently disables phase-rotation detection — and other comment
// states carry no phase.
func (h *Handler) postAndTrackComment(
	ctx context.Context, repo string, pr int, installationID int64,
	apply *storage.Apply, commentState string, body string,
) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client for tracked comment", "error", err)
		return
	}

	commentID, _, err := client.CreateIssueComment(ctx, repo, pr, h.renderPRComment(body))
	if err != nil {
		h.logger.Error("failed to post tracked comment",
			"repo", repo, "pr", pr, "commentState", commentState, "error", err)
		return
	}

	comment := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    commentState,
		GitHubCommentID: commentID,
	}
	if commentState == state.Comment.Progress {
		phase := controlPhase(apply.State)
		comment.PostedPhase = &phase
	}
	if err := h.service.Storage().ApplyComments().Upsert(ctx, comment); err != nil {
		h.logger.Error("failed to store comment ID",
			"applyID", apply.ID, "commentState", commentState, "commentID", commentID, "error", err)
	}
}

// postInitialProgressComment posts the initial progress comment for a freshly
// accepted apply and, when the apply reached a terminal state before the
// comment landed, finalizes the comment in place. The driver's observer can
// only edit a tracked comment that exists: an apply that finishes faster than
// this post (for example a metadata-only DDL) has already had its terminal
// callback find nothing to edit, so the freshly posted comment would otherwise
// stay frozen at its starting state after the summary comment. Re-checking the
// apply after the post closes that window from this side — whichever of the
// observer's terminal edit and this finalize runs last converges the comment
// on the terminal rendering.
func (h *Handler) postInitialProgressComment(ctx context.Context, repo string, pr int, installationID int64, apply *storage.Apply, body string) {
	applyID := apply.ID
	h.postAndTrackComment(ctx, repo, pr, installationID, apply, state.Comment.Progress, body)

	apply, err := h.service.Storage().Applies().Get(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to re-check apply state after initial progress comment; if the apply already finished, its progress comment stays at the starting state",
			"repo", repo, "pr", pr, "error", err)
		return
	}
	if apply == nil {
		h.logger.Error("apply missing when re-checking state after initial progress comment",
			"repo", repo, "pr", pr)
		return
	}
	if !state.IsTerminalApplyState(apply.State) {
		h.logger.Debug("apply is still active after initial progress comment; the observer owns all further edits",
			apply.LogAttrs()...)
		return
	}

	h.logger.Info("apply reached a terminal state before its initial progress comment; finalizing the comment in place",
		apply.LogAttrs()...)

	comment, err := h.service.Storage().ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	if err != nil {
		h.logger.Error("failed to load tracked progress comment for finalization",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	if comment == nil {
		// Nothing to finalize: either the GitHub post itself failed, or the post
		// succeeded but the tracking upsert did not (postAndTrackComment logged
		// which). In the latter case a comment exists on the PR with no stored
		// ID to edit, so it stays at its starting state until reconciliation.
		h.logger.Debug("no tracked progress comment to finalize for terminal apply",
			apply.LogAttrs()...)
		return
	}
	if comment.EditCount > 0 {
		// The observer has already found and edited the tracked comment, so its
		// terminal edit lands (or has landed) with the full per-operation
		// rendering. Skipping keeps this no-operations fallback from
		// overwriting that richer body.
		h.logger.Debug("observer already edits the tracked progress comment; skipping handler finalize",
			apply.LogAttrs()...)
		return
	}

	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Error("failed to create GitHub client to finalize progress comment",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	finalBody := formatProgressComment(apply, nil, nil, h.deploymentTenant())
	if err := client.EditIssueComment(ctx, repo, comment.GitHubCommentID, h.renderPRComment(finalBody)); err != nil {
		h.logger.Error("failed to finalize progress comment for already-terminal apply",
			append(apply.LogAttrs(), "github_comment_id", comment.GitHubCommentID, "error", err)...)
		return
	}
	if err := h.service.Storage().ApplyComments().IncrementEditCount(ctx, applyID, state.Comment.Progress); err != nil {
		h.logger.Error("failed to increment edit count after finalizing progress comment",
			append(apply.LogAttrs(), "error", err)...)
	}
}

// acknowledgeCommandActPoint adds the eyes reaction once a handler commits to
// acting on an unscoped command on an aggregate-role repo — there, a fan-out
// means "heard" and "acting" differ, so only the deployments actually doing
// work acknowledge and an ignoring deployment leaves only its skip log. Repos
// without an aggregate role and -t-scoped commands acknowledged at dispatch
// already, so this is a no-op for them.
func (h *Handler) acknowledgeCommandActPoint(repo string, pr int, installationID int64, result CommandResult) {
	if result.Tenant != "" {
		return
	}
	if h.service == nil || h.service.Config().AggregateRoleForRepo(repo) == "" {
		return
	}
	h.acknowledgeCommand(repo, pr, installationID, result.DeliveryID, result.CommentID)
}

// acknowledgeCommandEarlyIfOwned acknowledges an unscoped command on an
// aggregate-role repo as soon as ownership is decidable from config discovery
// alone — the config file resolves to a database this deployment's registry
// knows, under an allowed schema directory — without waiting for the schema
// files to load, which on large schema directories dominates the latency
// between the command and its acknowledgment. The probe is advisory: it mirrors
// the source-policy predicates without their logs and metrics, the authoritative
// checks still run in discovery immediately after, and any probe miss defers to
// the handler's act-point acknowledgment. Returns whether it acknowledged.
func (h *Handler) acknowledgeCommandEarlyIfOwned(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, databaseName, tenant string, installationID int64, deliveryID string, commentID int64) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok || config.AggregateRoleForRepo(repo) == "" {
		return false
	}
	var (
		sbConfig  *ghclient.SchemabotConfig
		configDir string
		err       error
	)
	if databaseName != "" {
		sbConfig, configDir, err = client.FindConfigByDatabaseName(ctx, repo, pr, databaseName)
	} else {
		sbConfig, configDir, err = client.FindConfigForPR(ctx, repo, pr)
	}
	if err != nil || sbConfig == nil {
		h.logger.Debug("early ownership probe could not resolve a schema config; acknowledgment defers to the act-point",
			"repo", repo, "pr", pr, "database", databaseName, "error", err)
		return false
	}
	if config.RepoHasSchemaDirAllowlist(repo) && !config.SchemaPathAllowedForRepo(repo, configDir) {
		return false
	}
	if config.Database(sbConfig.Database) == nil {
		return false
	}
	h.acknowledgeCommand(repo, pr, installationID, deliveryID, commentID)
	return true
}

// knownEnvironments returns the configured environment roster for error
// comments, or nil when the handler has no service configuration.
func (h *Handler) knownEnvironments() []string {
	if h.service == nil {
		return nil
	}
	return h.service.Config().KnownEnvironments()
}

// commandAcknowledgmentReaction is the reaction a deployment leaves on a
// command comment to signal that the command is its work and it has committed
// to acting (UX-2). On a repository several deployments serve it is also the
// only signal about a command that every one of them can read, which is how a
// command no deployment claimed is distinguished from one a sibling owns.
const commandAcknowledgmentReaction = "eyes"

// acknowledgeCommand adds the acknowledgment reaction to the command comment,
// signalling "this deployment is acting on your command".
func (h *Handler) acknowledgeCommand(repo string, pr int, installationID int64, deliveryID string, commentID int64) {
	if commentID <= 0 || h.ghClients.Len() == 0 {
		return
	}
	h.goSafe(repo, pr, installationID, deliveryID, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client, err := h.clientForRepo(repo, installationID)
		if err != nil {
			h.logger.Error("failed to create GitHub client for command acknowledgment",
				"repo", repo, "pr", pr, "error", err)
			return
		}
		if err := client.AddReactionToComment(ctx, repo, commentID, commandAcknowledgmentReaction); err != nil {
			h.logger.Error("failed to add command acknowledgment reaction",
				"repo", repo, "pr", pr, "error", err)
		}
	})
}

func (h *Handler) renderPRComment(body string) string {
	return appendSupportChannelFooter(body, h.supportChannel())
}

func (h *Handler) supportChannel() api.SupportChannelConfig {
	cfg := h.config()
	if cfg == nil {
		return api.SupportChannelConfig{}
	}
	return cfg.SupportChannel
}

func (h *Handler) agentHint() string {
	cfg := h.config()
	if cfg == nil {
		return ""
	}
	return cfg.AgentHint
}

// appendSupportChannelFooter adds the configured support-channel footer to
// comments that declared themselves eligible at render time (see
// templates.OffersSupportChannel). Eligibility is a render-layer decision;
// this layer only checks the deployment has a support channel configured.
func appendSupportChannelFooter(body string, support api.SupportChannelConfig) string {
	if !support.Enabled() || !templates.OffersSupportChannel(body) {
		return body
	}
	return templates.RenderSupportChannelFooter(body, templates.SupportChannelData{
		Name: support.Name,
		URL:  support.URL,
	})
}

// enqueueDurableIssueCommentCommand persists a command delivery the
// ready-check admits into the durable inbox and ACKs the webhook. The request
// path has already run the routing and usage gates, so the stored row is a
// command this deployment committed to act on; a leased driver re-drives the
// command core with retries so a process restart cannot drop an acknowledged
// command.
// Enqueue failure is a deliberate 500 with no in-process fallback — it fails
// loudly (a red delivery in GitHub's webhook UI) so an operator can Redeliver.
func (h *Handler) enqueueDurableIssueCommentCommand(ctx context.Context, w http.ResponseWriter, metricApp string, body []byte, deliveryID, repo string, pr int, installationID int64, commandAction string) {
	inserted, err := h.enqueueDurableIssueComment(ctx, body, deliveryID, repo, pr, installationID)
	if err != nil {
		h.logger.Error("failed to enqueue durable issue_comment command",
			"repo", repo, "pr", pr, "command", commandAction,
			"installation_id", installationID, "delivery_id", deliveryID, "error", err)
		metrics.RecordWebhookEvent(ctx, metricApp, "issue_comment", "created", repo, "durable_enqueue_failed")
		h.writeError(w, http.StatusInternalServerError, "failed to enqueue webhook delivery")
		return
	}
	if !inserted {
		h.logger.Info("durable issue_comment command already queued",
			"repo", repo, "pr", pr, "command", commandAction, "delivery_id", deliveryID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": commandAction + " already queued"})
		return
	}
	h.logger.Info("durable issue_comment command queued",
		"repo", repo, "pr", pr, "command", commandAction, "delivery_id", deliveryID)
	h.writeJSON(w, http.StatusOK, map[string]string{"message": commandAction + " queued"})
}

// enqueueDurableIssueComment persists an issue_comment delivery in the inbox.
// The stored TenantID is the resolved installation ID so the driver, which
// runs outside any HTTP request, does not have to re-resolve a repo-level
// install.
func (h *Handler) enqueueDurableIssueComment(ctx context.Context, body []byte, deliveryID, repo string, pr int, installationID int64) (bool, error) {
	return h.enqueueDurableWebhookEvent(ctx, &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  deliveryID,
		Event:       "issue_comment",
		Action:      "created",
		Repository:  repo,
		PullRequest: pr,
		TenantID:    strconv.FormatInt(installationID, 10),
		Payload:     body,
	})
}

// processDurableIssueComment re-drives a durably dispatched command from
// a claimed issue_comment delivery. The request path runs the synchronous
// routing gates (tenant addressing, environment ownership, usage errors) and
// the acknowledgment reaction before enqueueing, so the driver's job is the
// command itself: it re-parses the stored comment — parsing is deterministic
// on the payload — and routes to the command core, whose (retry, err)
// disposition drives the lease. Every enqueue-time gate is re-validated
// fail-closed (rows can arrive via replay or a hand-crafted insert, and
// configuration can change between enqueue and drive), so a delivery the
// request path would not have enqueued — a non-command comment, an
// unsupported flag, or a tenant or environment this deployment does not
// route — completes as a no-op rather than running unrouted work.
func (h *Handler) processDurableIssueComment(ctx context.Context, event *storage.WebhookEvent) (retry bool, err error) {
	var payload webhookPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return false, fmt.Errorf("decode durable issue_comment delivery %s: %w", event.DeliveryID, err)
	}
	if payload.Action != "created" || payload.Issue == nil || payload.Issue.PullRequest == nil ||
		payload.Comment == nil || payload.Repository == nil {
		h.logger.Info("durable issue_comment delivery ignored because it is not a PR comment creation",
			"delivery_id", event.DeliveryID, "action", payload.Action,
			"repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}
	payload.Repository.FullName = storage.CanonicalKey(payload.Repository.FullName)
	if payload.Comment.User != nil && payload.Comment.User.Type == "Bot" {
		h.logger.Info("durable issue_comment delivery ignored because the comment author is a bot",
			"delivery_id", event.DeliveryID, "repo", event.Repository, "pr", event.PullRequest)
		return false, nil
	}

	installationID, err := durableInstallationID(event)
	if err != nil {
		return false, err
	}

	repo := payload.Repository.FullName
	pr := payload.Issue.Number
	if repo == "" || pr == 0 {
		return false, fmt.Errorf("durable issue_comment delivery %s missing repo or PR", event.DeliveryID)
	}
	if h.service != nil && !h.service.Config().IsRepoAllowed(repo) {
		h.logger.Warn("durable issue_comment delivery from unregistered repository",
			"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "installation_id", installationID)
		metrics.RecordUnregisteredRepositoryWebhook(ctx, h.metricAppForRepo(repo), "issue_comment", payload.Action, repo)
		return false, nil
	}

	requestedBy := ""
	if payload.Comment.User != nil {
		requestedBy = payload.Comment.User.Login
	}
	parser := NewCommandParser()
	result := parser.ParseCommand(payload.Comment.Body)
	result.CommentID = payload.Comment.ID
	result.DeliveryID = event.DeliveryID
	result.SuppressRetryComments = true
	// The two drop paths below can swallow a command the user already saw
	// acknowledged: a config or command-spec change between enqueue and drive
	// reclassifies a legitimately enqueued row, and the user is left waiting on
	// the acknowledgment reaction. Warn plus a distinct completion status keeps
	// the swallow operator-visible so the delivery can be found and redelivered.
	if !durableIssueCommentCommandReady(result) {
		h.logger.Warn("durable issue_comment delivery completed without dispatch because its comment is not a durably dispatched command",
			"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "command", result.Action)
		metrics.RecordWebhookEvent(ctx, h.metricAppForRepo(repo), "issue_comment", payload.Action, repo, "durable_command_not_ready")
		return false, nil
	}
	if reason := h.issueCommentGateBlock(repo, result, parser, payload.Comment.Body); reason != issueCommentGatePass {
		if reason == issueCommentGateInvalidTenant ||
			reason == issueCommentGateInvalidEnvironment || reason == issueCommentGateMissingEnvironment {
			h.logger.Warn("durable issue_comment delivery completed without dispatch because its command is malformed",
				"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "command", result.Action, "reason", reason)
			metrics.RecordWebhookEvent(ctx, h.metricAppForRepo(repo), "issue_comment", payload.Action, repo, "durable_command_not_ready")
			return false, nil
		}
		h.logger.Warn("durable issue_comment delivery completed without dispatch because the request path would not have dispatched it",
			"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "command", result.Action, "reason", reason)
		metrics.RecordWebhookEvent(ctx, h.metricAppForRepo(repo), "issue_comment", payload.Action, repo, "durable_command_routing_blocked")
		return false, nil
	}

	var coreRetry bool
	var coreErr error
	switch result.Action {
	case action.Apply:
		coreRetry, coreErr = h.applyCommandCore(ctx, repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
	case action.ApplyConfirm:
		coreRetry, coreErr = h.applyConfirmCommandCore(ctx, repo, pr, result.Environment, result.Database, installationID, requestedBy, result)
	case action.Unlock:
		coreRetry, coreErr = h.unlockCommandCore(ctx, event.ReceivedAt, repo, pr, installationID, requestedBy, result)
	default:
		// The ready-check and this routing switch are two enumerations of the
		// durably dispatched command set; a command admitted by one and missing
		// from the other would swallow work the user already saw acknowledged.
		// Keep the swallow operator-visible the same way as a ready-check miss.
		h.logger.Warn("durable issue_comment delivery completed without dispatch because its command has no durable driver",
			"delivery_id", event.DeliveryID, "repo", repo, "pr", pr, "command", result.Action)
		metrics.RecordWebhookEvent(ctx, h.metricAppForRepo(repo), "issue_comment", payload.Action, repo, "durable_command_unrouted")
		return false, nil
	}
	if coreErr != nil {
		return coreRetry, coreErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The run was cancelled (shutdown or lease loss) — the command may have
		// stopped partway, so keep the delivery retryable instead of completing
		// it. The cores bound their own work with a private timeout, so ctx here
		// only carries the driver's run lifetime.
		return true, fmt.Errorf("durable issue_comment delivery %s run cancelled during %s command: %w", event.DeliveryID, result.Action, ctxErr)
	}
	return coreRetry, nil
}

// durableIssueCommentCommand identifies the command and PR destination in a
// stored issue_comment payload. Terminal notification re-derives the repo,
// PR, command, and requester from the payload rather than the denormalized
// inbox columns, applying the same shape and command-ready gates as normal
// dispatch; the installation ID comes from the tenant column, the same
// source normal dispatch uses. The routing gates (repo
// allow-list, tenant and environment ownership) are intentionally not
// re-run: a routing-blocked delivery completes as a no-op on its first
// attempt and never exhausts its retry budget, so no routing-blocked row can
// reach terminal notification.
func durableIssueCommentCommand(event *storage.WebhookEvent) (CommandResult, string, int, int64, string, error) {
	var payload webhookPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return CommandResult{}, "", 0, 0, "", fmt.Errorf("decode durable issue_comment terminal notification %s: %w", event.DeliveryID, err)
	}
	if payload.Action != "created" || payload.Issue == nil || payload.Issue.PullRequest == nil ||
		payload.Comment == nil || payload.Repository == nil {
		return CommandResult{}, "", 0, 0, "", fmt.Errorf("durable issue_comment terminal notification %s is not a PR comment creation", event.DeliveryID)
	}
	result := NewCommandParser().ParseCommand(payload.Comment.Body)
	if !durableIssueCommentCommandReady(result) {
		return CommandResult{}, "", 0, 0, "", fmt.Errorf("durable issue_comment terminal notification %s is not a durable command", event.DeliveryID)
	}
	installationID, err := durableInstallationID(event)
	if err != nil {
		return CommandResult{}, "", 0, 0, "", err
	}
	repo := storage.CanonicalKey(payload.Repository.FullName)
	if repo == "" || payload.Issue.Number == 0 {
		return CommandResult{}, "", 0, 0, "", fmt.Errorf("durable issue_comment terminal notification %s is missing repo or PR", event.DeliveryID)
	}
	requestedBy := ""
	if payload.Comment.User != nil {
		requestedBy = payload.Comment.User.Login
	}
	return result, repo, payload.Issue.Number, installationID, requestedBy, nil
}

// durableIssueCommentCommandReady reports whether the driver implements the
// re-parsed command. Routing and usage validity are evaluated by the shared
// gate predicate after this command-set check, so a command outside the
// durable set classifies as not-ready even when it also trips a gate.
func durableIssueCommentCommandReady(result CommandResult) bool {
	switch result.Action {
	case action.Apply, action.ApplyConfirm, action.Unlock:
		return true
	default:
		return false
	}
}
