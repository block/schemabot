package webhook

import (
	"errors"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// postCommandError posts a generic command-failure comment to the PR with a
// consistent UTC timestamp.
//
// Centralized so handlers cannot accidentally omit Timestamp, which previously
// caused the comment footer to render as "Requested by @<user> at  UTC"
// (empty timestamp).
//
// Uses templates.NowFunc so previews and tests can substitute a deterministic
// clock — the same hook used by templates that already render UTC timestamps.
//
// Specialized error templates (RenderDatabaseNotFound, RenderInvalidConfig,
// RenderNoConfig, RenderMultipleConfigs, RenderReviewRequired,
// RenderUnsafeChangesBlocked, etc.) are intentionally not routed through this
// helper — they have additional fields or distinct rendering and remain
// constructed explicitly by their handlers.
func (h *Handler) postCommandError(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy, errorDetail string,
) {
	h.postCommandErrorLanded(repo, pr, installationID, commandName, environment, requestedBy, errorDetail)
}

// postPlanCommandError posts a failed plan command's error. A plan refused over
// statements in the schema files lists each one with where it is, as the
// multi-environment plan comment does, so the author can fix them all in one
// push.
func (h *Handler) postPlanCommandError(
	repo string, pr int, installationID int64,
	environment, requestedBy string, err error,
) {
	h.postCommentBestEffortID(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		CommandName:  action.Plan,
		ErrorDetail:  userFacingErrorDetail(userFacingError(err)),
		PlanProblems: api.PlanRefusalProblems(err),
	}))
}

// postCommandErrorLanded is postCommandError reporting whether the comment
// landed, for a caller whose own message points at it.
func (h *Handler) postCommandErrorLanded(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy, errorDetail string,
) bool {
	return h.postCommentBestEffortID(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		CommandName:  commandName,
		ErrorDetail:  userFacingErrorDetail(errorDetail),
	})) != 0
}

// postChangeGateRefusal posts a change gate's answer to a PR command, for an
// err that isChangeGateError accepts. A freeze refusal gets its own comment,
// which SchemaBot writes itself: what is frozen, until when and why, and what
// the commenter can do next. A gate that could not decide posts its fixed
// message as a command error. It reports whether that comment landed, for a
// caller whose own message points at it.
func (h *Handler) postChangeGateRefusal(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy string, err error,
) bool {
	if refused, ok := errors.AsType[*api.ChangeGateRefusedError](err); ok {
		var approvers []string
		onReason := true // the default policy
		if config, ok := h.serverConfig(); ok {
			approvers = config.Freeze.GitHubTeams
			onReason = config.Freeze.OverrideOnReason()
		}
		data := changeGateRefusalData(refused, commandName, h.deploymentTenant(), requestedBy, approvers)
		data.OverrideOnReason = onReason
		return h.postCommentBestEffortID(repo, pr, installationID, templates.RenderChangeGateRefusal(data)) != 0
	}
	message := "SchemaBot could not check for freezes, so this did not start. Try again."
	if unavailable, ok := errors.AsType[*api.ChangeGateUnavailableError](err); ok {
		message = unavailable.UserMessage()
	}
	return h.postCommandErrorLanded(repo, pr, installationID, commandName, environment, requestedBy, message)
}

// deploymentEnvironmentScope returns the environments this deployment
// handles, for rendering in error comments about commands that were not
// scoped to a single environment. Returns nil when the command already
// targeted one environment (the comment renders that instead) or when the
// deployment is unscoped (the comment omits the environment segment — there
// is no concrete list to show).
func (h *Handler) deploymentEnvironmentScope(environment string) []string {
	if environment != "" {
		return nil
	}
	config, ok := h.serverConfig()
	if !ok {
		return nil
	}
	return config.AllowedEnvironments
}

// dropTablesCheckFailedMessage is the PR comment line for a plan refused
// because SchemaBot could not read whether an apply still needs a table the
// config lists under drop_tables.
const dropTablesCheckFailedMessage = "SchemaBot could not check whether an apply that may still resume needs a table listed under `drop_tables`, so the plan was refused. Run the command again; if it fails again, the SchemaBot server logs have the cause."

func userFacingError(err error) string {
	if message, ok := userFacingConfigNotAuthorizedError(err); ok {
		return message
	}
	// The cause is a storage failure the server logs with its identifiers.
	if _, ok := errors.AsType[*api.DropTablesCheckError](err); ok {
		return dropTablesCheckFailedMessage
	}
	// The cause is the data plane's error, which the caller logs with its
	// identifiers.
	if resolveErr, ok := errors.AsType[*api.NamespaceResolutionError](err); ok {
		return namespaceResolutionFailedMessage(resolveErr)
	}
	var remoteErr *api.RemoteDeploymentUnavailableError
	if errors.As(err, &remoteErr) {
		return userFacingRemoteUnavailableError(remoteErr.Deployment, remoteErr.Target)
	}
	if status.Code(err) == codes.Unavailable {
		return userFacingRemoteUnavailableError("", "")
	}
	return err.Error()
}

// namespaceResolutionFailedMessage is the PR comment line for a shared schema
// check that could not learn which schemas a database's namespaces address on
// a target. A data plane too old to answer needs an upgrade, and saying so is
// the whole remedy. A data plane whose engine cannot resolve namespaces will
// refuse every retry, so the line says so instead of suggesting one. Any other
// failure is named only by its identifiers.
func namespaceResolutionFailedMessage(err *api.NamespaceResolutionError) string {
	where := "target `" + err.Target + "` of deployment `" + err.Deployment + "`"
	if err.DataPlaneTooOld() {
		return "SchemaBot could not check which schemas `" + err.Database + "` shares with other databases on " + where +
			", because that deployment's data plane cannot report which schema each namespace addresses. Upgrade that data plane, then run the command again."
	}
	if errors.Is(err, tern.ErrResolveNamespacesUnsupported) {
		return "SchemaBot could not check which schemas `" + err.Database + "` shares with other databases on " + where +
			", because that deployment's data plane cannot resolve namespaces for this database's engine. Running the command again will not help; the SchemaBot server logs have the cause."
	}
	return "SchemaBot could not check which schemas `" + err.Database + "` shares with other databases on " + where +
		", so the command was refused. Run the command again; if it fails again, the SchemaBot server logs have the cause."
}

func userFacingConfigNotAuthorizedError(err error) (string, bool) {
	var configErr *schemaConfigOutsideAllowedDirsError
	if !errors.As(err, &configErr) {
		return "", false
	}
	return templates.RenderConfigNotAuthorizedLine(configErr.Database, configErr.SchemaPath), true
}

// userFacingErrorDetail replaces a command error that is a gRPC unavailable
// status, passed along as text by a caller that has no typed error, with the
// fixed unreachable line. Any other detail is returned as given.
func userFacingErrorDetail(errorDetail string) string {
	if strings.Contains(strings.ToLower(errorDetail), "rpc error: code = unavailable") {
		return userFacingRemoteUnavailableError("", "")
	}
	return errorDetail
}

// userFacingRemoteUnavailableError is the PR comment line for a remote data
// plane that did not answer. It names the deployment and target SchemaBot
// tried to reach and never the transport error, which carries hostnames and
// addresses. The caller logs that error with its identifiers.
func userFacingRemoteUnavailableError(deployment, target string) string {
	service, healthy := remoteServiceName(deployment, target)
	return "SchemaBot could not reach the " + service + ". Run the command again once " + healthy + " is healthy; if it fails again, the SchemaBot server logs have the cause."
}

// remoteServiceName names the remote data plane SchemaBot tried to reach, as
// specifically as the deployment and target allow, and how a sentence asking
// the reader to wait for it refers back to it.
func remoteServiceName(deployment, target string) (service, healthy string) {
	switch {
	case deployment != "" && target != "":
		return "remote deployment `" + deployment + "` for target `" + target + "`", "that deployment"
	case deployment != "":
		return "remote deployment `" + deployment + "`", "that deployment"
	default:
		return "remote schema change service", "the service"
	}
}
