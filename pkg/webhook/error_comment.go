package webhook

import (
	"errors"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/presentation"
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
	problems, omitted := api.PlanRefusalProblems(err)
	h.postCommentBestEffortID(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		CommandName:  action.Plan,
		ErrorDetail:  userFacingError(err),
		PlanProblems: templates.RefusedStatements{Problems: problems, Omitted: omitted},
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
		ErrorDetail:  errorDetail,
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

// unexpectedFailureMessage is the PR comment line for a failure whose text
// SchemaBot did not write, such as a storage, driver, GitHub client, or engine
// error. That text can carry hostnames, addresses, and driver internals, so the
// line names none of it and the caller logs the error with its identifiers.
const unexpectedFailureMessage = "SchemaBot could not complete the command. " + rerunAdvice

// userFacingError is the line a PR comment or check summary shows for a failed
// command. Errors SchemaBot recognizes get a line it wrote. An error that marks
// its own text as written for the person who ran the command shows that text.
// Every other error shows unexpectedFailureMessage, never its own text (AV-8).
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
	if peerErr, ok := errors.AsType[*sharedSchemaPeerUnreadableError](err); ok {
		return sharedSchemaPeerUnreadableMessage(peerErr)
	}
	// Authored text can still quote names from the pull request, such as a
	// parser's complaint about a statement, so it is never trusted as given:
	// InlineError strips control characters, redacts connection details,
	// collapses line breaks so the text stays inside the blockquote or table
	// row that holds it, and clamps it to the length every error detail on the
	// pull request has.
	if text, _, ok := api.AuthoredTextOf(err); ok {
		if line := presentation.InlineError(text); line != "" {
			return line
		}
	}
	if message, ok := userFacingConfigDiscoveryError(err); ok {
		return message
	}
	return unexpectedFailureMessage
}

// userFacingConfigDiscoveryError is the line for a schemabot.yaml discovery
// failure. These sentinels carry no text of their own to publish, so each has a
// line SchemaBot wrote that says what the author can do.
func userFacingConfigDiscoveryError(err error) (string, bool) {
	switch {
	case errors.Is(err, ghclient.ErrConfigDiscoveryTruncated):
		return "GitHub returned a truncated repository tree, so SchemaBot could not search this repository for `schemabot.yaml` configurations. " +
			"Ask a SchemaBot operator to give each of this repository's databases an `allowed_dirs` entry naming its schema directory.", true
	case errors.Is(err, ghclient.ErrNoConfig):
		return "No `schemabot.yaml` configuration file was found in this repository. " +
			"Add one in the directory that holds the `.sql` files declaring your tables, or use `-d` to name a database that already has one.", true
	case errors.Is(err, ghclient.ErrInvalidConfig):
		return "A `schemabot.yaml` in this repository is not valid. It must include the `database` and `type` fields; fix the file and push a commit.", true
	case errors.Is(err, ghclient.ErrMultipleConfigs):
		return "This repository has more than one `schemabot.yaml`, so SchemaBot cannot tell which database you mean. Use `-d` with the database name.", true
	}
	return "", false
}

// sharedSchemaPeerUnreadableMessage is the PR comment line for a shared schema
// check that could not read the schema files of a database sharing a target
// with the planned one. It names the peer, the planned database, and the
// environment, then adds the reason only when the cause is text SchemaBot or a
// parser wrote for the author, such as an unauthorized schema path or a file
// that does not parse. Any other cause stays in the server logs.
func sharedSchemaPeerUnreadableMessage(err *sharedSchemaPeerUnreadableError) string {
	line := "SchemaBot could not read the schema files of `" + err.Peer + "`, which shares a target with `" + err.Database + "` in " + err.Environment +
		", so it could not compare their ignore_tables."
	if text, _, ok := api.AuthoredTextOf(err.Cause); ok {
		line += " " + text
	}
	if safe := presentation.InlineError(line); safe != "" {
		return safe
	}
	return unexpectedFailureMessage
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
