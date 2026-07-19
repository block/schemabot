package webhook

import (
	"errors"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const remoteSchemaServiceUnavailableMessage = "SchemaBot could not reach the remote schema change service for this environment."

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
	h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		CommandName:  commandName,
		ErrorDetail:  userFacingErrorDetail(errorDetail),
	}))
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

// internalErrorDetail builds the PR-facing detail for a failure inside a
// SchemaBot-internal operation (storage reads/writes, GitHub API calls, lock
// bookkeeping). Raw error text from those operations can carry hostnames, DSN
// fragments, or driver internals, so it never renders on the PR: the comment
// shows the fixed summary and points at the server logs, where the caller
// logs the raw error with triage identifiers.
func internalErrorDetail(summary string) string {
	return summary + " See SchemaBot server logs for details."
}

// userFacingError maps an execution error to the text rendered on the PR.
// Known transport failures are replaced with fixed guidance; other errors
// render their message, which for plan and apply execution carries the
// engine's user-actionable detail (for example rejected DDL).
func userFacingError(err error) string {
	if message, ok := mappedUserFacingError(err); ok {
		return message
	}
	return err.Error()
}

// mappedUserFacingError returns the fixed PR-facing message for errors with a
// known user-facing mapping, and false when the caller must decide how to
// render the error itself.
func mappedUserFacingError(err error) (string, bool) {
	if message, ok := userFacingConfigNotAuthorizedError(err); ok {
		return message, true
	}
	var remoteErr *api.RemoteDeploymentUnavailableError
	if errors.As(err, &remoteErr) {
		return userFacingRemoteUnavailableError(remoteErr.Deployment, remoteErr.Target, err.Error()), true
	}
	if status.Code(err) == codes.Unavailable {
		return userFacingRemoteUnavailableError("", "", err.Error()), true
	}
	return "", false
}

// userFacingSchemaRequestError maps a schema discovery or environment
// validation error for rendering inside a per-environment error cell. Errors
// whose message is composed for PR display render it; everything else is an
// internal failure and renders the fixed pointer to server logs.
func userFacingSchemaRequestError(err error) string {
	if message, ok := mappedUserFacingError(err); ok {
		return message
	}
	if isUserMeaningfulSchemaRequestError(err) {
		return err.Error()
	}
	return internalErrorDetail("Failed to prepare the schema change request for this environment.")
}

// isUserMeaningfulSchemaRequestError reports whether a schema request error's
// message was authored for PR display — configuration and discovery errors
// composed only of SchemaBot-owned wording and config identifiers.
func isUserMeaningfulSchemaRequestError(err error) bool {
	var envConfigErr *environmentConfigError
	if errors.As(err, &envConfigErr) {
		return true
	}
	var dbNotFoundErr *ghclient.DatabaseNotFoundError
	if errors.As(err, &dbNotFoundErr) {
		return true
	}
	return errors.Is(err, ghclient.ErrNoConfig) ||
		errors.Is(err, ghclient.ErrInvalidConfig) ||
		errors.Is(err, ghclient.ErrMultipleConfigs) ||
		errors.Is(err, ghclient.ErrGitTreeTruncated)
}

func userFacingConfigNotAuthorizedError(err error) (string, bool) {
	var configErr *schemaConfigOutsideAllowedDirsError
	if !errors.As(err, &configErr) {
		return "", false
	}
	return strings.Join([]string{
		"SchemaBot found a `schemabot.yaml` configuration, but this SchemaBot instance is not configured to manage its schema directory.",
		"Schema directory: `" + configErr.SchemaPath + "`.",
		"Ask a SchemaBot operator to add this directory to `databases." + configErr.Database + ".allowed_dirs` in the server config, or move the schema config and files under an allowed directory.",
	}, " "), true
}

func userFacingErrorDetail(errorDetail string) string {
	lowerDetail := strings.ToLower(errorDetail)
	if isUserFacingRemoteUnavailableError(lowerDetail) {
		return errorDetail
	}
	if strings.Contains(lowerDetail, strings.ToLower(remoteSchemaServiceUnavailableMessage)) {
		return errorDetail
	}
	if strings.Contains(lowerDetail, "rpc error: code = unavailable") {
		return userFacingRemoteUnavailableError("", "", errorDetail)
	}
	return errorDetail
}

func isUserFacingRemoteUnavailableError(lowerDetail string) bool {
	return strings.Contains(lowerDetail, "schemabot could not reach the remote ")
}

// userFacingRemoteUnavailableError renders fixed guidance for an unreachable
// remote deployment. The raw error is examined to pick the guidance but never
// rendered — dial failures carry hostnames and transport internals that do not
// belong on a PR; callers log it server-side with the deployment identifiers.
func userFacingRemoteUnavailableError(deployment, target, rawError string) string {
	service := "remote schema change service"
	if deployment != "" && target != "" {
		service = "remote deployment `" + deployment + "` for target `" + target + "`"
	} else if deployment != "" {
		service = "remote deployment `" + deployment + "`"
	}
	if strings.Contains(strings.ToLower(rawError), "no healthy upstream") {
		return "SchemaBot could not reach the " + service + ". No healthy upstream is available. The service or network path is unavailable; retry after the upstream is healthy. See SchemaBot server logs for the underlying error."
	}
	return "SchemaBot could not reach the " + service + ". Retry after the service is healthy. See SchemaBot server logs for the underlying error."
}
