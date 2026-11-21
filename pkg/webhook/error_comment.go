package webhook

import (
	"errors"
	"strings"

	"github.com/block/schemabot/pkg/api"
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
	h.postCommandErrorWithin(repo, pr, installationID, commandName, environment, requestedBy, userFacingErrorDetail(errorDetail), 0)
}

// postChangeGateRefusal is postCommandError for a change gate's refusal,
// which SchemaBot writes itself and whose last sentences say how to get the
// change through, so it is posted under the longer MaxChangeGateRefusalLen.
// It is posted as written: the refusal quotes people's reasons, and a reason
// that reads like a gRPC error must not make it look like an outage.
func (h *Handler) postChangeGateRefusal(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy, errorDetail string,
) {
	h.postCommandErrorWithin(repo, pr, installationID, commandName, environment, requestedBy, errorDetail, templates.MaxChangeGateRefusalLen)
}

func (h *Handler) postCommandErrorWithin(
	repo string, pr int, installationID int64,
	commandName, environment, requestedBy, errorDetail string, detailLimit int,
) {
	h.postComment(repo, pr, installationID, templates.RenderGenericError(templates.SchemaErrorData{
		RequestedBy:  requestedBy,
		Timestamp:    templates.NowFunc().UTC().Format("2006-01-02 15:04:05"),
		Environment:  environment,
		Environments: h.deploymentEnvironmentScope(environment),
		CommandName:  commandName,
		ErrorDetail:  errorDetail,
		DetailLimit:  detailLimit,
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
	var remoteErr *api.RemoteDeploymentUnavailableError
	if errors.As(err, &remoteErr) {
		return userFacingRemoteUnavailableError(remoteErr.Deployment, remoteErr.Target, err.Error())
	}
	if status.Code(err) == codes.Unavailable {
		return userFacingRemoteUnavailableError("", "", err.Error())
	}
	return err.Error()
}

func userFacingConfigNotAuthorizedError(err error) (string, bool) {
	var configErr *schemaConfigOutsideAllowedDirsError
	if !errors.As(err, &configErr) {
		return "", false
	}
	return templates.RenderConfigNotAuthorizedLine(configErr.Database, configErr.SchemaPath), true
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
	return strings.Contains(lowerDetail, "schemabot could not reach the remote ") && strings.Contains(lowerDetail, "raw error:")
}

func userFacingRemoteUnavailableError(deployment, target, rawError string) string {
	service := "remote schema change service"
	if deployment != "" && target != "" {
		service = "remote deployment `" + deployment + "` for target `" + target + "`"
	} else if deployment != "" {
		service = "remote deployment `" + deployment + "`"
	}
	if strings.Contains(strings.ToLower(rawError), "no healthy upstream") {
		return "SchemaBot could not reach the " + service + ". No healthy upstream is available. The service or network path is unavailable; retry after the upstream is healthy. Raw error: " + rawError
	}
	return "SchemaBot could not reach the " + service + ". Retry after the service is healthy. Raw error: " + rawError
}
