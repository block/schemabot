package webhook

import (
	"context"
	"errors"
	"fmt"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/metrics"
)

// defaultTransientPlanRetryDelay is the pause before retrying a plan request
// that failed with transient remote unavailability. Webhook commands respond
// asynchronously through PR comments, so a short wait is invisible to the
// user, and it gives the network path time to recover beyond the gRPC
// client's own sub-second retry budget.
const defaultTransientPlanRetryDelay = 3 * time.Second

// isTransientRemotePlanError reports whether a plan failure means the remote
// deployment was unreachable (a retryable transport condition) rather than a
// policy, validation, or planning failure.
func isTransientRemotePlanError(err error) bool {
	var remoteErr *api.RemoteDeploymentUnavailableError
	if errors.As(err, &remoteErr) {
		return true
	}
	return grpcstatus.Code(err) == grpccodes.Unavailable
}

func (h *Handler) planRetryDelay() time.Duration {
	if h.transientPlanRetryDelay > 0 {
		return h.transientPlanRetryDelay
	}
	return defaultTransientPlanRetryDelay
}

// executePlanWithTransientRetry runs ExecutePlan and retries once when the
// failure is transient remote unavailability. Plan requests are safe to
// re-send: each attempt produces an independent plan record and only the
// returned plan ID is used. Every other failure class (policy, validation,
// planning) is returned immediately without a retry.
//
// Failing a webhook command on a brief network blip costs the user the whole
// command ceremony — for apply-confirm that means re-running apply and
// confirming again. One delayed retry absorbs blips that outlast the gRPC
// client's retry budget while still surfacing sustained outages within
// seconds.
func (h *Handler) executePlanWithTransientRetry(ctx context.Context, planReq api.PlanRequest, repo string, pr int) (*apitypes.PlanResponse, error) {
	planResp, err := h.service.ExecutePlan(ctx, planReq)
	if err == nil || !isTransientRemotePlanError(err) {
		return planResp, err
	}

	delay := h.planRetryDelay()
	h.logger.Warn("plan failed with transient remote unavailability; retrying once",
		"repo", repo,
		"pr", pr,
		"database", planReq.Database,
		"environment", planReq.Environment,
		"retry_delay", delay,
		"error", err)

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("plan retry for %s/%s cancelled: %w", planReq.Database, planReq.Environment, ctx.Err())
	case <-timer.C:
	}

	planResp, retryErr := h.service.ExecutePlan(ctx, planReq)
	if retryErr != nil {
		metrics.RecordTransientPlanRetry(ctx, planReq.Database, planReq.Environment, "exhausted")
		h.logger.Error("plan retry failed; remote deployment is still unavailable",
			"repo", repo,
			"pr", pr,
			"database", planReq.Database,
			"environment", planReq.Environment,
			"error", retryErr)
		return nil, retryErr
	}
	metrics.RecordTransientPlanRetry(ctx, planReq.Database, planReq.Environment, "recovered")
	h.logger.Info("plan retry recovered from transient remote unavailability",
		"repo", repo,
		"pr", pr,
		"database", planReq.Database,
		"environment", planReq.Environment,
		"plan_id", planResp.PlanID)
	return planResp, nil
}
