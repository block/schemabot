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
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

const (
	// defaultTransientPlanRetryDelay is the pause before retrying a plan request
	// that failed with transient remote unavailability. Webhook commands respond
	// asynchronously through PR comments, so a short wait is invisible to the
	// user, and it gives the network path time to recover beyond the gRPC
	// client's own sub-second retry budget.
	defaultTransientPlanRetryDelay = 3 * time.Second

	// maxTransientPlanRetries bounds webhook plan retries so brief transport
	// blips can recover without hiding sustained remote deployment outages.
	maxTransientPlanRetries = 3
)

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

// executePlanWithTransientRetry runs ExecutePlan and retries when the
// failure is transient remote unavailability. Plan requests are safe to
// re-send: each attempt produces an independent plan record and only the
// returned plan ID is used. Every other failure class (policy, validation,
// planning) is returned immediately without a retry.
//
// Failing a webhook command on a brief network blip costs the user the whole
// command ceremony — for apply-confirm that means re-running apply and
// confirming again. A bounded delayed retry loop absorbs blips that outlast
// the gRPC client's retry budget while still surfacing sustained outages
// within seconds.
func (h *Handler) executePlanWithTransientRetry(ctx context.Context, planReq api.PlanRequest, repo string, pr int) (*apitypes.PlanResponse, error) {
	_, planResp, err := h.executePlanProtoWithTransientRetry(ctx, planReq, repo, pr)
	return planResp, err
}

// executePlanProtoWithTransientRetry runs ExecutePlanProto with the same
// transient-retry behavior as executePlanWithTransientRetry, additionally
// returning the reviewed primary plan proto so callers can feed it to the
// review-time drift rollup without re-planning or reconstructing it from
// storage.
func (h *Handler) executePlanProtoWithTransientRetry(ctx context.Context, planReq api.PlanRequest, repo string, pr int) (*ternv1.PlanResponse, *apitypes.PlanResponse, error) {
	planProto, planResp, err := h.service.ExecutePlanProto(ctx, planReq)
	if err == nil || !isTransientRemotePlanError(err) {
		return planProto, planResp, err
	}

	delay := h.planRetryDelay()
	firstErr := err
	lastErr := err
	for retryAttempt := 1; retryAttempt <= maxTransientPlanRetries; retryAttempt++ {
		h.logger.Warn("plan failed with transient remote unavailability; retrying",
			"repo", repo,
			"pr", pr,
			"database", planReq.Database,
			"environment", planReq.Environment,
			"retry_attempt", retryAttempt,
			"max_retries", maxTransientPlanRetries,
			"retry_delay", delay,
			"error", lastErr)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, fmt.Errorf("plan retry for %s/%s cancelled: %w", planReq.Database, planReq.Environment, ctx.Err())
		case <-timer.C:
		}

		planProto, planResp, retryErr := h.service.ExecutePlanProto(ctx, planReq)
		if retryErr == nil {
			metrics.RecordTransientPlanRetry(ctx, planReq.Database, planReq.Environment, "recovered")
			h.logger.Info("plan retry recovered from transient remote unavailability",
				"repo", repo,
				"pr", pr,
				"database", planReq.Database,
				"environment", planReq.Environment,
				"retry_attempt", retryAttempt,
				"plan_id", planResp.PlanID)
			return planProto, planResp, nil
		}
		if !isTransientRemotePlanError(retryErr) {
			metrics.RecordTransientPlanRetry(ctx, planReq.Database, planReq.Environment, "stopped_non_transient")
			h.logger.Warn("plan retry reached non-transient failure; stopping retries",
				"repo", repo,
				"pr", pr,
				"database", planReq.Database,
				"environment", planReq.Environment,
				"retry_attempt", retryAttempt,
				"error", retryErr)
			return nil, nil, retryErr
		}
		lastErr = retryErr
	}

	metrics.RecordTransientPlanRetry(ctx, planReq.Database, planReq.Environment, "exhausted")
	h.logger.Error("plan retries exhausted; remote deployment is still unavailable",
		"repo", repo,
		"pr", pr,
		"database", planReq.Database,
		"environment", planReq.Environment,
		"max_retries", maxTransientPlanRetries,
		"first_error", firstErr,
		"last_error", lastErr)
	return nil, nil, firstErr
}
