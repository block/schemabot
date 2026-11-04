package webhook

import (
	"encoding/json"
	"errors"
	"fmt"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// These types carry processing-boundary facts without changing retry policy
// or deriving metric labels from error text.
type durablePayloadError struct{ error }

func (e *durablePayloadError) Unwrap() error { return e.error }

type durableRouteError struct{ error }

func (e *durableRouteError) Unwrap() error { return e.error }

func decodeDurablePayload(event *storage.WebhookEvent, payload any) error {
	if err := json.Unmarshal(event.Payload, payload); err != nil {
		return &durablePayloadError{fmt.Errorf("decode durable %s delivery %s: %w", event.Event, event.DeliveryID, err)}
	}
	return nil
}

// Only call after the processor has already decided the error is permanent.
func durableFailureReason(err error) metrics.WebhookFailureReason {
	var payloadErr *durablePayloadError
	var routeErr *durableRouteError
	switch {
	case errors.Is(err, ghclient.ErrPRFilesIncomplete):
		return metrics.WebhookFailurePRFileCap
	case errors.As(err, &payloadErr):
		return metrics.WebhookFailureInvalidPayload
	case errors.As(err, &routeErr):
		return metrics.WebhookFailureUnresolvedRoute
	default:
		return metrics.WebhookFailureOther
	}
}
