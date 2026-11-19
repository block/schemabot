package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

type durableSchemaPlanContextKey struct{}

var errPRSchemaPlanClaimActive = errors.New("an excepted PR plan is already running")

const prSchemaPlanCompleted = "completed"

// An exception can be observed by several deliveries or replicas. Claim each
// database separately so a retry can finish a partial fan-out. A crashed job's
// claim expires after the command's maximum execution time.
func (h *Handler) claimPRSchemaPlan(ctx context.Context, admission prSchemaAdmission, database string) (*storage.Setting, bool, error) {
	settings := h.service.Storage().Settings()
	revision, err := json.Marshal(admission.key.revision)
	if err != nil {
		return nil, false, err
	}
	plane := h.config().PRSchemaCapForRepo(admission.key.repo).ControlPlane
	key := schemaCapKey("dispatch", admission.key.repo, admission.key.pr, plane, string(revision),
		fmt.Sprint(admission.key.registration), admission.exception, database, admission.dispatchID)
	row, err := settings.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if row != nil && row.Value != "" {
		if row.Value == prSchemaPlanCompleted {
			return nil, false, nil
		}
		deadline, err := time.Parse(time.RFC3339Nano, row.Value)
		if err != nil {
			return nil, false, err
		}
		if time.Now().Before(deadline) {
			return nil, false, errPRSchemaPlanClaimActive
		}
	}
	value := time.Now().Add(commandTimeout + time.Minute).UTC().Format(time.RFC3339Nano)
	claimed, err := settings.CompareAndSet(ctx, key, row, value)
	if err != nil {
		return nil, false, err
	}
	if !claimed {
		return nil, false, errPRSchemaPlanClaimActive
	}
	return &storage.Setting{Key: key, Value: value}, true, nil
}

func (h *Handler) finishPRSchemaPlan(claim *storage.Setting, complete bool) {
	outcome := ""
	if complete {
		outcome = prSchemaPlanCompleted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.service.Storage().Settings().CompareAndSet(ctx, claim.Key, claim, outcome); err != nil {
		h.logger.Warn("could not finish the PR schema limit plan claim", "error", err)
	}
}
