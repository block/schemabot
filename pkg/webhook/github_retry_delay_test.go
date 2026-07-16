package webhook

import (
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
)

// Webhook tests stub GitHub servers that answer reads with 5xx to exercise
// failure paths; without shrinking the backoff base every such test would
// wait out the full production retry schedule.
func init() {
	_ = ghclient.SetUnavailableReadRetryDelayForTest(time.Millisecond)
}
