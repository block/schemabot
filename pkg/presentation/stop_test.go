package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// Every rollout footer decides stop and retry from these predicates. A
// failure is retried only once the apply is terminal, since a new apply for
// the same targets is refused until then, and until then stop is offered
// even when the only live member waits for cutover. A rollout whose live
// members only wait for cutover otherwise keeps the cutover as its one
// command, and a terminal apply offers no stop.
func TestRolloutFooterPredicates(t *testing.T) {
	so := state.ApplyOperation
	for name, tc := range map[string]struct {
		ops                []Operation
		stop, retry, waits bool
	}{
		"failure beside a sibling waiting for cutover": {
			ops:   []Operation{{Deployment: "us", State: so.Failed}, {Deployment: "eu", State: so.WaitingForCutover}},
			stop:  true,
			waits: true,
		},
		"failure beside a sibling still copying": {
			ops:   []Operation{{Deployment: "us", State: so.Failed, Parallel: true}, {Deployment: "eu", State: so.Running, Parallel: true}},
			stop:  true,
			waits: true,
		},
		"failure once the rollout settled": {
			ops:   []Operation{{Deployment: "us", State: so.Completed}, {Deployment: "eu", State: so.Failed}},
			retry: true,
		},
		"cutover ready with the next member not started": {
			ops: []Operation{{Deployment: "us", State: so.WaitingForCutover}, {Deployment: "eu", State: so.Pending}},
		},
		"running with nothing pending": {
			ops:  []Operation{{Deployment: "us", State: so.Running}, {Deployment: "eu", State: so.Pending}},
			stop: true,
		},
		"stopped": {
			ops: []Operation{{Deployment: "us", State: so.Stopped}, {Deployment: "eu", State: so.Running}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := Derive(tc.ops)
			assert.Equal(t, tc.stop, a.OffersRolloutStop(), "stop under %s", a.State)
			assert.Equal(t, tc.retry, a.OffersRetry(), "retry under %s", a.State)
			assert.Equal(t, tc.waits, a.RetryWaitsOnActiveApply(), "retry waits under %s", a.State)
		})
	}
}
