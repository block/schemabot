package tern

import (
	"context"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// recordRemoteDriverChange reports the data-plane driver changing hands under a
// running schema change.
//
// A drive that polls another plane sees state and figures, not liveness. When
// the data-plane driver's lease expires and another driver claims the schema
// change, the copy pauses, changes hands, and resumes from its checkpoint —
// none of which alters the state being polled, so the poller records nothing
// and the control-plane timeline shows an unbroken run with a gap in it. An
// operator reading that timeline cannot tell a healthy long copy from one that
// lost its driver.
//
// The data plane names its current holder on the progress response, so a change
// between polls is the handover. It is recorded on the operator's timeline and
// counted, and the caller carries the returned holder into the next poll.
//
// A data plane that reports no holder leaves the last known one in place rather
// than being read as "no driver": an absent field is no evidence of a handover.
func (c *GRPCClient) recordRemoteDriverChange(ctx context.Context, apply *storage.Apply, md map[string]string, lastDriver string) string {
	driver := md["driver"]
	if driver == "" || driver == lastDriver {
		return lastDriver
	}
	logger := c.applyLogger(apply)
	if lastDriver == "" {
		// The first holder observed on this drive is the baseline, not a
		// handover: an earlier one may have existed, but this poller has no
		// evidence either way.
		logger.Debug("data-plane driver observed for the first time on this drive",
			append(apply.MutableLogAttrs(), "data_plane_driver", driver)...)
		return driver
	}
	logger.Warn("the data-plane driver changed under a running schema change; the previous driver's lease expired and the work was re-claimed",
		append(apply.MutableLogAttrs(), "previous_data_plane_driver", lastDriver, "data_plane_driver", driver)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo,
		"The data plane changed drivers: the driver holding this schema change lost its lease and another claimed it. Work resumes from its checkpoint.", "", "")
	metrics.RecordDataPlaneDriverHandover(ctx, apply.Database, apply.Deployment, apply.Environment)
	return driver
}
