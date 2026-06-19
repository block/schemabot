package tern

import "github.com/block/schemabot/pkg/storage"

// stopTerminatesChange reports whether stopping a schema change on the given
// engine type permanently cancels it — the apply and its tasks move to the
// terminal Cancelled state with no resume possible — rather than pausing it for
// resume (the Stopped state).
//
// Vitess (PlanetScale): Stop cancels the deploy request, which is permanent —
// see pkg/engine/planetscale Stop (CancelDeployRequest); its Start rejects a
// cancelled deploy request. MySQL (Spirit): Stop pauses at a checkpoint and
// Start resumes. Past binlog expiry a resume restarts the copy from scratch, but
// that is still a resumable Stopped, not a Cancelled.
//
// This is the single source of truth for the stop-terminality decision. It is
// keyed on database type rather than an engine capability because the gRPC drive
// must decide it for undispatched applies, where no engine instance — local or
// remote — exists to query. Keep it consistent with each engine's Stop
// implementation in pkg/engine.
func stopTerminatesChange(databaseType string) bool {
	return databaseType == storage.DatabaseTypeVitess
}
