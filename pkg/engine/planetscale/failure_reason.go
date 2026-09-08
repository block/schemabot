package planetscale

import (
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// A failed shard reports why in Vitess's own words, and those words quote the
// statement that failed — which is a row out of the customer's table. None of
// that text is rendered. Vitess reports through its own API rather than as a Go
// error, so the message goes to mysqlerr as text: it is read only for the error
// code it labels, and the code selects a sentence SchemaBot wrote.

// adoptedFailureReason returns the reason a progress result should record, or
// empty when this poll must not record one. A shard message is the apply's
// failure only once the deploy request reports failure too: a shard that
// stopped while the change is still live is Vitess's to retry, and promoting
// its message would report a failure the apply has not had.
func adoptedFailureReason(engineState engine.State, failed vitessMigrationRow, shardFailed bool) string {
	if engineState != engine.StateFailed || !shardFailed {
		return ""
	}
	return mysqlerr.ReasonFromText(failed.Message)
}

// maxLoggedTargetMessageLen bounds the raw message written to the log. The
// reason sits at the front of it and the quoted statement that follows is
// unbounded — a wide table makes one failure write a log line measured in
// kilobytes — so the bound costs no triage detail.
const maxLoggedTargetMessageLen = 2000

// clampTargetMessage bounds the raw message for the server log. Unlike the
// rendered reason this keeps the target's own words: the log is where they are
// allowed to be, and where an operator goes to find them.
func clampTargetMessage(msg string) string {
	runes := []rune(msg)
	if len(runes) <= maxLoggedTargetMessageLen {
		return msg
	}
	return string(runes[:maxLoggedTargetMessageLen]) + "… (truncated)"
}
