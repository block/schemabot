package planetscale

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/block/schemabot/pkg/engine"
)

// A failed shard reports why in Vitess's own words, and those words quote the
// statement that failed — which is a row out of the customer's table. None of
// that text is rendered. The message is read only as a lookup key: a numeric
// error code selects a sentence SchemaBot wrote, and that sentence plus the
// code is the entire output. Nothing the target says reaches a pull request,
// whatever the target decides to say.
//
// The cost of reading the message this way lands on the input side, where being
// wrong is inert. A reworded message, an unfamiliar code, or a changed code
// format all miss the lookup and fall back to the generic sentence: less
// helpful, never unsafe. Scrubbing the message and rendering the remainder
// fails the other way, publishing anything it did not anticipate.

// genericVitessFailure is what an unrecognized failure reports. It says where
// the reason is rather than guessing at it: the raw target message is logged
// server-side at the point this is chosen.
const genericVitessFailure = "The schema change failed on the target; see the server logs for the reason."

// vitessFailureReasons maps a MySQL error code to SchemaBot's account of what
// it means for a schema change and what to do next. Row-copy failures dominate:
// the DDL is legal, and it is the data already in the table that cannot satisfy
// it.
//
// A code missing from this map is not a gap to fill on sight. Each entry is a
// claim about what an operator should do, and a confidently wrong claim during
// an incident costs more than the generic sentence does.
var vitessFailureReasons = map[int]string{
	1048: "A row held NULL in a column that cannot be null. When a change adds NOT NULL, backfill the existing NULLs before applying it.",
	1062: "Existing rows hold duplicate values for a unique key. Resolve the duplicates before applying a change that adds or narrows that key.",
	1264: "An existing value is out of range for the column's target type. Widen the type or correct the data before applying the change.",
	1292: "An existing value would be truncated by the column's target type. Correct the data or choose a compatible type before applying the change.",
	1366: "An existing value cannot be stored in the column's target type. Correct the data or choose a compatible type before applying the change.",
	1406: "Existing data is too long for the column's target type. Widen the type or shorten the data before applying the change.",
	3819: "Existing rows violate a check constraint. Correct the data before applying a change that adds the constraint.",
	1451: "A foreign key constraint blocked the change: child rows still reference a row it would remove.",
	1452: "A foreign key constraint blocked the change: a row references a parent row that does not exist.",
	1050: "The table already exists on the target.",
	1146: "The table does not exist on the target.",
	1091: "The column, index, or constraint does not exist on the target.",
	1205: "The change timed out waiting for a lock on the target. Retrying when the table is quieter usually clears it.",
	1213: "The target rolled the change back after a deadlock. Retrying usually clears it.",
}

// MySQL error codes occupy 1000-1999 for the server, 2000-2999 for the client,
// and 3000 up for codes added since. The bound is only a plausibility filter —
// it rejects numbers outside that range, but an in-range number after the
// right label still parses. Separating codes from data is the label
// requirement's and the scan region's job.
const (
	minMySQLErrorCode = 1000
	maxMySQLErrorCode = 4999
)

// vitessErrorCodePattern matches the ways the target labels an error code:
// the Go MySQL driver writes "Error 1048 (23000):" and Vitess writes
// "(errno 1048)". Requiring one of those labels is what separates a code from
// a four-digit value inside the quoted statement.
var vitessErrorCodePattern = regexp.MustCompile(`(?i)\b(?:errno|error)[ :=]+([0-9]{4})\b`)

// vitessQuotedStatementMarker separates the target's reason from the failed
// statement it quotes after it. Codes sit in the reason; what follows the
// marker is customer row data, where a labelled number is just a value.
const vitessQuotedStatementMarker = "during query:"

// codeScanRegion returns the part of the message where a labelled number can
// be trusted as an error code: everything before the quoted statement. The
// marker is an input-side dependency of the kind the package comment
// describes — if Vitess rewords it, the scan covers the whole message again,
// which is less precise but degrades only toward the generic sentence's
// territory, never toward rendering target text.
func codeScanRegion(msg string) string {
	if before, _, ok := strings.Cut(msg, vitessQuotedStatementMarker); ok {
		return before
	}
	return msg
}

// vitessErrorCode extracts the error code to look up. Vitess wraps a tablet's
// error in one of its own, so the reason often carries two codes — a generic
// outer one and the specific inner one that says what actually went wrong.
// Preferring a code this package recognizes picks the informative one without
// depending on where in the reason it sits. The scan stays inside the reason
// region: prefer-known must never let a value quoted from the customer's own
// rows outrank the genuine code in front of it.
func vitessErrorCode(msg string) (int, bool) {
	first := 0
	for _, m := range vitessErrorCodePattern.FindAllStringSubmatch(codeScanRegion(msg), -1) {
		code, err := strconv.Atoi(m[1])
		if err != nil || code < minMySQLErrorCode || code > maxMySQLErrorCode {
			continue
		}
		if _, known := vitessFailureReasons[code]; known {
			return code, true
		}
		if first == 0 {
			first = code
		}
	}
	return first, first != 0
}

// adoptedFailureReason returns the reason a progress result should record, or
// empty when this poll must not record one. A shard message is the apply's
// failure only once the deploy request reports failure too: a shard that
// stopped while the change is still live is Vitess's to retry, and promoting
// its message would report a failure the apply has not had.
func adoptedFailureReason(engineState engine.State, failed vitessMigrationRow, shardFailed bool) string {
	if engineState != engine.StateFailed || !shardFailed {
		return ""
	}
	return vitessFailureReason(failed.Message)
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

// vitessFailureReason renders why a shard stopped, assembled entirely from
// text this package owns. providerMessage is consulted for its error code and
// is never carried into the result.
func vitessFailureReason(providerMessage string) string {
	code, ok := vitessErrorCode(providerMessage)
	if !ok {
		return genericVitessFailure
	}
	if reason, known := vitessFailureReasons[code]; known {
		return fmt.Sprintf("%s (error %d)", reason, code)
	}
	return fmt.Sprintf("%s (error %d)", genericVitessFailure, code)
}
