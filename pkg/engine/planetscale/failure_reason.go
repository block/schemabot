package planetscale

import (
	"fmt"
	"regexp"
	"strconv"
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
// and 3000 up for codes added since. Bounding the accepted range keeps a
// four-digit number that happens to sit after the word "error" from being
// reported as a code.
const (
	minMySQLErrorCode = 1000
	maxMySQLErrorCode = 4999
)

// vitessErrorCodePattern matches the ways the target labels an error code:
// the Go MySQL driver writes "Error 1048 (23000):" and Vitess writes
// "(errno 1048)". Requiring one of those labels is what separates a code from
// a four-digit value inside the quoted statement.
var vitessErrorCodePattern = regexp.MustCompile(`(?i)\b(?:errno|error)[ :=]+([0-9]{4})\b`)

// vitessErrorCode extracts the error code to look up. Vitess wraps a tablet's
// error in one of its own, so a message often carries two codes — a generic
// outer one and the specific inner one that says what actually went wrong.
// Preferring a code this package recognizes picks the informative one without
// depending on where in the message it sits.
func vitessErrorCode(msg string) (int, bool) {
	first := 0
	for _, m := range vitessErrorCodePattern.FindAllStringSubmatch(msg, -1) {
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
