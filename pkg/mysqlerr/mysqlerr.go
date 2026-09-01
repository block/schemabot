// Package mysqlerr turns a failure from a MySQL-family target into a sentence
// an operator can read on a pull request.
//
// The text a target produces is never rendered. It quotes the statement that
// failed, so it carries rows out of the customer's table, and it names hosts,
// users and DSN fragments when a connection is what failed. Instead the error
// is read only for its numeric code, and the code selects a sentence SchemaBot
// wrote. Nothing the target says reaches a pull request, whatever the target
// decides to say.
//
// The cost of reading a failure this way lands on the input side, where being
// wrong is inert. An unfamiliar code, a reworded message, or a changed code
// format all miss the lookup and fall back to a generic sentence naming the
// server logs: less helpful, never unsafe. Scrubbing a message and rendering
// the remainder fails the other way, publishing anything it did not anticipate.
package mysqlerr

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Generic is what an unrecognized failure reports. It says where the reason is
// rather than guessing at it, so callers are expected to log the underlying
// error with the target identifiers.
const Generic = "The schema change failed on the target; see the server logs for the reason."

// Unreachable is what a failure to reach the target at all reports. It is kept
// apart from Generic because the two send an operator to different places: one
// is a schema change that ran and stopped, the other never got to run.
const Unreachable = "SchemaBot could not reach the target database; see the server logs for the connection error."

// reasons maps a MySQL error code to SchemaBot's account of what it means for a
// schema change and what to do next. Row-copy failures dominate: the DDL is
// legal, and it is the data already in the table that cannot satisfy it.
//
// A code missing from this map is not a gap to fill on sight. Each entry is a
// claim about what an operator should do, and a confidently wrong claim during
// an incident costs more than the generic sentence does.
var reasons = map[int]string{
	1048: "A row held NULL in a column that cannot be null. When a change adds NOT NULL, backfill the existing NULLs before applying it.",
	1062: "Existing rows hold duplicate values for a unique key. Resolve the duplicates before applying a change that adds or narrows that key.",
	1264: "An existing value is out of range for the column's target type. Widen the type or correct the data before applying the change.",
	1292: "An existing value would be truncated by the column's target type. Correct the data or choose a compatible type before applying the change.",
	1364: "A row had no value for a column that has no default. Give the column a default, or backfill the rows, before applying the change.",
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

	// Connection-family codes. Their messages are the ones that name hosts and
	// users, so they are mapped rather than rendered even though nothing about
	// them concerns a row.
	1045: "SchemaBot was refused access to the target database. Check the credentials configured for this deployment.",
	1049: "The target database does not exist.",
	2002: "SchemaBot could not connect to the target database.",
	2003: "SchemaBot could not connect to the target database.",
	2006: "The target closed the connection while the change was running.",
	2013: "The connection to the target was lost while the change was running.",
}

// MySQL error codes occupy 1000-1999 for the server, 2000-2999 for the client,
// and 3000 up for codes added since. The bound is only a plausibility filter —
// it rejects numbers outside that range, but an in-range number after the
// right label still parses. Separating codes from data is the label
// requirement's and the scan region's job.
const (
	minErrorCode = 1000
	maxErrorCode = 4999
)

// codePattern matches the ways a target labels an error code: the Go MySQL
// driver writes "Error 1048 (23000):" and Vitess writes "(errno 1048)".
// Requiring one of those labels is what separates a code from a four-digit
// value inside the quoted statement.
var codePattern = regexp.MustCompile(`(?i)\b(?:errno|error)[ :=]+([0-9]{4})\b`)

// quotedStatementMarker separates a target's reason from the failed statement
// it quotes after it. Codes sit in the reason; what follows the marker is
// customer row data, where a labelled number is just a value.
const quotedStatementMarker = "during query:"

// codeScanRegion returns the part of a message where a labelled number can be
// trusted as an error code: everything before the quoted statement. The marker
// is an input-side dependency of the kind the package comment describes — if a
// target rewords it, the scan covers the whole message again, which is less
// precise but degrades only toward the generic sentence's territory, never
// toward rendering target text.
func codeScanRegion(msg string) string {
	if before, _, ok := strings.Cut(msg, quotedStatementMarker); ok {
		return before
	}
	return msg
}

// Reason renders why a MySQL-family target failed, assembled entirely from text
// this package owns. err is read for its error code and is never carried into
// the result, so callers are free to pass an error wrapping anything.
func Reason(err error) string {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return render(int(mysqlErr.Number))
	}
	if unreachable(err) {
		return Unreachable
	}
	return Generic
}

// ReasonFromText renders why a target failed when the failure arrives as text
// rather than as a Go error — a provider that reports a message through its own
// API, such as Vitess through SHOW VITESS_MIGRATIONS.
func ReasonFromText(msg string) string {
	code, ok := codeFromText(msg)
	if !ok {
		return Generic
	}
	return render(code)
}

// unreachable reports whether the target was never reached, as opposed to
// reached and unable to run the change. These arrive as network and driver
// errors rather than as a MySQL error with a code.
func unreachable(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// codeFromText extracts the code to look up. A target wraps a lower-level error
// in one of its own, so a message often carries two codes: a generic outer one
// and the specific inner one that says what actually went wrong. Preferring a
// code this package recognizes picks the informative one without depending on
// where in the message it sits. The scan stays inside the reason region:
// prefer-known must never let a value quoted from the customer's own rows
// outrank the genuine code in front of it.
func codeFromText(msg string) (int, bool) {
	first := 0
	for _, m := range codePattern.FindAllStringSubmatch(codeScanRegion(msg), -1) {
		code, err := strconv.Atoi(m[1])
		if err != nil || code < minErrorCode || code > maxErrorCode {
			continue
		}
		if _, known := reasons[code]; known {
			return code, true
		}
		if first == 0 {
			first = code
		}
	}
	return first, first != 0
}

// render pairs a sentence with the code that chose it. The code is carried on
// every reason so an operator can search for it, and so stripping it leaves a
// sentence this package wrote.
//
// A number outside the MySQL range is reported without a code at all. Naming
// one would tell an operator to go looking for an error that does not exist.
func render(code int) string {
	if code < minErrorCode || code > maxErrorCode {
		return Generic
	}
	if reason, known := reasons[code]; known {
		return fmt.Sprintf("%s (error %d)", reason, code)
	}
	return fmt.Sprintf("%s (error %d)", Generic, code)
}

// Authored returns every sentence this package can render, so a test can assert
// that a rendered reason is one of them.
func Authored() map[string]bool {
	all := map[string]bool{Generic: true, Unreachable: true}
	for _, reason := range reasons {
		all[reason] = true
	}
	return all
}
