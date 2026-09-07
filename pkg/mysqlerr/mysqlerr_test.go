package mysqlerr

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What a target says about a failure quotes the statement that failed, so it
// carries a row out of the customer's table. What reaches the pull request is
// assembled from SchemaBot's own sentences instead, keyed off the error code.
const notNullFailure = "Error 1048 (23000): vttablet: rpc error: code = Unknown desc = " +
	"Column 'lending_product' cannot be null (errno 1048) (sqlstate 23000) " +
	"during query: insert into `_vt_vrp_related_external_entities` " +
	"(`entity_token`,`customer_id`) values ('ent_abc123', 'cst_xyz789')"

// A duplicate-entry failure puts the offending value in the reason itself,
// ahead of any quoted statement — the shape that makes reading the target's
// words unsafe no matter where the quoted statement begins.
const duplicateFailure = "Error 1062 (23000): vttablet: rpc error: code = Unknown desc = " +
	"Duplicate entry 'cst_xyz789' for key 'unq_entity_token_type' (errno 1062) (sqlstate 23000)"

func TestReasonFromText(t *testing.T) {
	t.Run("reports what a NOT NULL failure means and what to do", func(t *testing.T) {
		got := ReasonFromText(notNullFailure)
		assert.Equal(t, reasons[1048]+" (error 1048)", got)
		assert.NotContains(t, got, "ent_abc123")
		assert.NotContains(t, got, "cst_xyz789")
		assert.NotContains(t, got, "lending_product")
	})

	t.Run("a value in the reason itself is not carried over", func(t *testing.T) {
		got := ReasonFromText(duplicateFailure)
		assert.Equal(t, reasons[1062]+" (error 1062)", got)
		assert.NotContains(t, got, "cst_xyz789")
	})

	t.Run("an unrecognized code reports the code alone", func(t *testing.T) {
		got := ReasonFromText("Error 1317 (70100): Query execution was interrupted")
		assert.Equal(t, Generic+" (error 1317)", got)
		assert.NotContains(t, got, "interrupted")
	})

	t.Run("no code at all falls back", func(t *testing.T) {
		assert.Equal(t, Generic, ReasonFromText("vreplication stopped: unrecoverable"))
		assert.Equal(t, Generic, ReasonFromText(""))
	})

	// A target wraps a tablet error in a generic one of its own, so the outer
	// code says nothing and the inner one says everything.
	t.Run("prefers the informative code over the wrapper", func(t *testing.T) {
		wrapped := "Error 1105 (HY000): vttablet: rpc error: code = Unknown desc = " +
			"Column 'x' cannot be null (errno 1048)"
		assert.Equal(t, reasons[1048]+" (error 1048)", ReasonFromText(wrapped))
	})

	// A four-digit number sitting in the quoted row data is not a code, and
	// reporting one would be reporting a customer value.
	t.Run("digits in row data are not read as a code", func(t *testing.T) {
		assert.Equal(t, Generic, ReasonFromText("failed during query: insert into `t` values ('1048', 2013)"))
	})

	t.Run("out-of-range numbers are not codes", func(t *testing.T) {
		assert.Equal(t, Generic, ReasonFromText("error 9999 and errno 0042"))
	})

	// Preferring a recognized code has to stop at the quoted statement: a
	// labelled number inside it is a customer value, not a code. An unmapped
	// genuine failure must fall back to the generic sentence rather than adopt
	// whatever the row data happens to say.
	t.Run("a mapped code quoted in row data cannot outrank the genuine code", func(t *testing.T) {
		msg := "Duplicate column name 'x' (errno 1060) during query: " +
			"insert into `t` (`note`) values ('error 1062 seen in prod')"
		assert.Equal(t, Generic+" (error 1060)", ReasonFromText(msg))
	})

	t.Run("a mapped code in the reason wins whatever the row data says", func(t *testing.T) {
		msg := "Duplicate entry 'a' for key 'k' (errno 1062) during query: " +
			"insert into `t` values ('errno 1048')"
		assert.Equal(t, reasons[1062]+" (error 1062)", ReasonFromText(msg))
	})
}

// A Go error carries its code structurally, so nothing has to be read out of
// the message at all — and the message is what names hosts, users and rows.
func TestReason(t *testing.T) {
	t.Run("reads the code off a driver error", func(t *testing.T) {
		err := fmt.Errorf("run ALTER on table %q: %w", "orders", &mysql.MySQLError{
			Number:  1048,
			Message: "Column 'lending_product' cannot be null",
		})
		got := Reason(err)
		assert.Equal(t, reasons[1048]+" (error 1048)", got)
		assert.NotContains(t, got, "lending_product")
	})

	// Access-denied names the user and the host it was refused from, which is
	// the deployment's own topology.
	t.Run("a refused connection does not report who was refused", func(t *testing.T) {
		err := &mysql.MySQLError{
			Number:  1045,
			Message: "Access denied for user 'schemabot'@'10.4.2.19' (using password: YES)",
		}
		got := Reason(err)
		assert.Equal(t, reasons[1045]+" (error 1045)", got)
		assert.NotContains(t, got, "10.4.2.19")
		assert.NotContains(t, got, "schemabot")
	})

	t.Run("an unrecognized code reports the code alone", func(t *testing.T) {
		err := &mysql.MySQLError{Number: 1317, Message: "Query execution was interrupted"}
		assert.Equal(t, Generic+" (error 1317)", Reason(err))
	})

	// A dial failure names the host and port SchemaBot connects to, and it also
	// means the change never started — which is a different thing to tell an
	// operator than a change that ran and stopped.
	t.Run("a target that was never reached says so", func(t *testing.T) {
		err := fmt.Errorf("connect for direct execution: %w", &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: errors.New("connect: connection refused"),
		})
		got := Reason(err)
		assert.Equal(t, Unreachable, got)
		assert.NotContains(t, got, "dial")
	})

	// A connection that existed and broke is a different thing to tell an
	// operator than one that was never established: the change ran, so there is
	// a partial copy on the target to account for.
	t.Run("a connection lost mid-change says so", func(t *testing.T) {
		assert.Equal(t, ConnectionLost, Reason(fmt.Errorf("read result: %w", io.EOF)))
		assert.Equal(t, ConnectionLost, Reason(fmt.Errorf("exec: %w", mysql.ErrInvalidConn)))
		assert.Equal(t, ConnectionLost, Reason(fmt.Errorf("copy chunk: %w", driver.ErrBadConn)))
	})

	// An exceeded deadline satisfies net.Error, and a context that fires while a
	// query is in flight also invalidates that connection. Either would report a
	// target that could not be reached — so an engine-internal chunk or checksum
	// budget running out would send an operator after a connection that was
	// working the whole time.
	t.Run("an exceeded deadline is not reported as a connection problem", func(t *testing.T) {
		got := Reason(fmt.Errorf("fix chunk: %w", context.DeadlineExceeded))
		assert.Equal(t, Timeout, got)
		assert.NotEqual(t, Unreachable, got)
		assert.NotEqual(t, ConnectionLost, got)
	})

	// A deadline that kills a query in flight surfaces both the context error
	// and the broken connection it was using. The deadline is the cause.
	t.Run("a deadline that also breaks the connection reports the deadline", func(t *testing.T) {
		err := fmt.Errorf("copy rows: %w", fmt.Errorf("%w: %w", context.DeadlineExceeded, mysql.ErrInvalidConn))
		assert.Equal(t, Timeout, Reason(err))
	})

	// A cancelled change carries its outcome in its state, so the reason has
	// nothing to add beyond where to read the cause.
	t.Run("a cancelled change falls back", func(t *testing.T) {
		assert.Equal(t, Generic, Reason(fmt.Errorf("run: %w", context.Canceled)))
	})

	t.Run("an error with nothing to key on falls back", func(t *testing.T) {
		assert.Equal(t, Generic, Reason(errors.New("checkpoint watermark is not resumable")))
		assert.Equal(t, Generic, Reason(nil))
	})

	// Naming a code outside the MySQL range would send an operator looking for
	// an error that does not exist.
	t.Run("a number that is not a MySQL code is not reported as one", func(t *testing.T) {
		assert.Equal(t, Generic, Reason(&mysql.MySQLError{Number: 995, Message: "boom"}))
		assert.Equal(t, Generic, Reason(&mysql.MySQLError{Number: 0, Message: "boom"}))
	})
}

// The property that has to survive a target that changes its wording: whatever
// the message says, the output is one of the sentences this package wrote.
// It names no message format, so it keeps holding as formats change.
func FuzzReasonFromText(f *testing.F) {
	f.Add(notNullFailure)
	f.Add(duplicateFailure)
	f.Add("")
	f.Add("Error 1105 (HY000): vttablet: rpc error: code = Unknown desc = Column 'x' cannot be null (errno 1048)")
	f.Add("errno 1062 | during query: insert into `t` values ('a\nb')")
	f.Add("Duplicate column name 'x' (errno 1060) during query: insert into `t` (`note`) values ('error 1062 seen in prod')")
	f.Add("ERROR=3819 check constraint 'c' is violated.")

	authored := Authored()
	codeSuffix := regexp.MustCompile(` \(error [0-9]{4}\)$`)

	f.Fuzz(func(t *testing.T, msg string) {
		got := ReasonFromText(msg)
		sentence := codeSuffix.ReplaceAllString(got, "")
		require.True(t, authored[sentence],
			"rendered a sentence this package did not write: %q (from %q)", got, msg)
	})
}

// The same property for a Go error, where the message an engine wrapped along
// the way is every bit as untrusted as the target's own.
func FuzzReason(f *testing.F) {
	f.Add(notNullFailure, uint16(0))
	f.Add(duplicateFailure, uint16(1062))
	f.Add("", uint16(1048))
	f.Add("Access denied for user 'schemabot'@'10.4.2.19'", uint16(1045))

	authored := Authored()
	codeSuffix := regexp.MustCompile(` \(error [0-9]{4}\)$`)

	f.Fuzz(func(t *testing.T, msg string, code uint16) {
		err := error(errors.New(msg))
		if code != 0 {
			err = fmt.Errorf("%s: %w", msg, &mysql.MySQLError{Number: code, Message: msg})
		}
		got := Reason(err)
		sentence := codeSuffix.ReplaceAllString(got, "")
		require.True(t, authored[sentence],
			"rendered a sentence this package did not write: %q (from %q/%d)", got, msg, code)
	})
}

// The four sentences a failure with no usable code can reach each send an
// operator somewhere different: a partial copy to account for, a connection
// that never opened, a budget that ran out, or the logs. Collapsing any two of
// them would report one of those as another.
func TestCauseSentencesAreDistinctAndAuthored(t *testing.T) {
	authored := Authored()
	seen := map[string]string{}
	for name, sentence := range map[string]string{
		"Generic":        Generic,
		"Unreachable":    Unreachable,
		"ConnectionLost": ConnectionLost,
		"Timeout":        Timeout,
	} {
		assert.True(t, authored[sentence], "%s is not in Authored()", name)
		if prior, dup := seen[sentence]; dup {
			t.Errorf("%s and %s render the same sentence: %q", prior, name, sentence)
		}
		seen[sentence] = name
	}
}

// Every sentence has to survive being dropped into a table cell in a pull
// request comment, so none of them may carry a newline or the cell separator.
func TestAuthoredReasonsAreRenderSafe(t *testing.T) {
	const maxReasonLen = 200
	for sentence := range Authored() {
		assert.NotContains(t, sentence, "|", "cell separator in %q", sentence)
		assert.NotContains(t, sentence, "\n", "newline in %q", sentence)
		assert.NotContains(t, sentence, "\r", "carriage return in %q", sentence)
		assert.LessOrEqual(t, len([]rune(sentence)), maxReasonLen, "too long to render: %q", sentence)
	}
}

// SchemaBot's terminology rule reaches operator-facing text: these sentences are
// the words an operator reads when their change fails.
func TestAuthoredReasonsUseSchemaChangeTerminology(t *testing.T) {
	for sentence := range Authored() {
		assert.NotContains(t, strings.ToLower(sentence), "migration", "in %q", sentence)
	}
}

// Guards the shape the fuzz property depends on: every rendered reason ends in
// its code, so stripping the code leaves an authored sentence.
func TestEveryKnownReasonRendersWithItsCode(t *testing.T) {
	for code, reason := range reasons {
		want := fmt.Sprintf("%s (error %d)", reason, code)
		assert.Equal(t, want, ReasonFromText(fmt.Sprintf("something went wrong (errno %d)", code)))
		assert.Equal(t, want, Reason(&mysql.MySQLError{Number: uint16(code), Message: "something went wrong"}))
	}
}
