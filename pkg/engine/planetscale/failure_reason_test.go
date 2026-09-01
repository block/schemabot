package planetscale

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// The message a failed shard reports quotes the statement that failed, so it
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

func TestVitessFailureReason(t *testing.T) {
	t.Run("reports what a NOT NULL failure means and what to do", func(t *testing.T) {
		got := vitessFailureReason(notNullFailure)
		assert.Equal(t, vitessFailureReasons[1048]+" (error 1048)", got)
		assert.NotContains(t, got, "ent_abc123")
		assert.NotContains(t, got, "cst_xyz789")
		assert.NotContains(t, got, "lending_product")
	})

	t.Run("a value in the reason itself is not carried over", func(t *testing.T) {
		got := vitessFailureReason(duplicateFailure)
		assert.Equal(t, vitessFailureReasons[1062]+" (error 1062)", got)
		assert.NotContains(t, got, "cst_xyz789")
	})

	t.Run("an unrecognized code reports the code alone", func(t *testing.T) {
		got := vitessFailureReason("Error 1317 (70100): Query execution was interrupted")
		assert.Equal(t, genericVitessFailure+" (error 1317)", got)
		assert.NotContains(t, got, "interrupted")
	})

	t.Run("no code at all falls back", func(t *testing.T) {
		assert.Equal(t, genericVitessFailure, vitessFailureReason("vreplication stopped: unrecoverable"))
		assert.Equal(t, genericVitessFailure, vitessFailureReason(""))
	})

	// Vitess wraps a tablet error in a generic one of its own, so the outer code
	// says nothing and the inner one says everything.
	t.Run("prefers the informative code over the wrapper", func(t *testing.T) {
		wrapped := "Error 1105 (HY000): vttablet: rpc error: code = Unknown desc = " +
			"Column 'x' cannot be null (errno 1048)"
		assert.Equal(t, vitessFailureReasons[1048]+" (error 1048)", vitessFailureReason(wrapped))
	})

	// A four-digit number sitting in the quoted row data is not a code, and
	// reporting one would be reporting a customer value.
	t.Run("digits in row data are not read as a code", func(t *testing.T) {
		assert.Equal(t, genericVitessFailure, vitessFailureReason("failed during query: insert into `t` values ('1048', 2013)"))
	})

	t.Run("out-of-range numbers are not codes", func(t *testing.T) {
		assert.Equal(t, genericVitessFailure, vitessFailureReason("error 9999 and errno 0042"))
	})
}

// The property that has to survive a target that changes its wording: whatever
// the message says, the output is one of the sentences this package wrote. The
// blocklist arrangement this replaced could only promise that for the shapes it
// had already seen.
func FuzzVitessFailureReason(f *testing.F) {
	f.Add(notNullFailure)
	f.Add(duplicateFailure)
	f.Add("")
	f.Add("Error 1105 (HY000): vttablet: rpc error: code = Unknown desc = Column 'x' cannot be null (errno 1048)")
	f.Add("errno 1062 | during query: insert into `t` values ('a\nb')")
	f.Add("ERROR=3819 check constraint 'c' is violated.")

	authored := authoredReasons()
	codeSuffix := regexp.MustCompile(` \(error [0-9]{4}\)$`)

	f.Fuzz(func(t *testing.T, msg string) {
		got := vitessFailureReason(msg)
		sentence := codeSuffix.ReplaceAllString(got, "")
		require.True(t, authored[sentence],
			"rendered a sentence this package did not write: %q (from %q)", got, msg)
	})
}

// Every sentence has to survive being dropped into a table cell in a pull
// request comment, so none of them may carry a newline or the cell separator.
func TestAuthoredReasonsAreRenderSafe(t *testing.T) {
	const maxReasonLen = 200
	for sentence := range authoredReasons() {
		assert.NotContains(t, sentence, "|", "cell separator in %q", sentence)
		assert.NotContains(t, sentence, "\n", "newline in %q", sentence)
		assert.NotContains(t, sentence, "\r", "carriage return in %q", sentence)
		assert.LessOrEqual(t, len([]rune(sentence)), maxReasonLen, "too long to render: %q", sentence)
	}
}

// SchemaBot's terminology rule reaches operator-facing text: these sentences are
// the words an operator reads when their change fails.
func TestAuthoredReasonsUseSchemaChangeTerminology(t *testing.T) {
	for sentence := range authoredReasons() {
		assert.NotContains(t, strings.ToLower(sentence), "migration", "in %q", sentence)
	}
}

func authoredReasons() map[string]bool {
	all := map[string]bool{genericVitessFailure: true}
	for _, reason := range vitessFailureReasons {
		all[reason] = true
	}
	return all
}

func TestFailedShard(t *testing.T) {
	t.Run("reports the shard that stopped", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: notNullFailure},
		}

		failed, ok := failedShard(rows)
		require.True(t, ok)
		assert.Equal(t, "-80", failed.Shard)
		assert.Equal(t, notNullFailure, failed.Message, "the raw message is kept for the server log")
	})

	// A running shard carries a message too — a throttle note, a retry — and
	// none of them mean the schema change stopped.
	t.Run("no failed shard", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Running, Message: "throttled"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Complete, Message: "done"},
		}
		_, ok := failedShard(rows)
		assert.False(t, ok)
	})

	t.Run("one shard failed among healthy shards", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Running, Message: "throttled"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Failed, Message: notNullFailure},
		}

		failed, ok := failedShard(rows)
		require.True(t, ok)
		assert.Equal(t, "80-", failed.Shard)
	})

	t.Run("failed shard Vitess left no message for", func(t *testing.T) {
		rows := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "   "},
		}
		_, ok := failedShard(rows)
		assert.False(t, ok)
	})

	// Shards report in whatever order the keyspaces answer, so an unstable pick
	// would make successive polls rewrite the pull request with a different
	// reason for the same failure.
	t.Run("picks the same shard whatever order the rows arrive in", func(t *testing.T) {
		forward := []vitessMigrationRow{
			{Keyspace: "commerce", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "first"},
			{Keyspace: "commerce", Shard: "80-", Table: "orders", Status: state.Vitess.Failed, Message: "second"},
			{Keyspace: "zebra", Shard: "-80", Table: "orders", Status: state.Vitess.Failed, Message: "third"},
		}
		reversed := []vitessMigrationRow{forward[2], forward[1], forward[0]}

		first, ok := failedShard(forward)
		require.True(t, ok)
		assert.Equal(t, "first", first.Message)

		again, ok := failedShard(reversed)
		require.True(t, ok)
		assert.Equal(t, "first", again.Message)
	})
}

// Guards the shape the fuzz property depends on: every rendered reason ends in
// its code, so stripping the code leaves an authored sentence.
func TestEveryKnownReasonRendersWithItsCode(t *testing.T) {
	for code, reason := range vitessFailureReasons {
		got := vitessFailureReason(fmt.Sprintf("vttablet: something went wrong (errno %d)", code))
		assert.Equal(t, fmt.Sprintf("%s (error %d)", reason, code), got)
	}
}
