package storage

import (
	"strings"
	"unicode/utf8"
)

// MaxStoredMessageBytes is the width, in bytes, of the text columns that hold
// a failure message or an apply log line: the most a MySQL text column holds.
// PostgreSQL's text holds more, and is held to the same width so a message
// reads the same on every dialect.
const MaxStoredMessageBytes = 65535

// StoredMessage returns msg as storage records it in a failure message or
// apply log column: any bytes that are not valid UTF-8 replaced with U+FFFD,
// then cut to MaxStoredMessageBytes on a character boundary, with an ellipsis
// marking the cut. A message carries engine and database error text that has
// no bound of its own and can quote binary key values. Written as it is, one
// would refuse every write that records it, and a refused failure verdict
// leaves the apply active through every claim that retries the same write.
//
// Storage keeps only this form, so code that compares a stored message with
// one it holds in memory compares the stored value with StoredMessage of its
// own. Applying StoredMessage to its own result returns that result unchanged.
func StoredMessage(msg string) string {
	// Replacement comes first because it can make the message longer: one
	// invalid byte becomes the three bytes of U+FFFD.
	if !utf8.ValidString(msg) {
		msg = strings.ToValidUTF8(msg, string(utf8.RuneError))
	}
	if len(msg) <= MaxStoredMessageBytes {
		return msg
	}
	const marker = "…"
	cut := MaxStoredMessageBytes - len(marker)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + marker
}
