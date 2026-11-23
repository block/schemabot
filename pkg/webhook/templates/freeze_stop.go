package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
)

// maxStoppedByFreezeReason bounds the freeze reason a stopped comment quotes.
const maxStoppedByFreezeReason = 150

// StoppedByFreezeNote is what a stopped comment says when a freeze with
// running "stop" stopped the apply (AZ-11), so nobody reads the stop as an
// operator's, or starts the apply again expecting the freeze to let it
// through. Once the freeze has ended, it says a plain start resumes the
// apply. It is empty when no freeze stopped it. The reason is a person's
// text, so it is redacted and escaped, and a mention in it notifies nobody.
// StoppedByFreezeText in the CLI templates tells status the same.
func StoppedByFreezeNote(freeze *apitypes.FreezeStopRef) string {
	if freeze == nil || freeze.ID == "" {
		return ""
	}
	note := "Stopped by freeze " + inlineCode(freeze.ID)
	if reason := RedactedReason(freeze.Reason, maxStoppedByFreezeReason); reason != "" {
		note += " (" + markdownReason(reason) + ")"
	}
	if freeze.Ended {
		return note + ", which has ended. Start it again to resume it."
	}
	return note + ". Start it again after the freeze ends or is lifted. " +
		"To start it while the freeze is in effect, add `--override` and your reason to the start command, then have a freeze approver approve the request."
}

// writeStoppedByFreeze writes StoppedByFreezeNote above a stopped apply's
// footer.
func writeStoppedByFreeze(sb *strings.Builder, freeze *apitypes.FreezeStopRef) {
	if note := StoppedByFreezeNote(freeze); note != "" {
		fmt.Fprintf(sb, "\n> ⏹️ %s\n", note)
	}
}
