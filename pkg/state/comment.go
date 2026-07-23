package state

// Comment holds the comment state constants for PR comment tracking.
var Comment = struct {
	Progress string
	Cutover  string
	// CutoverAck tracks the acknowledgement posted when an operator's cutover
	// command records durable cutover intent, so the ack can be folded once
	// the outcome lands.
	CutoverAck string
	Summary    string
}{
	Progress:   "progress",
	Cutover:    "cutover",
	CutoverAck: "cutover_ack",
	Summary:    "summary",
}
