package state

// Comment holds the comment state constants for PR comment tracking.
var Comment = struct {
	Progress string
	Cutover  string
	Summary  string
}{
	Progress: "progress",
	Cutover:  "cutover",
	Summary:  "summary",
}
