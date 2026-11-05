package livee2e

import (
	"regexp"
	"strings"
)

// Comment is one pull request conversation comment.
type Comment struct {
	ID     int64
	Author string
	IsBot  bool
	Body   string
}

// Status labels as the apply status comment renders them. The runner compares
// against these, and the tests render the real comment templates for each
// apply state so a template change that renames a label fails CI instead of a
// live run.
const (
	StatusQueued            = "Queued"
	StatusInProgress        = "In Progress"
	StatusResuming          = "Resuming"
	StatusWaitingForCutover = "Waiting for Cutover"
	StatusCuttingOver       = "Cutting Over"
	StatusApplied           = "Applied"
	StatusStopped           = "Stopped"
	StatusCancelled         = "Cancelled"
	StatusFailed            = "Failed"
)

var (
	applyIDLine = regexp.MustCompile("\\*\\*Apply ID\\*\\*: `(apply-[0-9a-f]+)`")
	statusLine  = regexp.MustCompile(`(?m)^\*\*Status\*\*: (.+)$`)
)

// statusHeadings are the titles of the apply status comment SchemaBot edits
// in place. Summary comments repeat the bold Apply ID line under their own
// titles, so the title is what tells a status comment apart.
var statusHeadings = []string{"Schema Change Status", "Rollback Status"}

// ApplyStatus is the state one apply status comment shows.
type ApplyStatus struct {
	ApplyID string
	Status  string
}

// ParseApplyStatus reads the apply ID and status label from an apply status
// comment. It reports false for any other comment, including the terminal
// summary comments, which name the apply in a different form. Status is empty
// when the comment names the apply but shows no status yet, as the first body
// of a progress comment does before its first update.
func ParseApplyStatus(body string) (ApplyStatus, bool) {
	if !isStatusComment(body) {
		return ApplyStatus{}, false
	}
	id := applyIDLine.FindStringSubmatch(body)
	if id == nil {
		return ApplyStatus{}, false
	}
	status := statusLine.FindStringSubmatch(body)
	if status == nil {
		return ApplyStatus{ApplyID: id[1]}, true
	}
	// A revert window appends its countdown after " | ", and a sharded apply
	// appends its change count after " — "; the label is the part before.
	label, _, _ := strings.Cut(status[1], " | ")
	label, _, _ = strings.Cut(label, " — ")
	return ApplyStatus{ApplyID: id[1], Status: strings.TrimSpace(label)}, true
}

func isStatusComment(body string) bool {
	headline := firstLine(body)
	if !strings.HasPrefix(headline, "## ") {
		return false
	}
	for _, h := range statusHeadings {
		if strings.Contains(headline, h) {
			return true
		}
	}
	return false
}

// latestApplyStatus returns the newest status the bot's comments show for
// applyID. Control operations rotate the progress comment, so the comment with
// the highest ID is the current one; superseded comments keep their last
// status.
func latestApplyStatus(comments []Comment, applyID string) (ApplyStatus, bool) {
	var latest ApplyStatus
	var latestID int64
	found := false
	for _, c := range comments {
		s, ok := ParseApplyStatus(c.Body)
		if !ok || s.ApplyID != applyID || s.Status == "" {
			continue
		}
		if !found || c.ID > latestID {
			latest, latestID, found = s, c.ID, true
		}
	}
	return latest, found
}

// firstApplyAfter returns the first apply status comment posted after the
// comment with ID after: the apply a command started.
func firstApplyAfter(comments []Comment, after int64) (ApplyStatus, bool) {
	var first ApplyStatus
	var firstID int64
	found := false
	for _, c := range comments {
		if c.ID <= after {
			continue
		}
		s, ok := ParseApplyStatus(c.Body)
		if !ok {
			continue
		}
		if !found || c.ID < firstID {
			first, firstID, found = s, c.ID, true
		}
	}
	return first, found
}

// blockedHeadline starts the reply SchemaBot posts when another pull request
// or a CLI session holds the database's lock. Other refusals, such as failing
// checks, share "Apply Blocked" without the lock glyph.
const blockedHeadline = "🔒 Apply Blocked"

// blockedAfter reports whether SchemaBot refused a command posted as comment
// after because something else holds the database's lock.
func blockedAfter(comments []Comment, after int64) bool {
	for _, c := range comments {
		if c.ID > after && strings.Contains(firstLine(c.Body), blockedHeadline) {
			return true
		}
	}
	return false
}

func firstLine(body string) string {
	line, _, _ := strings.Cut(body, "\n")
	return line
}

// summaryApplyIDLine names the apply in a completed apply's summary comment.
var summaryApplyIDLine = regexp.MustCompile("_Apply ID: `(apply-[0-9a-f]+)`_")

// finalSummaryTitles maps the title of the summary comment SchemaBot posts when
// an apply finishes to the status it reports. A reverted apply's title takes
// either form, depending on which comment announces it. A stopped apply's
// summary is left out: a start can resume the apply after it.
var finalSummaryTitles = map[string]string{
	"Schema Change Applied":   StatusApplied,
	"Schema Change Failed":    StatusFailed,
	"Schema Change Cancelled": StatusCancelled,
	"Schema Change Reverted":  "Reverted",
	"Schema Change: Reverted": "Reverted",
}

// parseFinalSummary reads the apply ID and final status from the summary
// comment SchemaBot posts when an apply finishes. It reports false for any
// other comment.
func parseFinalSummary(body string) (ApplyStatus, bool) {
	headline := firstLine(body)
	if !strings.HasPrefix(headline, "## ") {
		return ApplyStatus{}, false
	}
	var label string
	for title, l := range finalSummaryTitles {
		if strings.Contains(headline, title) {
			label = l
			break
		}
	}
	if label == "" {
		return ApplyStatus{}, false
	}
	id := summaryApplyIDLine.FindStringSubmatch(body)
	if id == nil {
		id = applyIDLine.FindStringSubmatch(body)
	}
	if id == nil {
		return ApplyStatus{}, false
	}
	return ApplyStatus{ApplyID: id[1], Status: label}, true
}

// finishedStatus returns the final status the bot's comments show for applyID:
// a final summary comment, or a status comment showing a terminal status.
func finishedStatus(comments []Comment, applyID string) (string, bool) {
	for _, c := range comments {
		if s, ok := parseFinalSummary(c.Body); ok && s.ApplyID == applyID {
			return s.Status, true
		}
	}
	if s, ok := latestApplyStatus(comments, applyID); ok && isTerminalStatus(s.Status) {
		return s.Status, true
	}
	return "", false
}

// alreadyFinishedReply matches SchemaBot's refusal of a stop or cancel for an
// apply that has already finished, and captures the state it finished in.
var alreadyFinishedReply = regexp.MustCompile(`is already terminal \(current state: ([a-z_]+)\)`)

// startRefusals maps SchemaBot's refusal of a start for a finished apply,
// which words each state differently, to the state it names.
var startRefusals = map[string]string{
	"schema change already completed and cannot be started": "completed",
	"schema change failed and cannot be started":            "failed",
	"schema change was cancelled and cannot be started":     "cancelled",
	"schema change was reverted and cannot be started":      "reverted",
}

// refusedAsFinished returns the state SchemaBot named when it refused the
// command posted as comment after because the apply had already finished.
func refusedAsFinished(comments []Comment, after int64) (string, bool) {
	for _, c := range comments {
		if c.ID <= after {
			continue
		}
		if m := alreadyFinishedReply.FindStringSubmatch(c.Body); m != nil {
			return m[1], true
		}
		for refusal, finished := range startRefusals {
			if strings.Contains(c.Body, refusal) {
				return finished, true
			}
		}
	}
	return "", false
}

// inFlightApplies returns the applies that have not finished, so the sweep
// cancels them before it closes their pull request. An apply whose comments
// show no status yet counts as in flight.
func inFlightApplies(comments []Comment) []string {
	seen := map[string]bool{}
	var order []string
	for _, c := range comments {
		s, ok := ParseApplyStatus(c.Body)
		if !ok || seen[s.ApplyID] {
			continue
		}
		seen[s.ApplyID] = true
		order = append(order, s.ApplyID)
	}
	var ids []string
	for _, id := range order {
		if _, finished := finishedStatus(comments, id); !finished {
			ids = append(ids, id)
		}
	}
	return ids
}

// isTerminalStatus reports whether an apply showing this label is finished.
// Stopped is not: a stopped apply keeps its lock and checkpoint until someone
// starts or cancels it, so the sweep cancels it like a running one.
func isTerminalStatus(label string) bool {
	switch label {
	case StatusApplied, StatusCancelled, StatusFailed, "Reverted", "Rolled Back":
		return true
	}
	return false
}

// applyRejections are headlines of SchemaBot's replies that refuse an apply
// command before it reaches the engine, so no apply can be running for it.
// A generic "Apply Failed" reply is left out: SchemaBot can post it after the
// engine accepted the apply.
var applyRejections = []string{
	"Apply Blocked",
	"Plan Failed",
	"Review Required",
	"Review Gate Error",
	"Command Not Authorized",
	"Authorization Check Failed",
	"Database Not Registered",
	"Databases Not Registered",
	"Database Not Configured",
	"Database Not Available to This Repository",
	"No Schema Files Changed",
	"Missing Environment",
}

func isApplyRejection(body string) bool {
	headline := firstLine(body)
	if !strings.HasPrefix(headline, "## ") {
		return false
	}
	for _, r := range applyRejections {
		if strings.Contains(headline, r) {
			return true
		}
	}
	return false
}

// unansweredApply reports whether the last apply command in comments has no
// answer from SchemaBot yet. isBot picks out SchemaBot's comments. An answer
// is a comment about an apply that started after the command, or a refusal
// of the command before it reached the engine. Any other comment, such as a
// late summary of an earlier apply or a plan, leaves the command unanswered,
// since an accepted apply may still be running without its progress comment.
// Only the last apply command can be unanswered: the runner waits for each
// apply to start before it posts the next command.
func unansweredApply(comments []Comment, isBot func(Comment) bool, commandPrefix string) bool {
	var command int64
	for _, c := range comments {
		fields := strings.Fields(c.Body)
		if !isBot(c) && len(fields) >= 2 && fields[0] == commandPrefix && fields[1] == "apply" && c.ID > command {
			command = c.ID
		}
	}
	if command == 0 {
		return false
	}
	earlier := map[string]bool{}
	for _, c := range comments {
		if !isBot(c) || c.ID >= command {
			continue
		}
		if s, ok := ParseApplyStatus(c.Body); ok {
			earlier[s.ApplyID] = true
		}
	}
	for _, c := range comments {
		if !isBot(c) || c.ID <= command {
			continue
		}
		if isApplyRejection(c.Body) {
			return false
		}
		s, ok := ParseApplyStatus(c.Body)
		if !ok {
			s, ok = parseFinalSummary(c.Body)
		}
		if ok && !earlier[s.ApplyID] {
			return false
		}
	}
	return true
}
