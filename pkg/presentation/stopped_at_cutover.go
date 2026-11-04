package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/state"
)

// StoppedAtCutoverLabel is the status word for a member SchemaBot stopped at
// a deferred cutover once another member's failure meant the cutover could
// never run.
const StoppedAtCutoverLabel = "stopped at cutover"

// OperationRef names one operation row of an apply: a deployment and an
// operation key identify exactly one row.
type OperationRef struct {
	Deployment   string
	OperationKey string
}

// stoppedAtCutoverLabel is the label of a member stopped at the cutover,
// naming the member whose failure stopped it when that member is known.
func stoppedAtCutoverLabel(endedBy string) string {
	if endedBy == "" {
		return StoppedAtCutoverLabel + " — copy saved"
	}
	return fmt.Sprintf("%s — %s failed; copy saved", StoppedAtCutoverLabel, endedBy)
}

// resolveEndedBy sets each member's endedByName to the name of the member
// its EndedBy row belongs to; memberOf maps each row to its member. A
// reference to a row the apply does not list, or to the member's own row,
// resolves to no name, so the label still says the member was stopped at the
// cutover without naming a member it cannot place.
func resolveEndedBy(ops, members []Operation, names []string, memberOf []int) {
	rowOf := make(map[OperationRef]int, len(ops))
	for i, op := range ops {
		rowOf[OperationRef{Deployment: op.Deployment, OperationKey: op.OperationKey}] = i
	}
	for j := range members {
		ref := members[j].EndedBy
		if ref == nil {
			continue
		}
		if i, ok := rowOf[*ref]; ok && memberOf[i] != j {
			members[j].endedByName = names[memberOf[i]]
		}
	}
}

// endedByAll is the EndedBy of the row at lead among rows, but only when
// every failed row among them was failed for another row. A group with a row
// that failed on its own reads failed.
func endedByAll(ops []Operation, rows []int, lead int) *OperationRef {
	for _, i := range rows {
		if ops[i].State == state.ApplyOperation.Failed && ops[i].EndedBy == nil {
			return nil
		}
	}
	return ops[lead].EndedBy
}

// StoppedAtCutover is how many members of an apply were stopped at the
// cutover, and the member whose failure stopped them when every one of them
// names the same member.
type StoppedAtCutover struct {
	Count   int
	EndedBy string
}

// StoppedAtCutoverOf counts the members of deps stopped at the cutover.
func StoppedAtCutoverOf(deps []Deployment) StoppedAtCutover {
	var s StoppedAtCutover
	for _, d := range deps {
		if d.Presentation == StateStoppedAtCutover {
			s.Add(d.EndedBy)
		}
	}
	return s
}

// Add counts one more member stopped at the cutover for endedBy's failure.
func (s *StoppedAtCutover) Add(endedBy string) {
	switch {
	case s.Count == 0:
		s.EndedBy = endedBy
	case endedBy != s.EndedBy:
		s.EndedBy = ""
	}
	s.Count++
}

// Note explains the stopped members in the words of noun, with name
// formatting the failed member's name for the surface: "2 targets stopped at
// the cutover because -40 failed. Their copies are saved, so applying again
// resumes them." It is empty when no member was stopped at the cutover.
func (s StoppedAtCutover) Note(noun Noun, name func(string) string) string {
	if s.Count == 0 {
		return ""
	}
	cause := "another " + noun.Singular + " failed"
	if s.EndedBy != "" {
		cause = name(s.EndedBy) + " failed"
	}
	if s.Count == 1 {
		return fmt.Sprintf("1 %s stopped at the cutover because %s. Its copy is saved, so applying again resumes it.", noun.Singular, cause)
	}
	return fmt.Sprintf("%d %s stopped at the cutover because %s. Their copies are saved, so applying again resumes them.", s.Count, noun.Plural, cause)
}
