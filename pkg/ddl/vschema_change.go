package ddl

import (
	"fmt"

	"github.com/block/schemabot/pkg/vschema"
)

// SameVSchemaChange reports whether a pull request's edit to a Vitess
// keyspace's VSchema makes the same change on two different bases: the head
// changes nothing the reviewer did not approve.
//
// approvedBase and approved are the VSchema at the approved commit's merge
// base and at the approved commit; headBase and head are the VSchema at the
// head's merge base and at the head. A VSchema the pull request adds is
// absent at both bases, passed as "".
//
// Each table, each vindex, and the keyspace's own settings is one element
// (see vschema.Elements), and the change is the same when every element is
// the three-way merge of the approved commit and headBase (see
// UnmergedElement). That is the whole check: Vitess applies a VSchema as a
// document, with no statements or order of its own, so the merge alone
// proves the head changes only elements the reviewer approved, less those
// the base now carries itself. A rebase that combines the pull request's
// table with tables the base added keeps the approval; a head that routes
// any table differently from the approved commit does not.
//
// When the change is not the same, reason says why, for operator logs. A
// document the vendored VSchema definition cannot read is an error: it
// proves nothing.
func SameVSchemaChange(approvedBase, approved, headBase, head string) (same bool, reason string, err error) {
	files := [...]struct {
		name    string
		content string
	}{
		{"approved commit's base", approvedBase},
		{"approved commit", approved},
		{"head's base", headBase},
		{"head", head},
	}
	var elements [len(files)]map[string]string
	for i, f := range files {
		e, err := vschema.Elements(f.content)
		if err != nil {
			return false, "", fmt.Errorf("read the VSchema at the %s: %w", f.name, err)
		}
		elements[i] = e
	}
	if why := UnmergedElement(elements[0], elements[1], elements[2], elements[3]); why != "" {
		return false, "vschema: " + why, nil
	}
	return true, "", nil
}
