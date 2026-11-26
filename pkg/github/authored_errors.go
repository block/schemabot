package github

import (
	"fmt"
	"strings"
)

// AuthoredText marks the errors in this file as written for the PR author:
// SchemaBot's own words, naming only the database, environment, and paths the
// author already has. A renderer that shows only marked errors can publish
// these as written and show a fixed line for everything else. The method
// matches the interface the API package defines, without importing it, and
// returns the words alone, without any cause or any wrap an outer caller added.
// The second result is true when a library composed the words from the
// repository's own input, so a renderer sanitizes them. None of the errors here
// do.
//
// Types that are deliberately unmarked are listed with a reason in the
// completeness test, so a new error type has to choose.

// AuthoredText returns the whole message: it carries no cause. A miss with
// configs it could not read does not establish that the database is absent, so
// the words say how many configs could not be read rather than stating a
// definite miss. The count, not the paths, keeps repository input out of them.
func (e *DatabaseNotFoundError) AuthoredText() (string, bool) {
	if e.Conclusive() {
		return e.Error(), false
	}
	noun := "configs"
	if len(e.UnreadableConfigs) == 1 {
		noun = "config"
	}
	return fmt.Sprintf("%s; SchemaBot could not read %d schemabot.yaml %s, so the database may be declared in one of them",
		e.Error(), len(e.UnreadableConfigs), noun), false
}

// noSchemaFilesError reports that the schema root for an environment holds no
// schema files to plan, or none after the PR's own removals or ignored
// namespaces are taken into account. The message is composed where the reason
// is known.
type noSchemaFilesError struct {
	message string
}

func (e *noSchemaFilesError) Error() string {
	return e.message
}

// AuthoredText returns the whole message: it carries no cause.
func (e *noSchemaFilesError) AuthoredText() (string, bool) {
	return e.message, false
}

// ambiguousDatabaseConfigError reports that a database name matches more than
// one schemabot.yaml, so the command cannot tell which one it means.
type ambiguousDatabaseConfigError struct {
	Database string
	Paths    []string
}

func (e *ambiguousDatabaseConfigError) Error() string {
	return fmt.Sprintf("ambiguous: database '%s' matches multiple configs at: %s",
		e.Database, strings.Join(e.Paths, ", "))
}

// AuthoredText returns the whole message: it carries no cause.
func (e *ambiguousDatabaseConfigError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the whole message: it carries no cause.
func (e *MultipleConfigsError) AuthoredText() (string, bool) {
	return e.Error(), false
}
