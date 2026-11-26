package api

import "errors"

// AuthoredError marks an error whose text was written for the person who ran
// the command, so a renderer that shows a fixed line for every error it does
// not recognize may publish this one's text.
//
// A type opts in by implementing the method; nothing else makes an error
// authored. An error that does not, such as a dial failure or a storage error
// wrapped with context, stays unmarked and is never shown as it arrived (AV-8).
// Forgetting to mark an error costs detail on the pull request; the arrangement
// that fails the other way costs a leak.
type AuthoredError interface {
	error

	// AuthoredText returns the words to show, without the cause and without any
	// wrap an outer caller added.
	//
	// quotesRepository reports that those words were composed by a library from
	// the repository's own input, such as a parser's complaint about a schema
	// file or a planner's refusal of a statement. That text can quote the
	// statement, so a renderer sanitizes it, where SchemaBot's own words are
	// published as written.
	AuthoredText() (text string, quotesRepository bool)
}

// AuthoredTextOf returns the text of the first AuthoredError in err's chain,
// and whether the chain held one at all.
func AuthoredTextOf(err error) (text string, quotesRepository, ok bool) {
	authored, found := errors.AsType[AuthoredError](err)
	if !found {
		return "", false, false
	}
	text, quotesRepository = authored.AuthoredText()
	return text, quotesRepository, true
}

// The errors below are the ones on the plan path whose text SchemaBot wrote
// for the person reading the pull request. An error type that is not here
// stays unmarked, and TestEveryErrorTypeIsMarkedOrExplained fails until a new
// one is added to one side or the other. The engine's OperatorMessage and
// PlanRefusedError implement the same method in their own package.
//
// Each returns the error's own message and never its cause. Apart from the
// parser's complaint, the message names only identifiers the reader already
// has: a database, an environment, a target, a namespace, a table the plan
// names.

func (e *DatabaseNotConfiguredError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *EnvironmentNotConfiguredError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *databaseTypeMismatchError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *SourcePolicyError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *RolloutMemberSelectionError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *NamespaceCoverageError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *NamespaceSelectionError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *DropTablesInUseError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *UnselectedTableDropError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *SharedSchemaIgnoreListsError) AuthoredText() (string, bool) { return e.Error(), false }

func (e *operatorMessageError) AuthoredText() (string, bool) { return e.message, false }

// AuthoredText is the parser's complaint about a schema file, which can quote
// the statement it rejected.
func (e *schemaFileParseError) AuthoredText() (string, bool) { return e.message, true }
