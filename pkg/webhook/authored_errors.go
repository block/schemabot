package webhook

// AuthoredText marks the errors in this file as written for the PR author:
// SchemaBot's own words, naming only the database, environment, path, and PR
// the author already has. A renderer that shows only marked errors can publish
// these as written and show a fixed line for everything else. The method
// matches the interface the API package defines, without importing it, and
// returns the words alone, without any cause or any wrap an outer caller added.
// The second result is true when a library composed the words from the
// repository's own input, so a renderer sanitizes them. None of the errors here
// do.
//
// Types that are deliberately unmarked are listed with a reason in the
// completeness test, so a new error type has to choose.

// AuthoredText returns the whole message: it carries no cause.
func (e *environmentNotConfiguredError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the whole message: it carries no cause.
func (e *schemaConfigOutsideAllowedDirsError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the whole message: it carries no cause.
func (e *databaseNotRegisteredError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the whole message: it carries no cause.
func (e *closedPRRefreshError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the whole message: it carries no cause.
func (e *prSchemaRefusalError) AuthoredText() (string, bool) {
	return e.Error(), false
}

// AuthoredText returns the refusal's own words, without the cause it was built
// from.
func (e *unlockRefusalError) AuthoredText() (string, bool) {
	return e.message, false
}
