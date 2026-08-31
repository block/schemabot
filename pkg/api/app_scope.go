package api

import (
	"fmt"
	"slices"
	"strings"
)

// maxAppNameChars caps the length of a database's app identifier so app names
// stay usable in PR comments, log lines, and error messages.
const maxAppNameChars = 64

// validateDatabaseApp checks the optional app grouping identifier on a
// database config. An empty value means the database belongs to no app. When
// set, the value must be lowercase alphanumeric with interior hyphens so app
// names are unambiguous in PR comment commands (`--app <name>`), which are
// case-normalized and whitespace-delimited.
func validateDatabaseApp(database string, dbConfig DatabaseConfig) error {
	app := dbConfig.App
	if app == "" {
		return nil
	}
	if len(app) > maxAppNameChars {
		return fmt.Errorf("databases.%s.app value %q exceeds %d characters", database, app, maxAppNameChars)
	}
	for _, r := range app {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return fmt.Errorf("databases.%s.app value %q must be lowercase alphanumeric with hyphens", database, app)
		}
	}
	if app[0] == '-' || app[len(app)-1] == '-' {
		return fmt.Errorf("databases.%s.app value %q must start and end with a letter or digit", database, app)
	}
	if strings.Contains(app, "--") {
		return fmt.Errorf("databases.%s.app value %q must not contain consecutive hyphens", database, app)
	}
	return nil
}

// DatabasesForApp returns the sorted names of every configured database whose
// app field matches app. An app no configured database declares is an error:
// app-scoped commands fail closed rather than silently targeting nothing.
func (c *ServerConfig) DatabasesForApp(app string) ([]string, error) {
	if c == nil {
		return nil, fmt.Errorf("resolve app %q: server config not loaded", app)
	}
	// An unset app field means "belongs to no app", so an empty query must not
	// match every untagged database.
	if app == "" {
		return nil, fmt.Errorf("resolve app: app identifier is empty")
	}
	var names []string
	for name, dbConfig := range c.Databases {
		if dbConfig.App == app {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("app %q has no configured databases", app)
	}
	slices.Sort(names)
	return names, nil
}
