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
	for _, r := range app {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
		default:
			return fmt.Errorf("databases.%s.app value %q must be lowercase alphanumeric with hyphens", database, app)
		}
	}
	// The charset above is all ASCII, so at this point bytes and characters
	// agree and len(app) counts what the error message promises.
	if len(app) > maxAppNameChars {
		return fmt.Errorf("databases.%s.app value %q exceeds %d characters", database, app, maxAppNameChars)
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
	for name, dbConfig := range c.DatabaseConfigs() {
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

// DatabaseForApp resolves app to the single configured database that declares
// it. Callers that operate on one database at a time (schema pull) use this
// instead of DatabasesForApp: an app declared by several databases is an error
// naming the candidates, so the answer is never an arbitrary pick.
func (c *ServerConfig) DatabaseForApp(app string) (string, error) {
	databases, err := c.DatabasesForApp(app)
	if err != nil {
		return "", err
	}
	if len(databases) > 1 {
		return "", fmt.Errorf("app %q is declared by %d databases (%s); name the database directly", app, len(databases), strings.Join(databases, ", "))
	}
	return databases[0], nil
}
