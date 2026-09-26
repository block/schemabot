// Package lintguidance connects disclosed lint findings to operator guides.
package lintguidance

import "slices"

// Guide describes a document that explains a lint finding.
type Guide struct {
	Label string
	URL   string
}

// Scope names the findings a surface actually shows for one database.
type Scope struct {
	Rules   []string
	IsMySQL bool
}

// Guides returns matching guides once, in a stable order across environments.
func Guides(scopes ...Scope) []Guide {
	registry := []struct {
		rule      string
		guide     Guide
		mysqlOnly bool
	}{
		{"primary_key", Guide{"Choosing a primary key", "https://github.com/block/schemabot/blob/main/docs/mysql.md#choosing-a-primary-key"}, true},
		{"rename_column", Guide{"Renaming a column or table", "https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md#renaming-a-column-or-table"}, false},
	}
	var result []Guide
	for _, entry := range registry {
		for _, scope := range scopes {
			if (!entry.mysqlOnly || scope.IsMySQL) && slices.Contains(scope.Rules, entry.rule) {
				result = append(result, entry.guide)
				break
			}
		}
	}
	return result
}
