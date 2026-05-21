package commands

import (
	"fmt"

	"github.com/block/schemabot/pkg/cmd/templates"
)

// previewExitContext dispatches exit context preview types.
func previewExitContext(previewType templates.PreviewType) {
	switch previewType {
	case templates.PreviewExitDetachMySQL:
		previewExitDetachMySQL()
	case templates.PreviewExitDetachVitess:
		previewExitDetachVitess()
	case templates.PreviewExitErrorMySQL:
		previewExitErrorMySQL()
	case templates.PreviewExitErrorVitess:
		previewExitErrorVitess()
	case templates.PreviewExitAll:
		previewExitAll()
	}
}

func previewExitAll() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"MySQL Detach", previewExitDetachMySQL},
		{"Vitess Detach", previewExitDetachVitess},
		{"MySQL Connection Lost", previewExitErrorMySQL},
		{"Vitess Connection Lost", previewExitErrorVitess},
	}
	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("=== Exit Context: %s ===\n", s.name)
		s.fn()
	}
}

func previewExitDetachMySQL() {
	fmt.Print(formatExitContext("apply-8487c4ad", "", "mydb", "staging"))
}

func previewExitDetachVitess() {
	fmt.Print(formatExitContext(
		"apply-9849818a9cec4ba7",
		"https://app.planetscale.com/square-production/inventory2/deploy-requests/109",
		"inventory2",
		"production",
	))
}

func previewExitErrorMySQL() {
	fmt.Println("Error: unexpected error: connection refused")
	fmt.Print(formatExitContext("apply-8487c4ad", "", "mydb", "staging"))
}

func previewExitErrorVitess() {
	fmt.Println("Error: unexpected error: connection refused")
	fmt.Print(formatExitContext(
		"apply-9849818a9cec4ba7",
		"https://app.planetscale.com/square-production/inventory2/deploy-requests/109",
		"inventory2",
		"production",
	))
}
