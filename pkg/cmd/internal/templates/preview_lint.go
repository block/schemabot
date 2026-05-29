package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
)

func previewLintViolationsOutput() {
	fmt.Println("Lint violations: Non-blocking warnings during plan/apply")
	fmt.Println()

	warnings := []apitypes.LintViolationResponse{
		{Message: "has_float: New column uses floating-point data type", Table: "orders", Linter: "has_float"},
		{Message: "no_default: Column added without DEFAULT value", Table: "users", Linter: "no_default"},
	}
	WriteLintViolations(warnings)
}

func previewUnsafeBlockedOutput() {
	fmt.Println("Unsafe blocked: Destructive changes require --allow-unsafe")
	fmt.Println()

	changes := []UnsafeChange{
		{Table: "users", Reason: "DROP COLUMN email", ChangeType: "DROP COLUMN"},
		{Table: "orders", Reason: "DROP TABLE", ChangeType: "DROP TABLE"},
		{Table: "products", Reason: "MODIFY COLUMN price_cents: INT → SMALLINT (potential data loss); DROP INDEX idx_category", ChangeType: "MODIFY COLUMN"},
	}
	WriteUnsafeChangesBlocked(changes, "testapp", "staging", "./schema/testapp")
}

func previewUnsafeAllowedOutput() {
	fmt.Println("Unsafe allowed: Proceeding with --allow-unsafe flag")
	fmt.Println()

	changes := []UnsafeChange{
		{Table: "users", Reason: "DROP COLUMN email", ChangeType: "DROP COLUMN"},
		{Table: "orders", Reason: "DROP TABLE", ChangeType: "DROP TABLE"},
	}
	WriteUnsafeWarningAllowed(changes)
}

func previewLintAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"LINT WARNINGS", previewLintViolationsOutput},
		{"UNSAFE CHANGES BLOCKED", previewUnsafeBlockedOutput},
		{"UNSAFE CHANGES ALLOWED", previewUnsafeAllowedOutput},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
	}
}
