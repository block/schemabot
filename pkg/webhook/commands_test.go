package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommand(t *testing.T) {
	parser := NewCommandParser()

	tests := []struct {
		name     string
		body     string
		expected CommandResult
	}{
		{
			name: "plan with environment",
			body: "schemabot plan -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "plan with production",
			body: "schemabot plan -e production",
			expected: CommandResult{
				Action:      "plan",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "plan with database flag",
			body: "schemabot plan -e staging -d my-database",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Database:    "my-database",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply with skip-revert",
			body: "schemabot apply -e staging --skip-revert",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				SkipRevert:  true,
			},
		},
		{
			name: "apply with defer-cutover",
			body: "schemabot apply -e production --defer-cutover",
			expected: CommandResult{
				Action:       "apply",
				Environment:  "production",
				Found:        true,
				IsMention:    true,
				DeferCutover: true,
			},
		},
		{
			name: "help command",
			body: "schemabot help",
			expected: CommandResult{
				Action:    "help",
				IsHelp:    true,
				IsMention: true,
			},
		},
		{
			name: "unlock without -e",
			body: "schemabot unlock",
			expected: CommandResult{
				Action:    "unlock",
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "plan without -e (multi-env)",
			body: "schemabot plan",
			expected: CommandResult{
				Action:     "plan",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "apply without -e (error)",
			body: "schemabot apply",
			expected: CommandResult{
				Action:     "apply",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "unknown mention",
			body: "hey schemabot what's up",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name:     "no mention",
			body:     "just a regular comment",
			expected: CommandResult{},
		},
		{
			name: "case insensitive",
			body: "SchemaBot Plan -e Staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "apply-confirm",
			body: "schemabot apply-confirm -e staging",
			expected: CommandResult{
				Action:      "apply-confirm",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "stop",
			body: "schemabot stop -e production",
			expected: CommandResult{
				Action:      "stop",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "cutover",
			body: "schemabot cutover -e staging",
			expected: CommandResult{
				Action:      "cutover",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "revert",
			body: "schemabot revert -e staging",
			expected: CommandResult{
				Action:      "revert",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "skip-revert",
			body: "schemabot skip-revert -e staging",
			expected: CommandResult{
				Action:      "skip-revert",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback with apply ID",
			body: "schemabot rollback apply_abc123",
			expected: CommandResult{
				Action:    "rollback",
				ApplyID:   "apply_abc123",
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "rollback without apply ID",
			body: "schemabot rollback",
			expected: CommandResult{
				Action:     "rollback",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "rollback-confirm",
			body: "schemabot rollback-confirm -e production",
			expected: CommandResult{
				Action:      "rollback-confirm",
				Environment: "production",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "database flag before env",
			body: "schemabot plan -d users_db -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Database:    "users_db",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "fix-lint without -e",
			body: "schemabot fix-lint",
			expected: CommandResult{
				Action:    "fix-lint",
				Found:     true,
				IsMention: true,
			},
		},
		{
			name: "fix-lint with database",
			body: "schemabot fix-lint -d users_db",
			expected: CommandResult{
				Action:    "fix-lint",
				Found:     true,
				IsMention: true,
				Database:  "users_db",
			},
		},
		{
			name: "apply with allow-unsafe",
			body: "schemabot apply -e staging --allow-unsafe",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				AllowUnsafe: true,
			},
		},
		{
			name: "all flags combined",
			body: "schemabot apply -e production -d payments_db --defer-cutover --skip-revert --allow-unsafe",
			expected: CommandResult{
				Action:       "apply",
				Environment:  "production",
				Database:     "payments_db",
				Found:        true,
				IsMention:    true,
				SkipRevert:   true,
				DeferCutover: true,
				AllowUnsafe:  true,
			},
		},
		{
			name: "apply with -y short flag",
			body: "schemabot apply -e staging -y",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				AutoConfirm: true,
			},
		},
		{
			name: "apply with --yes long flag",
			body: "schemabot apply -e staging --yes",
			expected: CommandResult{
				Action:      "apply",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
				AutoConfirm: true,
			},
		},
		{
			name: "apply with -y and --allow-unsafe",
			body: "schemabot apply -e production --allow-unsafe -y",
			expected: CommandResult{
				Action:      "apply",
				Environment: "production",
				Found:       true,
				IsMention:   true,
				AllowUnsafe: true,
				AutoConfirm: true,
			},
		},
		{
			name: "-y ignored on apply-confirm (already a confirmation)",
			body: "schemabot apply-confirm -e staging -y",
			expected: CommandResult{
				Action:      "apply-confirm",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "-y ignored on plan",
			body: "schemabot plan -e staging -y",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parser.ParseCommand(tt.body)
			assert.Equal(t, tt.expected, result)
		})
	}
}
