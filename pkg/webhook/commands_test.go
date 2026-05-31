package webhook

import (
	"testing"

	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/stretchr/testify/assert"
)

// TestCommandSpecs_CoverEveryDispatcherAction enforces that every command the
// dispatcher branches on has a spec in the registry. A spec missing here is
// the proximate cause of "schemabot $cmd" silently degrading to IsMention.
func TestCommandSpecs_CoverEveryDispatcherAction(t *testing.T) {
	required := []string{
		action.Help,
		action.Plan,
		action.Apply,
		action.ApplyConfirm,
		action.Unlock,
		action.FixLint,
		action.Stop,
		action.Revert,
		action.SkipRevert,
		action.Cutover,
		action.Rollback,
		action.RollbackConfirm,
	}
	for _, name := range required {
		_, ok := specByName[name]
		assert.Truef(t, ok, "commandSpecs is missing %q", name)
	}
}

// TestCommandSpecs_FlagsRespected pins which commands opt into which flags.
// A flag mistakenly enabled here silently broadens command behavior; a flag
// mistakenly removed silently drops user input. Either change should be a
// deliberate, reviewable diff.
func TestCommandSpecs_FlagsRespected(t *testing.T) {
	cases := []struct {
		name                string
		requiresEnv         bool
		hasApplyID          bool
		supportsDB          bool
		supportsAutoConfirm bool
		supportsSkipRevert  bool
		supportsDefer       bool
		supportsAllowUnsafe bool
	}{
		{name: action.Help},
		{name: action.Plan, requiresEnv: true, supportsDB: true},
		{name: action.Apply, requiresEnv: true, supportsDB: true,
			supportsAutoConfirm: true, supportsSkipRevert: true,
			supportsDefer: true, supportsAllowUnsafe: true},
		{name: action.ApplyConfirm, requiresEnv: true, supportsDB: true,
			supportsSkipRevert: true, supportsDefer: true, supportsAllowUnsafe: true},
		{name: action.Unlock},
		{name: action.FixLint, supportsDB: true},
		{name: action.Stop, requiresEnv: true, hasApplyID: true},
		{name: action.Revert, requiresEnv: true},
		{name: action.SkipRevert, requiresEnv: true},
		{name: action.Cutover, requiresEnv: true},
		{name: action.Rollback, requiresEnv: true, hasApplyID: true,
			supportsDB: true, supportsDefer: true},
		{name: action.RollbackConfirm, requiresEnv: true, supportsDB: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, ok := specByName[tc.name]
			assert.True(t, ok)
			assert.Equal(t, tc.requiresEnv, spec.RequiresEnv, "RequiresEnv")
			assert.Equal(t, tc.hasApplyID, spec.HasApplyID, "HasApplyID")
			assert.Equal(t, tc.supportsDB, spec.SupportsDB, "SupportsDB")
			assert.Equal(t, tc.supportsAutoConfirm, spec.SupportsAutoConfirm, "SupportsAutoConfirm")
			assert.Equal(t, tc.supportsSkipRevert, spec.SupportsSkipRevert, "SupportsSkipRevert")
			assert.Equal(t, tc.supportsDefer, spec.SupportsDeferCutover, "SupportsDeferCutover")
			assert.Equal(t, tc.supportsAllowUnsafe, spec.SupportsAllowUnsafe, "SupportsAllowUnsafe")
		})
	}
}

func TestHasAutoConfirmFlag(t *testing.T) {
	p := NewCommandParser()
	assert.True(t, p.HasAutoConfirmFlag("schemabot apply -e staging -y"))
	assert.True(t, p.HasAutoConfirmFlag("schemabot apply -e staging --yes"))
	assert.False(t, p.HasAutoConfirmFlag("schemabot apply -e staging"))
	assert.False(t, p.HasAutoConfirmFlag(""))
}

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
			body: "schemabot what's up",
			expected: CommandResult{
				IsMention: true,
			},
		},
		{
			name:     "inline prose mention ignored",
			body:     "I onboarded schemabot in this repo.",
			expected: CommandResult{},
		},
		{
			name:     "schemabot filename ignored",
			body:     "With `schemabot.yaml` sitting at `files/migrations/`, the app uses declarative schema changes.",
			expected: CommandResult{},
		},
		{
			name:     "schemabot url ignored",
			body:     "See https://github.com/block/schemabot for details.",
			expected: CommandResult{},
		},
		{
			name:     "quoted command ignored",
			body:     "> schemabot plan -e staging",
			expected: CommandResult{},
		},
		{
			name: "command after prose on new line",
			body: "Please run this:\n\nschemabot plan -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "command with markdown indentation",
			body: "  schemabot plan -e staging",
			expected: CommandResult{
				Action:      "plan",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name:     "command in fenced code block ignored",
			body:     "```sh\nschemabot plan -e staging\n```",
			expected: CommandResult{},
		},
		{
			name:     "command in tilde fenced code block ignored",
			body:     "~~~\nschemabot apply -e staging\n~~~",
			expected: CommandResult{},
		},
		{
			name:     "command in indented code block ignored",
			body:     "    schemabot plan -e staging",
			expected: CommandResult{},
		},
		{
			name: "command after fenced example",
			body: "```sh\nschemabot plan -e staging\n```\n\nschemabot plan -e production",
			expected: CommandResult{
				Action:      "plan",
				Environment: "production",
				Found:       true,
				IsMention:   true,
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
			body: "schemabot stop apply_abc123 -e production",
			expected: CommandResult{
				Action:      "stop",
				ApplyID:     "apply_abc123",
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
			name: "rollback with apply ID and env",
			body: "schemabot rollback apply_abc123 -e Staging",
			expected: CommandResult{
				Action:      "rollback",
				ApplyID:     "apply_abc123",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback with apply ID missing env",
			body: "schemabot rollback apply_abc123",
			expected: CommandResult{
				Action:     "rollback",
				ApplyID:    "apply_abc123",
				IsMention:  true,
				MissingEnv: true,
			},
		},
		{
			name: "rollback without apply ID",
			body: "schemabot rollback -e Staging",
			expected: CommandResult{
				Action:      "rollback",
				Environment: "staging",
				Found:       true,
				IsMention:   true,
			},
		},
		{
			name: "rollback without apply ID or env",
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
