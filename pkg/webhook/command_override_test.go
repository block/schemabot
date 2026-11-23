package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/webhook/action"
)

// Quotes group the value of --override and nothing else, so a line without
// them splits exactly as strings.Fields always split it, Unicode spaces
// included.
func TestSplitCommandWords(t *testing.T) {
	for _, line := range []string{
		"apply -e production",
		"  apply\t-e   production\r",
		"apply -e staging --allow-unsafe -d users",
		`"apply" failed isn't "done"`,
		"apply --overridden fix",
		"apply -e production --override fix",
	} {
		words, bound, bad := splitCommandWords(line)
		assert.Equal(t, strings.Fields(line), words, line)
		assert.Empty(t, bound, line)
		assert.False(t, bad, line)
	}

	tests := []struct {
		name  string
		line  string
		words []string
		bad   bool
	}{
		{"double quotes", `apply --override "fix the index" -e production`, []string{"apply", "--override", "fix the index", "-e", "production"}, false},
		{"single quotes", `apply --override 'fix the index'`, []string{"apply", "--override", "fix the index"}, false},
		{"equals form", `apply --override=fix`, []string{"apply", "--override", "fix"}, false},
		{"equals quoted", `apply --override="fix the index" -e production`, []string{"apply", "--override", "fix the index", "-e", "production"}, false},
		{"flag in capitals", `apply --OVERRIDE "a b"`, []string{"apply", "--OVERRIDE", "a b"}, false},
		{"empty quotes", `apply --override ""`, []string{"apply", "--override", ""}, false},
		{"empty equals", `apply --override=`, []string{"apply", "--override", ""}, false},
		{"other quote inside", `apply --override "it's a fix"`, []string{"apply", "--override", "it's a fix"}, false},
		{"unterminated", `apply --override "fix the`, []string{"apply", "--override", "fix the"}, true},
		{"unterminated equals", `apply --override='fix`, []string{"apply", "--override", "fix"}, true},
		{"text glued after the quote", `apply --override "fix"x -e production`, []string{"apply", "--override", "fix", "-e", "production"}, true},
		{"quote on another flag is plain", `apply -d "users"`, []string{"apply", "-d", `"users"`}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			words, _, bad := splitCommandWords(tt.line)
			assert.Equal(t, tt.words, words)
			assert.Equal(t, tt.bad, bad)
		})
	}
}

func TestParseCommandOverride(t *testing.T) {
	p := NewCommandParser()
	tests := []struct {
		name      string
		comment   string
		action    string
		override  string
		given     bool
		unmatched string // "malformed" or "prose" when the line is not a command
	}{
		{name: "apply quoted", comment: `schemabot apply -e production --override "fix the index"`, action: action.Apply, override: "fix the index", given: true},
		{name: "apply single quoted", comment: `schemabot apply -e production --override 'fix the index'`, action: action.Apply, override: "fix the index", given: true},
		{name: "apply one word", comment: `schemabot apply -e production --override hotfix`, action: action.Apply, override: "hotfix", given: true},
		{name: "apply equals", comment: `schemabot apply -e production --override=hotfix`, action: action.Apply, override: "hotfix", given: true},
		{name: "apply equals quoted", comment: `schemabot apply -e production --override="fix the index"`, action: action.Apply, override: "fix the index", given: true},
		{name: "before other flags", comment: `schemabot apply --override "fix it" -e production --allow-unsafe`, action: action.Apply, override: "fix it", given: true},
		{name: "start", comment: `schemabot start apply_abc123 -e production --override "fix it"`, action: action.Start, override: "fix it", given: true},
		{name: "cutover", comment: `schemabot cutover apply_abc123 -e production --override "fix it"`, action: action.Cutover, override: "fix it", given: true},
		{name: "empty", comment: `schemabot apply -e production --override ""`, action: action.Apply, given: true},
		{name: "blank", comment: `schemabot apply -e production --override "  "`, action: action.Apply, override: "  ", given: true},
		{name: "no value", comment: `schemabot apply -e production --override`, action: action.Apply, given: true},
		// A quoted or `=` value is the reason even when it opens with a dash;
		// only a bare word that looks like a flag is not.
		{name: "quoted reason opening with a dash", comment: `schemabot apply -e production --override "- incident follow-up"`, action: action.Apply, override: "- incident follow-up", given: true},
		{name: "equals quoted reason opening with a dash", comment: `schemabot apply -e production --override="- incident follow-up"`, action: action.Apply, override: "- incident follow-up", given: true},
		{name: "equals reason opening with a dash", comment: `schemabot apply -e production --override=-hotfix`, action: action.Apply, override: "-hotfix", given: true},
		{name: "a flag after it is not the reason", comment: `schemabot apply --override -e production`, action: action.Apply, given: true},
		{name: "absent", comment: `schemabot apply -e production`, action: action.Apply},
		{name: "unterminated quote", comment: `schemabot apply -e production --override "fix the index`, unmatched: "malformed"},
		{name: "text after the closing quote", comment: `schemabot apply -e production --override "fix"index`, unmatched: "malformed"},
		{name: "given twice", comment: `schemabot apply -e production --override a --override b`, unmatched: "malformed"},
		// Without quotes only the first word is the reason; the rest reads
		// as a sentence, which is never answered.
		{name: "several words unquoted", comment: `schemabot apply -e production --override fix the index`, unmatched: "prose"},
		// Quotes elsewhere keep their old meaning.
		{name: "quoted command word", comment: `schemabot "apply" -e production failed`, unmatched: "prose"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := p.ParseCommand(tt.comment)
			switch tt.unmatched {
			case "malformed":
				assert.True(t, result.IsMention)
				assert.False(t, result.Found, "a malformed line is answered as an invalid command")
				assert.Empty(t, result.Action)
				return
			case "prose":
				assert.False(t, result.IsMention)
				assert.True(t, result.ProseMention)
				return
			}
			require.True(t, result.Found)
			assert.Equal(t, tt.action, result.Action)
			assert.Equal(t, tt.override, result.Override)
			assert.Equal(t, tt.given, result.OverrideGiven)
		})
	}
}

// Rollback, revert and the pinned rollback skip the freeze gate, so --override
// on them is an unsupported flag, as -d and --target are where unsupported.
func TestOverrideFlagIsSupportedOnApplyStartAndCutoverOnly(t *testing.T) {
	// apply-confirm takes it because an apply that must discard an unfinished
	// copy stops for confirmation before the freeze gate runs.
	supported := map[string]bool{action.Apply: true, action.ApplyConfirm: true, action.Start: true, action.Cutover: true}
	for _, spec := range commandSpecs {
		assert.Equal(t, supported[spec.Name], spec.SupportsOverride, spec.Name)
		assert.Equal(t, supported[spec.Name], commandSupportsOverrideFlag(spec.Name), spec.Name)
	}

	p := NewCommandParser()
	for _, comment := range []string{
		`schemabot rollback apply_abc123 -e production --override "fix"`,
		`schemabot revert apply_abc123 -e production --override "fix"`,
		`schemabot stop apply_abc123 -e production --override=fix`,
	} {
		result := p.ParseCommand(comment)
		assert.False(t, result.OverrideGiven, comment)
		assert.Empty(t, result.Override, comment)
		assert.True(t, p.HasOverrideFlag(comment), comment)
	}
	assert.False(t, p.HasOverrideFlag(`schemabot apply -e production`))
	// A fenced example or a sentence describes the flag; it does not pass it.
	assert.False(t, p.HasOverrideFlag("schemabot stop apply_abc123 -e production\n\n```\nschemabot apply -e production --override fix\n```\n"))
}
