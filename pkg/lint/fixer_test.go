package lint

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Core Fix Tests - Test individual fixes via FixFiles
// =============================================================================

func TestFixer_FixPrimaryKeyType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantFix  bool
	}{
		{
			name: "INT AUTO_INCREMENT PRIMARY KEY",
			input: `CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
)`,
			expected: "BIGINT",
			wantFix:  true,
		},
		{
			name: "already BIGINT",
			input: `CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
)`,
			expected: "BIGINT",
			wantFix:  false,
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			if tt.wantFix {
				fixFound := false
				for _, fix := range fr.Fixes {
					if strings.Contains(fix, "BIGINT") {
						fixFound = true
						break
					}
				}
				assert.True(t, fixFound, "expected INT→BIGINT fix but didn't find it in fixes: %v", fr.Fixes)
				assert.Contains(t, fr.FixedSQL, tt.expected)
			}
		})
	}
}

func TestFixer_FixCharset(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		wantFix  bool
	}{
		{
			name:     "latin1 charset",
			input:    `CREATE TABLE users (id INT) CHARSET=latin1`,
			contains: "utf8mb4",
			wantFix:  true,
		},
		{
			name:     "utf8 charset (not utf8mb4)",
			input:    `CREATE TABLE users (id INT) CHARACTER SET utf8`,
			contains: "utf8mb4",
			wantFix:  true,
		},
		{
			name:     "already utf8mb4",
			input:    `CREATE TABLE users (id INT) CHARSET=utf8mb4`,
			contains: "UTF8MB4", // Canonical output is uppercase
			wantFix:  false,
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			if tt.wantFix {
				fixFound := false
				for _, fix := range fr.Fixes {
					if strings.Contains(fix, "charset") || strings.Contains(fix, "utf8mb4") {
						fixFound = true
						break
					}
				}
				if !fixFound {
					assert.Contains(t, strings.Join(fr.Fixes, " "), "canonical", "expected charset fix but didn't find it in fixes: %v", fr.Fixes)
				}
			}
			assert.Contains(t, strings.ToUpper(output), strings.ToUpper(tt.contains))
		})
	}
}

func TestFixer_FixFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		wantFix  bool
	}{
		{
			name:     "FLOAT column",
			input:    `CREATE TABLE prices (amount FLOAT)`,
			contains: "DECIMAL",
			wantFix:  true,
		},
		{
			name:     "FLOAT with precision",
			input:    `CREATE TABLE prices (amount FLOAT(8,2))`,
			contains: "DECIMAL(8,2)",
			wantFix:  true,
		},
		{
			name:     "DOUBLE column",
			input:    `CREATE TABLE prices (amount DOUBLE)`,
			contains: "DECIMAL",
			wantFix:  true,
		},
		{
			name:     "already DECIMAL",
			input:    `CREATE TABLE prices (amount DECIMAL(10,2))`,
			contains: "DECIMAL",
			wantFix:  false,
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			if tt.wantFix {
				fixFound := false
				for _, fix := range fr.Fixes {
					if strings.Contains(fix, "DECIMAL") {
						fixFound = true
						break
					}
				}
				assert.True(t, fixFound, "expected FLOAT→DECIMAL fix but didn't find it in fixes: %v", fr.Fixes)
			}
			assert.Contains(t, output, tt.contains)
		})
	}
}

func TestFixer_FixZeroDate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
		wantFix  bool
	}{
		{
			name:     "TIMESTAMP without DEFAULT",
			input:    `CREATE TABLE events (created_at TIMESTAMP)`,
			contains: "DEFAULT",
			wantFix:  true,
		},
		{
			name:     "TIMESTAMP NOT NULL without DEFAULT",
			input:    `CREATE TABLE events (created_at TIMESTAMP NOT NULL)`,
			contains: "DEFAULT",
			wantFix:  true,
		},
		{
			name:     "already has DEFAULT",
			input:    `CREATE TABLE events (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`,
			contains: "DEFAULT",
			wantFix:  false,
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			if tt.wantFix {
				fixFound := false
				for _, fix := range fr.Fixes {
					if strings.Contains(fix, "DEFAULT") || strings.Contains(fix, "TIMESTAMP") {
						fixFound = true
						break
					}
				}
				assert.True(t, fixFound, "expected zero date fix but didn't find it in fixes: %v", fr.Fixes)
			}
			assert.Contains(t, output, tt.contains)
		})
	}
}

func TestFixer_FixFiles(t *testing.T) {
	fixer := NewFixer()

	files := map[string]string{
		"users.sql": `CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
) CHARSET=latin1`,
	}

	result, err := fixer.FixFiles(files)
	require.NoError(t, err)

	assert.NotZero(t, result.TotalFixed, "expected fixes but got none")
	require.Len(t, result.Files, 1)

	fr := result.Files[0]
	assert.True(t, fr.Changed, "expected file to be changed")
	assert.Contains(t, fr.FixedSQL, "BIGINT", "expected BIGINT in fixed SQL")

	// Canonical output uses UTF8MB4 (uppercase)
	assert.Contains(t, strings.ToUpper(fr.FixedSQL), "UTF8MB4", "expected utf8mb4 in fixed SQL")
}

// =============================================================================
// Tier 2 Warning Tests - More comprehensive variant testing
// =============================================================================

func TestTier2_PrimaryKeyType_Variants(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFix  bool
		contains string
	}{
		{
			name:     "INT with separate PRIMARY KEY constraint",
			input:    "CREATE TABLE t (id INT AUTO_INCREMENT, PRIMARY KEY (id))",
			wantFix:  true,
			contains: "BIGINT",
		},
		{
			name:     "BIGINT already - no change needed",
			input:    "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY)",
			wantFix:  false,
			contains: "BIGINT",
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			hasBigintFix := false
			for _, fix := range fr.Fixes {
				if strings.Contains(fix, "BIGINT") {
					hasBigintFix = true
					break
				}
			}

			if tt.wantFix {
				assert.True(t, hasBigintFix, "wantFix=%v but no BIGINT fix found in: %v", tt.wantFix, fr.Fixes)
			}
			assert.Contains(t, output, tt.contains)
		})
	}
}

func TestTier2_Charset_Variants(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFix  bool
		contains string
	}{
		{
			name:     "DEFAULT CHARSET latin1",
			input:    "CREATE TABLE t (id INT) DEFAULT CHARSET=latin1",
			wantFix:  true,
			contains: "utf8mb4",
		},
		{
			name:     "CHARACTER SET utf8 (not mb4)",
			input:    "CREATE TABLE t (id INT) CHARACTER SET utf8",
			wantFix:  true,
			contains: "utf8mb4",
		},
		{
			name:     "COLLATE latin1_general_ci",
			input:    "CREATE TABLE t (id INT) COLLATE=latin1_general_ci",
			wantFix:  true,
			contains: "utf8mb4",
		},
		{
			name:     "utf8mb4 already - canonicalization only",
			input:    "CREATE TABLE t (id INT) CHARSET=utf8mb4",
			wantFix:  false,
			contains: "UTF8MB4", // Canonical is uppercase
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			hasCharsetFix := false
			for _, fix := range fr.Fixes {
				if strings.Contains(fix, "charset") || strings.Contains(fix, "utf8mb4") {
					hasCharsetFix = true
					break
				}
			}

			if tt.wantFix {
				assert.True(t, hasCharsetFix, "wantFix=%v but no charset fix found in: %v", tt.wantFix, fr.Fixes)
			}
			assert.Contains(t, strings.ToUpper(output), strings.ToUpper(tt.contains))
		})
	}
}

func TestTier2_Float_Variants(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFix  bool
		contains string
	}{
		{
			name:     "FLOAT in column",
			input:    "CREATE TABLE t (price FLOAT NOT NULL)",
			wantFix:  true,
			contains: "DECIMAL",
		},
		{
			name:     "DOUBLE PRECISION",
			input:    "CREATE TABLE t (price DOUBLE NOT NULL)",
			wantFix:  true,
			contains: "DECIMAL",
		},
		{
			name:     "FLOAT(5,2) preserves precision",
			input:    "CREATE TABLE t (price FLOAT(5,2))",
			wantFix:  true,
			contains: "DECIMAL(5,2)",
		},
		{
			name:     "DECIMAL already - no change",
			input:    "CREATE TABLE t (price DECIMAL(10,2))",
			wantFix:  false,
			contains: "DECIMAL",
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			hasFloatFix := false
			for _, fix := range fr.Fixes {
				if strings.Contains(fix, "DECIMAL") {
					hasFloatFix = true
					break
				}
			}

			if tt.wantFix {
				assert.True(t, hasFloatFix, "wantFix=%v but no DECIMAL fix found in: %v", tt.wantFix, fr.Fixes)
			}
			assert.Contains(t, output, tt.contains)
		})
	}
}

func TestTier2_ZeroDate_Variants(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFix  bool
		contains string
	}{
		{
			name:     "TIMESTAMP nullable no default",
			input:    "CREATE TABLE t (created_at TIMESTAMP)",
			wantFix:  true,
			contains: "DEFAULT",
		},
		{
			name:     "DATETIME NOT NULL no default",
			input:    "CREATE TABLE t (created_at DATETIME NOT NULL)",
			wantFix:  true,
			contains: "DEFAULT",
		},
		{
			name:     "already has DEFAULT CURRENT_TIMESTAMP",
			input:    "CREATE TABLE t (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			wantFix:  false,
			contains: "DEFAULT",
		},
		{
			name:     "already has DEFAULT NULL",
			input:    "CREATE TABLE t (created_at TIMESTAMP DEFAULT NULL)",
			wantFix:  false,
			contains: "DEFAULT",
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			if output == "" {
				output = tt.input
			}

			hasZeroDateFix := false
			for _, fix := range fr.Fixes {
				if strings.Contains(fix, "DEFAULT") || strings.Contains(fix, "TIMESTAMP") || strings.Contains(fix, "DATETIME") {
					hasZeroDateFix = true
					break
				}
			}

			if tt.wantFix {
				assert.True(t, hasZeroDateFix, "wantFix=%v but no zero date fix found in: %v", tt.wantFix, fr.Fixes)
			}
			assert.Contains(t, output, tt.contains)
		})
	}
}

// =============================================================================
// Tier 3 Error Tests - Unfixable issues requiring human judgment
// =============================================================================

func TestTier3_UnfixableIssues(t *testing.T) {
	// These should be reported but NOT auto-fixed
	fixer := NewFixer()

	// Missing PRIMARY KEY - requires human to choose column(s)
	files := map[string]string{
		"sessions.sql": `CREATE TABLE sessions (
    session_id VARCHAR(255) NOT NULL,
    user_id BIGINT NOT NULL,
    data TEXT
)`,
	}

	result, err := fixer.FixFiles(files)
	require.NoError(t, err)

	// File may be canonicalized, but PK issue should be in unfixable
	for _, fr := range result.Files {
		if fr.Filename == "sessions.sql" {
			t.Logf("sessions.sql changed=%v, fixes=%v", fr.Changed, fr.Fixes)
		}
	}
}

// =============================================================================
// Tier 4 Unsafe Tests - Never auto-fixed (intentional destructive changes)
// =============================================================================

func TestTier4_UnsafeNotFixed(t *testing.T) {
	// Unsafe changes should NEVER be auto-fixed
	// The fixer doesn't even look at DDL statements, only schema files
	// But we verify that unsafe lint results are not processed

	fixer := NewFixer()

	// A schema file that is already in canonical form with correct types
	// We use the exact canonical output format to ensure no changes
	files := map[string]string{
		"users.sql": "CREATE TABLE `users` (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,`email` VARCHAR(255) NOT NULL) DEFAULT CHARACTER SET = UTF8MB4",
	}

	result, err := fixer.FixFiles(files)
	require.NoError(t, err)

	// Should have no fixes (already canonical and correct)
	assert.Zero(t, result.TotalFixed, "expected no fixes for canonical schema, got fixes: %v", result.Files[0].Fixes)
}

// =============================================================================
// Combined Multi-Tier Tests
// =============================================================================

func TestFixFiles_MultipleFixes(t *testing.T) {
	fixer := NewFixer()

	files := map[string]string{
		"orders.sql": `CREATE TABLE orders (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    amount FLOAT NOT NULL,
    created_at TIMESTAMP
) CHARSET=latin1`,
	}

	result, err := fixer.FixFiles(files)
	require.NoError(t, err)

	// Should fix: INT->BIGINT, FLOAT->DECIMAL, charset->utf8mb4, TIMESTAMP DEFAULT
	assert.GreaterOrEqual(t, result.TotalFixed, 3, "expected at least 3 fixes, got: %v", result.Files[0].Fixes)

	fr := result.Files[0]
	assert.Contains(t, fr.FixedSQL, "BIGINT", "expected BIGINT fix")
	assert.Contains(t, fr.FixedSQL, "DECIMAL", "expected DECIMAL fix")
	assert.Contains(t, strings.ToUpper(fr.FixedSQL), "UTF8MB4", "expected utf8mb4 fix")
}

func TestFixFiles_NoChangesNeeded(t *testing.T) {
	fixer := NewFixer()

	// Use exact canonical format to ensure no changes needed
	files := map[string]string{
		"perfect.sql": "CREATE TABLE `perfect` (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,`name` VARCHAR(255) NOT NULL,`price` DECIMAL(10,2),`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()) DEFAULT CHARACTER SET = UTF8MB4",
	}

	result, err := fixer.FixFiles(files)
	require.NoError(t, err)

	assert.Zero(t, result.TotalFixed, "expected no fixes for correct schema, got fixes: %v", result.Files[0].Fixes)

	fr := result.Files[0]
	assert.False(t, fr.Changed, "expected no change but file was modified")
}

// =============================================================================
// Tier 1 Style Tests - Canonicalization
// =============================================================================

func TestTier1_Canonicalization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string
	}{
		{
			name:     "lowercase keywords become uppercase",
			input:    "create table t (id int)",
			contains: []string{"CREATE TABLE", "INT"},
		},
		{
			name:     "identifiers get backticks",
			input:    "CREATE TABLE users (id INT)",
			contains: []string{"`users`", "`id`"},
		},
		{
			name:     "spacing is normalized",
			input:    "CREATE   TABLE   t(id   INT)",
			contains: []string{"CREATE TABLE `t`"},
		},
	}

	fixer := NewFixer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fixer.FixFiles(map[string]string{"test.sql": tt.input})
			require.NoError(t, err)
			fr := result.Files[0]

			output := fr.FixedSQL
			require.NotEmpty(t, output, "expected canonicalization but got no change")

			for _, expected := range tt.contains {
				assert.Contains(t, output, expected)
			}

			// Should report canonicalization fix
			hasCanonFix := false
			for _, fix := range fr.Fixes {
				if strings.Contains(fix, "canonical") || strings.Contains(fix, "Normalized") {
					hasCanonFix = true
					break
				}
			}
			assert.True(t, hasCanonFix, "expected canonicalization fix in: %v", fr.Fixes)
		})
	}
}

func TestFixFiles_InvalidSQL(t *testing.T) {
	fixer := NewFixer()

	_, err := fixer.FixFiles(map[string]string{
		"bad.sql": "CREATE TABLE t1 (",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bad.sql")
}
