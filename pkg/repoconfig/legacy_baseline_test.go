package repoconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyBaselineValidate(t *testing.T) {
	valid := &LegacyBaseline{
		Version:     LegacyBaselineVersion,
		BaseCommit:  "0123456789abcdef0123456789abcdef01234567",
		LegacyPaths: []string{"service/src/main/resources/db-changes"},
	}
	require.NoError(t, valid.Validate())

	tests := []struct {
		name     string
		baseline *LegacyBaseline
		want     string
	}{
		{name: "missing", want: "required"},
		{name: "version", baseline: &LegacyBaseline{BaseCommit: valid.BaseCommit, LegacyPaths: valid.LegacyPaths}, want: "version"},
		{name: "short commit", baseline: &LegacyBaseline{Version: 1, BaseCommit: "abc", LegacyPaths: valid.LegacyPaths}, want: "40-character"},
		{name: "no paths", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit}, want: "at least one"},
		{name: "root", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit, LegacyPaths: []string{"."}}, want: "below the repository root"},
		{name: "parent", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit, LegacyPaths: []string{"../schema"}}, want: "below the repository root"},
		{name: "unclean", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit, LegacyPaths: []string{"service/../schema"}}, want: "normalized"},
		{name: "glob", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit, LegacyPaths: []string{"schema/*.sql"}}, want: "exact"},
		{name: "duplicate", baseline: &LegacyBaseline{Version: 1, BaseCommit: valid.BaseCommit, LegacyPaths: []string{"schema", "schema"}}, want: "duplicated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.baseline.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}
