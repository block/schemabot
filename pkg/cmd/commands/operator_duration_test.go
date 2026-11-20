package commands

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOperatorDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "go duration hours", value: "6h", want: 6 * time.Hour},
		{name: "go duration minutes", value: "90m", want: 90 * time.Minute},
		{name: "days suffix", value: "2d", want: 48 * time.Hour},
		{name: "days word", value: "2 days", want: 48 * time.Hour},
		{name: "hour word", value: "1 hour", want: time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseOperatorDuration(tt.value)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseOperatorDurationRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "0h", "-1h", "two days", "2 months"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			_, err := parseOperatorDuration(value)

			require.Error(t, err)
		})
	}
}
