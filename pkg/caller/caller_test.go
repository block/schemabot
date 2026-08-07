package caller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatSplitRoundTrip(t *testing.T) {
	t.Run("simple user", func(t *testing.T) {
		user, host, ok := SplitCLI(FormatCLI("jdoe", "macbook.local"))
		assert.True(t, ok)
		assert.Equal(t, "jdoe", user)
		assert.Equal(t, "macbook.local", host)
	})

	t.Run("email-shaped user splits at the last at-sign", func(t *testing.T) {
		user, host, ok := SplitCLI(FormatCLI("jdoe@example.com", "macbook.local"))
		assert.True(t, ok)
		assert.Equal(t, "jdoe@example.com", user)
		assert.Equal(t, "macbook.local", host)
	})
}

func TestSplitCLI(t *testing.T) {
	t.Run("non-CLI channel is rejected", func(t *testing.T) {
		_, _, ok := SplitCLI("github:jdoe@org/repo#42")
		assert.False(t, ok)
	})

	t.Run("bare subject is rejected", func(t *testing.T) {
		_, _, ok := SplitCLI("jdoe@example.com")
		assert.False(t, ok)
	})

	t.Run("CLI caller without a host is rejected", func(t *testing.T) {
		_, _, ok := SplitCLI("cli:jdoe")
		assert.False(t, ok)
	})
}

func TestValidHost(t *testing.T) {
	t.Run("hostname-shaped values are accepted", func(t *testing.T) {
		assert.True(t, ValidHost("build-agent_7.example.com"))
	})

	t.Run("whitespace and control characters are rejected", func(t *testing.T) {
		assert.False(t, ValidHost("host with spaces"))
		assert.False(t, ValidHost("host\x1b[2Kescape"))
		assert.False(t, ValidHost("host\nnewline"))
	})

	t.Run("empty and over the DNS length limit are rejected", func(t *testing.T) {
		assert.False(t, ValidHost(""))
		assert.True(t, ValidHost(strings.Repeat("h", 253)))
		assert.False(t, ValidHost(strings.Repeat("h", 254)))
	})
}

func TestShort(t *testing.T) {
	t.Run("CLI caller drops the machine", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe", Short("cli:jdoe@macbook.local"))
	})

	t.Run("email-shaped user keeps its domain and drops only the machine", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe@example.com", Short("cli:jdoe@example.com@macbook.local"))
	})

	t.Run("webhook caller drops the repo and PR", func(t *testing.T) {
		assert.Equal(t, "github:jdoe", Short("github:jdoe@acme/repo#42"))
	})

	t.Run("caller without a location passes through unchanged", func(t *testing.T) {
		assert.Equal(t, "cli:jdoe", Short("cli:jdoe"))
	})
}
