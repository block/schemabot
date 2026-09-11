package caller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestPullRequest(t *testing.T) {
	t.Run("webhook caller yields its repo and PR number", func(t *testing.T) {
		repo, pr, ok := PullRequest("github:jdoe@acme/repo#42")
		assert.True(t, ok)
		assert.Equal(t, "acme/repo", repo)
		assert.Equal(t, 42, pr)
	})

	t.Run("email-shaped user splits at the last at-sign", func(t *testing.T) {
		repo, pr, ok := PullRequest("github:jdoe@example.com@acme/repo#42")
		assert.True(t, ok)
		assert.Equal(t, "acme/repo", repo)
		assert.Equal(t, 42, pr)
	})

	t.Run("bare server-substituted location yields its repo and PR number", func(t *testing.T) {
		repo, pr, ok := PullRequest("acme/repo#42")
		assert.True(t, ok)
		assert.Equal(t, "acme/repo", repo)
		assert.Equal(t, 42, pr)
	})

	t.Run("CLI caller is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("cli:jdoe@macbook.local")
		assert.False(t, ok)
	})

	t.Run("bare subject with a slash-and-hash tail is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("jdoe@example.com@acme/repo#42")
		assert.False(t, ok)
		_, _, ok = PullRequest("jdoe@example.com")
		assert.False(t, ok)
	})

	t.Run("webhook caller without a location is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe")
		assert.False(t, ok)
	})

	t.Run("location without a PR number is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@acme/repo")
		assert.False(t, ok)
	})

	t.Run("location without an owner/name repo is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@repo#42")
		assert.False(t, ok)
	})

	t.Run("repo with extra path segments or empty segments is rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@owner/repo/extra#123")
		assert.False(t, ok)
		_, _, ok = PullRequest("owner/repo/extra#123")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@/repo#42")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@owner/#42")
		assert.False(t, ok)
	})

	t.Run("non-positive and non-numeric PR numbers are rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@acme/repo#0")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@acme/repo#abc")
		assert.False(t, ok)
	})

	t.Run("signed and zero-padded PR numbers are rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@acme/repo#+5")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@acme/repo#-1")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@acme/repo#007")
		assert.False(t, ok)
	})

	t.Run("repo segments outside repository-shaped characters are rejected", func(t *testing.T) {
		_, _, ok := PullRequest("github:jdoe@acme/repo name#42")
		assert.False(t, ok)
		_, _, ok = PullRequest("github:jdoe@acme/repo\x1b[2K#42")
		assert.False(t, ok)
		_, _, ok = PullRequest("acme/repo\nname#42")
		assert.False(t, ok)
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

func TestPullRequestURL(t *testing.T) {
	assert.Equal(t, "https://github.com/acme/shop/pull/412", PullRequestURL("acme/shop", 412))
}

func TestPullRequestMarkdownLink(t *testing.T) {
	assert.Equal(t, "[acme/shop#412](https://github.com/acme/shop/pull/412)",
		PullRequestMarkdownLink("acme/shop", 412))
}

func TestParsePullRequestReference(t *testing.T) {
	accepted := []struct {
		name      string
		reference string
		repo      string
		pr        int
	}{
		{"the rendered URL round-trips", PullRequestURL("acme/shop", 412), "acme/shop", 412},
		{"without a scheme", "github.com/acme/shop/pull/412", "acme/shop", 412},
		{"an enterprise host", "https://git.example.com/acme/shop/pull/412", "acme/shop", 412},
		{"a host with a path prefix", "https://example.com/git/acme/shop/pull/412", "acme/shop", 412},
		{"the files view", "https://github.com/acme/shop/pull/412/files", "acme/shop", 412},
		{"a commit view", "https://github.com/acme/shop/pull/412/commits/3f9a1c2", "acme/shop", 412},
		{"a trailing slash", "https://github.com/acme/shop/pull/412/", "acme/shop", 412},
		{"a comment fragment", "https://github.com/acme/shop/pull/412#issuecomment-98217", "acme/shop", 412},
		{"a query string", "https://github.com/acme/shop/pull/412/files?w=1", "acme/shop", 412},
		{"the plural path GitHub also serves", "https://github.com/acme/shop/pulls/412", "acme/shop", 412},
		{"the comment form", "acme/shop#412", "acme/shop", 412},
		{"surrounding whitespace", "  https://github.com/acme/shop/pull/412  ", "acme/shop", 412},
		{"a bare repository carries no number", "acme/shop", "acme/shop", 0},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			repo, pr, err := ParsePullRequestReference(tc.reference)
			require.NoError(t, err)
			assert.Equal(t, tc.repo, repo)
			assert.Equal(t, tc.pr, pr)
		})
	}

	refused := []struct {
		name      string
		reference string
	}{
		{"empty", ""},
		{"whitespace only", "   "},
		{"a bare number names no repository", "412"},
		{"a fragment names no repository", "#412"},
		{"an owner with no repository", "acme"},
		{"an owner with an empty repository", "acme/"},
		{"a URL with no pull request number", "https://github.com/acme/shop/pulls"},
		{"a pull request number that is not a number", "https://github.com/acme/shop/pull/head"},
		{"a zero pull request number", "acme/shop#0"},
		{"a negative pull request number", "acme/shop#-3"},
		{"a repository page", "https://github.com/acme/shop"},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			_, _, err := ParsePullRequestReference(tc.reference)
			require.Error(t, err)
		})
	}
}
