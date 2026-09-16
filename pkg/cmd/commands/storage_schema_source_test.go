package commands

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/schema"
)

// mysqlDialect answers the dialect resolver without a database or a server,
// for the cases where resolving it is not what is under test.
func mysqlDialect() (schema.Dialect, error) { return schema.DialectMySQL, nil }

// Exactly one release is named. Naming both is refused rather than resolved by
// precedence, and naming neither is refused rather than resolved by default: a
// report whose desired side the operator did not choose is the one report that
// can be read as the opposite of what it says.
func TestStorageSchemaSourceFlags_ValidateSource(t *testing.T) {
	require.NoError(t, (&storageSchemaSourceFlags{SchemaDir: "./schema/mysql"}).validateSource())
	require.NoError(t, (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: defaultStorageSchemaRepo}).validateSource())
	require.NoError(t, (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/mirror"}).validateSource())

	unnamed := (&storageSchemaSourceFlags{}).validateSource()
	require.Error(t, unnamed, "the desired schema has no default")
	assert.Contains(t, unnamed.Error(), "missing flags")
	assert.Contains(t, unnamed.Error(), "--release")
	assert.Contains(t, unnamed.Error(), "--schema-dir")

	err := (&storageSchemaSourceFlags{SchemaDir: "./schema/mysql", Release: "v1.4.0"}).validateSource()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--release and --schema-dir can't be used together")

	err = (&storageSchemaSourceFlags{Repo: "example/mirror"}).validateSource()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing flags")

	err = (&storageSchemaSourceFlags{SchemaDir: "./schema/mysql", Repo: "example/mirror"}).validateSource()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--release-repo only applies with --release")

	// The flag carries no parser default, so typing the default value is still
	// typing the flag. Defaulting it in the parser would make the two
	// indistinguishable here and quietly accept the very combination above.
	err = (&storageSchemaSourceFlags{SchemaDir: "./schema/mysql", Repo: defaultStorageSchemaRepo}).validateSource()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--release-repo only applies with --release")
}

// A token never travels in plaintext, including on a redirect. Go carries the
// Authorization header across a redirect that stays on the same host and
// compares hosts without comparing schemes, so an https host that redirects to
// itself over http would otherwise forward the token in the clear.
func TestRefuseInsecureRedirect(t *testing.T) {
	withToken := func(rawURL string) *http.Request {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+"not-a-real-token")
		return request
	}

	err := refuseInsecureRedirect(withToken("http://api.example/repos"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing a redirect to http://api.example")
	assert.Contains(t, err.Error(), "the token would follow it in plaintext")

	require.NoError(t, refuseInsecureRedirect(withToken("https://api.example/repos"), nil))

	// Without a token there is nothing to protect on the wire, so a plaintext
	// mirror is left alone — it is how a public release is fetched with no
	// credentials at all. The hop is still said out loud: the warning on the
	// configured base URL cannot speak for a host a redirect chose, and the
	// files arrive over this channel rather than that one.
	anonymous, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://api.example/repos", nil)
	require.NoError(t, err)
	stderr := captureStderr(t, func() {
		require.NoError(t, refuseInsecureRedirect(anonymous, nil))
	})
	assert.Contains(t, stderr, "read from http://api.example over plaintext")

	// A hop that stays on https carries no such warning, so the one above
	// means the downgrade rather than the redirect.
	secure, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.example/repos", nil)
	require.NoError(t, err)
	assert.Empty(t, captureStderr(t, func() {
		require.NoError(t, refuseInsecureRedirect(secure, nil))
	}))

	var chain []*http.Request
	for range maxStorageSchemaRedirects {
		chain = append(chain, anonymous)
	}
	err = refuseInsecureRedirect(anonymous, chain)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped after 10 redirects")
}

// A command that names no release asks the target about its own embedded
// schema: resolving reads nothing, fetches nothing, and needs no dialect. An
// apply's preview is the caller that does this — the schema a convergence runs
// is the answering binary's own, so it is asked rather than handed a copy.
func TestStorageSchemaSourceFlags_ResolveTargetsOwnSchema(t *testing.T) {
	desired, err := (&storageSchemaSourceFlags{}).resolve(t.Context(), func() (schema.Dialect, error) {
		t.Fatal("the dialect must not be resolved for the answering binary's own schema")
		return "", nil
	})
	require.NoError(t, err)
	assert.Nil(t, desired, "a nil source is the answering binary's own embedded schema")
}

// A directory of .sql files is read as given, and the report attributes the
// answer to the directory by path — an operator running two diffs from two
// checkouts has to be able to tell the answers apart.
func TestStorageSchemaFromDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "applies.sql"),
		[]byte("CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "checks.sql"),
		[]byte("CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("not schema"), 0o600))

	desired, err := (&storageSchemaSourceFlags{SchemaDir: dir}).resolve(t.Context(), mysqlDialect)
	require.NoError(t, err)
	require.NotNil(t, desired)
	assert.Equal(t, fmt.Sprintf("the schema files in %s", dir), desired.Description)
	assert.Len(t, desired.Files, 2, "only .sql files are schema")
	assert.Contains(t, desired.Files["applies.sql"], "CREATE TABLE `applies`")
	assert.NotContains(t, desired.Files, "README.md")
}

// A directory with no .sql files is refused. A diff against an empty schema
// would report every existing storage table as surplus, which reads as a
// storage database that needs destroying rather than as a mistyped path.
func TestStorageSchemaFromDirectory_RefusesEmptyDirectory(t *testing.T) {
	_, err := (&storageSchemaSourceFlags{SchemaDir: t.TempDir()}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .sql files in --schema-dir")
	assert.Contains(t, err.Error(), "one .sql file per storage table")
}

// Pointing one level too high is the likeliest mistake, so the refusal names
// the per-dialect directories that are actually there rather than restating
// that the path was wrong.
func TestStorageSchemaFromDirectory_NamesDialectSubdirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, string(schema.DialectMySQL)), 0o750))
	require.NoError(t, os.Mkdir(filepath.Join(dir, string(schema.DialectPostgres)), 0o750))

	_, err := (&storageSchemaSourceFlags{SchemaDir: dir}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), filepath.Join(dir, "mysql"))
	assert.Contains(t, err.Error(), filepath.Join(dir, "postgres"))
}

// A missing directory fails naming the path, rather than resolving to an empty
// schema.
func TestStorageSchemaFromDirectory_RefusesMissingDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-a-checkout")
	_, err := (&storageSchemaSourceFlags{SchemaDir: missing}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), missing)
}

// releaseSchemaServer stands in for the repository API: it serves a listing for
// the dialect's schema directory and the raw contents of each file, and records
// the refs it was asked for so a test can assert the tag was honored.
func releaseSchemaServer(t *testing.T, directory string, files map[string]string) (*httptest.Server, *[]string) {
	t.Helper()
	refs := make([]string, 0, len(files)+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refs = append(refs, r.URL.Query().Get("ref"))
		prefix := "/repos/example/schemabot/contents/"
		path := strings.TrimPrefix(r.URL.Path, prefix)
		if path == directory {
			entries := make([]string, 0, len(files))
			for name := range files {
				entries = append(entries, fmt.Sprintf(`{"name":%q,"path":%q,"type":"file"}`, name, directory+"/"+name))
			}
			entries = append(entries, fmt.Sprintf(`{"name":"nested","path":%q,"type":"dir"}`, directory+"/nested"))
			_, err := fmt.Fprintf(w, "[%s]", strings.Join(entries, ","))
			assert.NoError(t, err)
			return
		}
		content, ok := files[strings.TrimPrefix(path, directory+"/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, err := w.Write([]byte(content))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	t.Setenv("GITHUB_API_URL", server.URL)
	return server, &refs
}

// A release's schema comes from the files at that tag, for the dialect the live
// storage runs — the same directory that release's binary embedded. The report
// attributes the answer to the release, never to the binary that answered.
func TestStorageSchemaFromRelease(t *testing.T) {
	_, refs := releaseSchemaServer(t, "pkg/schema/mysql", map[string]string{
		"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
		"checks.sql":  "CREATE TABLE `checks` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
	})

	flags := &storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/schemabot"}
	desired, err := flags.resolve(t.Context(), mysqlDialect)
	require.NoError(t, err)
	require.NotNil(t, desired)
	assert.Equal(t, "the schema files of release v1.4.0 in example/schemabot", desired.Description)
	assert.Len(t, desired.Files, 2, "the listing's directory entry is not a schema file")
	assert.Contains(t, desired.Files["checks.sql"], "CREATE TABLE `checks`")
	for _, ref := range *refs {
		assert.Equal(t, "v1.4.0", ref, "every read is pinned to the tag that was asked for")
	}
}

// The dialect decides which of a release's schema directories to read, so a
// PostgreSQL storage is never diffed against MySQL DDL that happened to be
// published alongside it.
func TestStorageSchemaFromRelease_FetchesTheStorageDialectsFiles(t *testing.T) {
	releaseSchemaServer(t, "pkg/schema/postgres", map[string]string{
		"applies.sql": `CREATE TABLE "applies" (id bigserial PRIMARY KEY)`,
	})

	flags := &storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/schemabot"}
	desired, err := flags.resolve(t.Context(), func() (schema.Dialect, error) { return schema.DialectPostgres, nil })
	require.NoError(t, err)
	require.NotNil(t, desired)
	assert.Contains(t, desired.Files["applies.sql"], `CREATE TABLE "applies"`)

	_, err = flags.resolve(t.Context(), mysqlDialect)
	require.Error(t, err, "the MySQL directory is not published by this fixture")
	assert.Contains(t, err.Error(), "pkg/schema/mysql")
}

// A tag that does not exist is an error naming the tag. The one answer it must
// never produce is an empty schema, which diffs as "every storage table is
// surplus".
func TestStorageSchemaFromRelease_RefusesUnknownTag(t *testing.T) {
	releaseSchemaServer(t, "pkg/schema/mysql", map[string]string{})

	_, err := (&storageSchemaSourceFlags{Release: "v9.9.9", Repo: "example/schemabot"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v9.9.9")
	assert.Contains(t, err.Error(), "no .sql files")
}

// A repository the caller cannot read names the token to set and the offline
// alternative, because both are decisions the operator makes at the terminal
// mid-deploy.
func TestStorageSchemaFromRelease_RefusalNamesTheRemedy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	t.Setenv("GITHUB_API_URL", server.URL)

	_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/private"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GITHUB_TOKEN")
	assert.Contains(t, err.Error(), "--schema-dir")
}

// GitHub reports a repository the caller may not read as missing, so the 404
// remediation has to name the token as well as the tag. An operator whose only
// problem is an unset GITHUB_TOKEN would otherwise spend a deploy window
// re-checking a tag that was never wrong.
func TestStorageSchemaFromRelease_NotFoundNamesTheTokenToo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	t.Setenv("GITHUB_API_URL", server.URL)

	_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/private"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check the tag spelling")
	assert.Contains(t, err.Error(), "GITHUB_TOKEN")
}

// A rate limit and a permission failure arrive as the same status, and only the
// headers separate them. They get different remediations because they have
// different fixes: an operator told to re-issue a token mid-deploy will retry
// with a new one and be refused again, while waiting or authenticating for the
// larger budget is what actually clears it.
func TestStorageSchemaFromRelease_RateLimitIsNotAPermissionFailure(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		headers map[string]string
	}{
		{"primary limit spent", http.StatusForbidden, map[string]string{"X-RateLimit-Remaining": "0"}},
		{"secondary limit", http.StatusTooManyRequests, map[string]string{"Retry-After": "60"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(server.Close)
			t.Setenv("GITHUB_API_URL", server.URL)

			_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/schemabot"}).resolve(t.Context(), mysqlDialect)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "rate-limited")
			assert.Contains(t, err.Error(), "--schema-dir")
			assert.NotContains(t, err.Error(), "not allowed to read",
				"a spent budget is not a permission the operator can grant")
		})
	}
}

// A permission failure with budget left is still a permission failure: the
// remediation is the token, not the wait.
func TestStorageSchemaFromRelease_PermissionFailureWithBudgetLeft(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "4999")
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	t.Setenv("GITHUB_API_URL", server.URL)

	_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/private"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed to read")
	assert.NotContains(t, err.Error(), "rate-limited")
}

// The fetch is authorized when a token is available, so a private mirror works
// without a flag of its own.
func TestStorageSchemaFromRelease_SendsTheToken(t *testing.T) {
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		_, err := w.Write([]byte(`[]`))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	t.Setenv("GITHUB_API_URL", server.URL)
	t.Setenv("GITHUB_TOKEN", "fetch-token")

	_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/private"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err, "an empty listing is still refused")
	assert.Equal(t, "Bearer fetch-token", authorization)
}

// A token is never sent to a plaintext host that is not loopback. $GITHUB_API_URL
// is whatever the operator's environment set — a mirror, an enterprise host, a
// proxy — and a bearer credential on an http:// path is readable by anyone on
// it, so the fetch is refused before the request is made rather than after the
// token is on the wire.
func TestStorageSchemaRelease_RefusesTokenOverPlaintext(t *testing.T) {
	t.Setenv("GITHUB_API_URL", "http://ghe.example")
	t.Setenv("GITHUB_TOKEN", "fetch-token")

	_, err := (&storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/private"}).resolve(t.Context(), mysqlDialect)
	require.Error(t, err)
	assert.ErrorIs(t, err, cmdclient.ErrInsecureTokenTransport)
	assert.Contains(t, err.Error(), "GITHUB_API_URL is http://ghe.example")
	assert.Contains(t, err.Error(), "unset GITHUB_TOKEN and GH_TOKEN")
}

// An unauthenticated fetch over plaintext is allowed and said out loud. There
// is no token to leak, and a plaintext mirror is a legitimate thing to point
// $GITHUB_API_URL at — but the desired side of the diff then arrives over a
// channel anyone on the path can rewrite, and a report built on a rewritten
// schema reads exactly like a real one.
//
// The warning is exercised where it is decided rather than through a fetch:
// an off-box hostname would make this a live DNS lookup and a dial, which the
// unit layer does not do, and a resolver that wildcards unknown names would
// hang it for the whole fetch budget. The call site is covered by
// TestStorageSchemaRelease_QuietOnLoopback, which fetches for real and asserts
// the warning stays off.
func TestStorageSchemaRelease_WarnsOnPlaintextWithoutAToken(t *testing.T) {
	t.Setenv("GITHUB_API_URL", "http://ghe.example")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")

	stderr := captureStderr(t, warnIfReleaseHostIsPlaintext)
	assert.Contains(t, stderr, "read from http://ghe.example over plaintext")
	assert.Contains(t, stderr, "treat this report as unverified")
}

// A token turns the same host into a refusal at the request that would carry
// it, so the warning does not also fire — an operator told "treat this as
// unverified" about a fetch that never happened is being told about the wrong
// thing.
func TestStorageSchemaRelease_QuietOnPlaintextWithAToken(t *testing.T) {
	t.Setenv("GITHUB_API_URL", "http://ghe.example")
	t.Setenv("GITHUB_TOKEN", "fetch-token")

	assert.Empty(t, captureStderr(t, warnIfReleaseHostIsPlaintext))
}

// The warning follows the token refusal's definition of an insecure host, so a
// loopback mirror — which every test in this file runs against — does not
// carry it. There is no network path for anything to sit on.
func TestStorageSchemaRelease_QuietOnLoopback(t *testing.T) {
	_, refs := releaseSchemaServer(t, "pkg/schema/mysql", map[string]string{
		"applies.sql": "CREATE TABLE `applies` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)",
	})
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")

	stderr := captureStderr(t, func() {
		flags := &storageSchemaSourceFlags{Release: "v1.4.0", Repo: "example/schemabot"}
		source, err := flags.resolve(t.Context(), mysqlDialect)
		require.NoError(t, err)
		require.NotNil(t, source)
	})
	assert.Empty(t, stderr, "a loopback mirror is not a plaintext network path")
	assert.NotEmpty(t, *refs, "the fetch really ran")
}

// A repository and a path are both multi-segment, and their separators are
// structure rather than content: escaping them would point every fetch at a
// path that does not exist. A segment carrying a character the path grammar
// reserves is still encoded.
func TestEscapePathSegments(t *testing.T) {
	assert.Equal(t, "block/schemabot", escapePathSegments("block/schemabot"),
		"an owner/name repository keeps its separator")
	assert.Equal(t, "pkg/schema/mysql", escapePathSegments("pkg/schema/mysql"),
		"a directory path keeps its separators")
	assert.Equal(t, "block/schema%20bot", escapePathSegments("block/schema bot"),
		"a reserved character inside one segment is encoded")
	assert.Equal(t, "", escapePathSegments(""))
}

// A convergence runs the schema embedded in the binary running it, so the
// diff's release selectors are refused on `storage apply` — with the two ways
// to converge a release named, since that is what the operator is reaching
// for.
func TestStorageSchemaSourceRefusal(t *testing.T) {
	require.NoError(t, storageSchemaSourceRefusal("", ""))

	release := storageSchemaSourceRefusal("", "v1.4.0")
	require.Error(t, release)
	assert.Contains(t, release.Error(), "--release cannot be used with a convergence")
	assert.Contains(t, release.Error(), "run that release's binary")
	assert.Contains(t, release.Error(), "storage plan")

	dir := storageSchemaSourceRefusal("./schema/mysql", "")
	require.Error(t, dir)
	assert.Contains(t, dir.Error(), "--schema-dir cannot be used with a convergence")
}
