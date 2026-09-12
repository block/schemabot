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
