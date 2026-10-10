package webhook

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// scopeFixtureClient serves a repository whose schemabot.yaml configs sit at the
// given directories, each declaring the database it is mapped to, so config
// discovery over a changed-file list resolves the databases a command would
// reach. serveErrors makes every read fail instead.
func scopeFixtureClient(t *testing.T, configs map[string]string, serveErrors bool) *ghclient.InstallationClient {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		if serveErrors {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		path := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]
		database, ok := configs[path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":"schemabot.yaml","path":%q,"content":%q,"encoding":"base64"}`,
			path, base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "database: %s\ntype: mysql\n", database)))
	})

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	return ghclient.NewInstallationClient(client, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
}

func changedFiles(paths ...string) []ghclient.PRFile {
	files := make([]ghclient.PRFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, ghclient.PRFile{Filename: path, Status: "modified"})
	}
	return files
}

// A bare command offered by an auto-plan comment is resolved by the command
// path, over the pull request's raw changed files and across every deployment's
// configs. The count that decides whether the comment names its database has to
// come from that set: this deployment's own planning slice is narrower, and a
// bare command chosen from it can reach a database the comment is not about.
func TestAutoPlanCommandScopeDatabases(t *testing.T) {
	h := &Handler{logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))}

	twoConfigs := map[string]string{
		"schema/orders/schemabot.yaml":  "orders",
		"schema/billing/schemabot.yaml": "billing",
	}

	t.Run("one database needs no flag", func(t *testing.T) {
		client := scopeFixtureClient(t, twoConfigs, false)
		count := h.autoPlanCommandScopeDatabases(t.Context(), client, "octocat/hello-world", 1, "abc123",
			changedFiles("schema/orders/users.sql"))
		assert.Equal(t, 1, count)
	})

	t.Run("a file the PR did not propose still counts", func(t *testing.T) {
		// The command path discovers over the raw changed files, so a file
		// auto-plan drops as inherited is still one the pasted command sees.
		client := scopeFixtureClient(t, twoConfigs, false)
		count := h.autoPlanCommandScopeDatabases(t.Context(), client, "octocat/hello-world", 1, "abc123",
			changedFiles("schema/orders/users.sql", "schema/billing/invoices.sql"))
		assert.Equal(t, 2, count, "two databases in the raw file list make a bare command ambiguous")
	})

	t.Run("several configs for one database count once", func(t *testing.T) {
		client := scopeFixtureClient(t, map[string]string{
			"schema/orders/schemabot.yaml":    "orders",
			"schema/orders/eu/schemabot.yaml": "orders",
		}, false)
		count := h.autoPlanCommandScopeDatabases(t.Context(), client, "octocat/hello-world", 1, "abc123",
			changedFiles("schema/orders/users.sql", "schema/orders/eu/users.sql"))
		assert.Equal(t, 1, count, "the flag names a database, so duplicate configs for one are not ambiguous")
	})

	t.Run("a discovery failure names the database", func(t *testing.T) {
		client := scopeFixtureClient(t, twoConfigs, true)
		count := h.autoPlanCommandScopeDatabases(t.Context(), client, "octocat/hello-world", 1, "abc123",
			changedFiles("schema/orders/users.sql"))
		assert.Greater(t, count, 1, "an unanswerable question degrades to the scoped rendering, which always resolves")
	})
}
