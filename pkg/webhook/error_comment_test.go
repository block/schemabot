package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// postCommandError must always render a non-empty timestamp in the comment
// footer. A regression of the original bug renders "at  UTC" (two spaces) and
// is what the helper exists to prevent.
func TestPostCommandError_RendersTimestamp(t *testing.T) {
	original := templates.NowFunc
	t.Cleanup(func() { templates.NowFunc = original })
	templates.NowFunc = func() time.Time {
		return time.Date(2026, 5, 26, 12, 34, 56, 0, time.UTC)
	}

	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	h.postCommandError(
		"octocat/hello-world", 1, 12345,
		"plan", "staging", "alice",
		"boom",
	)

	select {
	case body := <-bodies:
		assert.NotContains(t, body, "at  UTC",
			"comment must not render empty timestamp")
		assert.Contains(t, body, "at 2026-05-26 12:34:56 UTC",
			"comment must render the stubbed timestamp from templates.NowFunc")
		assert.Contains(t, body, "*Requested by @alice")
		assert.Contains(t, body, "Plan Failed")
		assert.Contains(t, body, "`staging`")
		assert.Contains(t, body, "> boom")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for posted comment")
	}
}

// userFacingError shows an error's own text only when the error marks it as
// written for the person who ran the command, and shows a fixed line for every
// other error, so a dial failure or a storage error never reaches the pull
// request with its addresses.
func TestUserFacingError(t *testing.T) {
	const retryLine = "SchemaBot could not complete the command. Run the command again; if it fails again, the SchemaBot server logs have the cause."
	dialFailure := errors.New("dial tcp 10.1.2.3:3306: connect: connection refused")

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "an authored error shows its text",
			err:  &api.DatabaseNotConfiguredError{Database: "orders"},
			want: `database "orders" is not configured on this server`,
		},
		{
			name: "a wrapped authored error shows only its own text",
			err:  fmt.Errorf("plan orders/staging: %w", &api.DatabaseNotConfiguredError{Database: "orders"}),
			want: `database "orders" is not configured on this server`,
		},
		{
			name: "authored text with line breaks becomes one line",
			err:  &engine.OperatorMessage{Message: "the copy of \"orders\"\r\nhas not started\n\n- retry later", Err: dialFailure},
			want: `the copy of "orders" has not started - retry later`,
		},
		{
			name: "authored text carrying an address is redacted",
			err:  &engine.OperatorMessage{Message: "could not reach 10.1.2.3:3306 for orders"},
			want: "could not reach [endpoint redacted] for orders",
		},
		{
			name: "authored text quoting a statement is flattened",
			err:  &engine.PlanRefusedError{Err: errors.New("the schema files drop table `orders_seq`:\nDROP TABLE `orders_seq`;\n")},
			want: "the schema files drop table `orders_seq`: DROP TABLE `orders_seq`;",
		},
		{
			name: "authored text that sanitizes to nothing shows the fixed line",
			err:  &engine.OperatorMessage{Message: " \n\t" + string(rune(0x202e)) + " "},
			want: retryLine,
		},
		{
			name: "no config",
			err:  fmt.Errorf("find config: %w", ghclient.ErrNoConfig),
			want: "No `schemabot.yaml` configuration file was found in this repository. Add one in the directory that holds the `.sql` files declaring your tables, or use `-d` to name a database that already has one.",
		},
		{
			name: "invalid config shows no path from the pull request",
			err:  fmt.Errorf("%w at %s", ghclient.ErrInvalidConfig, "services/orders/schemabot.yaml"),
			want: "A `schemabot.yaml` in this repository is not valid. It must include the `database` and `type` fields; fix the file and push a commit.",
		},
		{
			name: "multiple configs without candidates",
			err:  ghclient.ErrMultipleConfigs,
			want: "This repository has more than one `schemabot.yaml`, so SchemaBot cannot tell which database you mean. Use `-d` with the database name.",
		},
		{
			name: "multiple configs with candidates show the choices",
			err:  &ghclient.MultipleConfigsError{Candidates: []ghclient.ConfigCandidate{{Database: "orders", SchemaDir: "services/orders"}, {Database: "users", SchemaDir: "services/users"}}},
			want: "multiple schemabot.yaml configs found - use -d flag to specify database: `orders` (services/orders), `users` (services/users)",
		},
		{
			name: "config discovery on a truncated tree",
			err:  fmt.Errorf("discover schemabot configs in repo octocat/hello-world ref abc123: %w", ghclient.ErrConfigDiscoveryTruncated),
			want: "GitHub returned a truncated repository tree, so SchemaBot could not search this repository for `schemabot.yaml` configurations. Ask a SchemaBot operator to give each of this repository's databases an `allowed_dirs` entry naming its schema directory.",
		},
		{
			name: "a peer that cannot be read names the databases and the authored reason",
			err: &sharedSchemaPeerUnreadableError{
				Peer: "users", Database: "orders", Environment: "staging",
				Cause: &api.DatabaseNotConfiguredError{Database: "users"},
			},
			want: "SchemaBot could not read the schema files of `users`, which shares a target with `orders` in staging, so it could not compare their ignore_tables. " +
				`database "users" is not configured on this server`,
		},
		{
			name: "a peer that cannot be read for an unwritten reason names no cause",
			err: &sharedSchemaPeerUnreadableError{
				Peer: "users", Database: "orders", Environment: "staging", Cause: dialFailure,
			},
			want: "SchemaBot could not read the schema files of `users`, which shares a target with `orders` in staging, so it could not compare their ignore_tables.",
		},
		{
			name: "an untyped error shows the fixed line",
			err:  dialFailure,
			want: retryLine,
		},
		{
			name: "a wrapped untyped error shows the fixed line",
			err:  fmt.Errorf("load schema files: %w", dialFailure),
			want: retryLine,
		},
		{
			name: "a joined error holding an authored error shows the authored text",
			err:  errors.Join(dialFailure, &api.EnvironmentNotConfiguredError{Database: "orders", Environment: "staging"}),
			want: (&api.EnvironmentNotConfiguredError{Database: "orders", Environment: "staging"}).Error(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := userFacingError(tc.err)

			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "10.1.2.3")
		})
	}
}

// A failed plan comment carries the fixed line for an error SchemaBot did not
// write, with no part of the error's own text.
func TestPostPlanCommandErrorShowsTheFixedLineForAnUnwrittenError(t *testing.T) {
	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, bodies))

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	err := fmt.Errorf("load schema files: %w", errors.New("dial tcp 10.1.2.3:3306: connect: connection refused"))

	h.postPlanCommandError("octocat/hello-world", 1, 12345, "staging", "alice", err)

	body := requireComment(t, bodies, "plan failure comment")
	assert.Contains(t, body, "> SchemaBot could not complete the command. Run the command again; if it fails again, the SchemaBot server logs have the cause.")
	assert.NotContains(t, body, "10.1.2.3")
	assert.NotContains(t, body, "load schema files")
}

// A shared schema check that cannot learn which schemas a database's
// namespaces address fails the command with a fixed line naming the database,
// target, and deployment. The data plane's own error stays in the server logs,
// a data plane too old to answer is told to upgrade, and a data plane whose
// engine cannot resolve namespaces is told a retry will not help.
func TestUserFacingErrorHidesNamespaceResolutionCause(t *testing.T) {
	resolution := func(cause error) error {
		return fmt.Errorf("compare ignore_tables of %q with the databases sharing its targets in staging: %w", "shop",
			&api.NamespaceResolutionError{Database: "shop", Environment: "staging", Deployment: "east", Target: "shop-001", Namespaces: []string{"shop"}, Cause: cause})
	}

	t.Run("a data plane that predates the RPC", func(t *testing.T) {
		message := userFacingError(resolution(fmt.Errorf("resolve namespaces: %w", tern.ErrResolveNamespacesUnimplemented)))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, because that deployment's data plane cannot report which schema each namespace addresses. Upgrade that data plane, then run the command again.", message)
	})
	t.Run("an engine that cannot resolve namespaces", func(t *testing.T) {
		message := userFacingError(resolution(fmt.Errorf("resolve namespaces: %w", tern.ErrResolveNamespacesUnsupported)))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, because that deployment's data plane cannot resolve namespaces for this database's engine. Running the command again will not help; the SchemaBot server logs have the cause.", message)
	})
	t.Run("any other failure", func(t *testing.T) {
		message := userFacingError(resolution(errors.New("dial tcp db-internal.example:3306: connection refused")))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, so the command was refused. Run the command again; if it fails again, the SchemaBot server logs have the cause.", message)
		assert.NotContains(t, message, "db-internal")
	})
}
