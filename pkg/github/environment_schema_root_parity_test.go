package github

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

const parityItemsSQL = "CREATE TABLE items (id bigint primary key);\n"

// A pull request and a local `schemabot plan -s <config dir> -e <env>` read
// the same schema for an environment from the same tree. Each case lays the
// tree out once on disk, serves it to the pull request flow through a GitHub
// Contents API that reads that disk, and requires the namespaces and files the
// two flows group to be identical.
func TestEnvironmentSchemaRootMatchesBetweenPullRequestAndCLI(t *testing.T) {
	cases := []struct {
		name        string
		layout      func(t *testing.T, configDir string)
		environment string
		wantRoot    string
		want        map[string]map[string]string
	}{
		{
			name: "symlinked environment root",
			layout: func(t *testing.T, configDir string) {
				writeParityFile(t, filepath.Join(configDir, "base", "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")
				require.NoError(t, os.Symlink("base", filepath.Join(configDir, "production")))
			},
			environment: "production",
			wantRoot:    "schema/base",
			want: map[string]map[string]string{
				"orders_001": {"orders.sql": "CREATE TABLE orders (id bigint primary key);\n"},
			},
		},
		{
			name:        "environment directory of namespace symlinks, staging",
			layout:      writeParityNamespaceSymlinkEnvironmentRoots,
			environment: "staging",
			wantRoot:    "schema/staging",
			want: map[string]map[string]string{
				"app_staging":   {"items.sql": parityItemsSQL},
				"app_1_staging": {"items.sql": parityItemsSQL},
			},
		},
		{
			name:        "environment directory of namespace symlinks, production",
			layout:      writeParityNamespaceSymlinkEnvironmentRoots,
			environment: "production",
			wantRoot:    "schema/production",
			want: map[string]map[string]string{
				"app_production": {"items.sql": parityItemsSQL},
			},
		},
		{
			name: "no environment root falls back to the config directory",
			layout: func(t *testing.T, configDir string) {
				writeParityFile(t, filepath.Join(configDir, "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")
			},
			environment: "staging",
			wantRoot:    "schema",
			want: map[string]map[string]string{
				"orders_001": {"orders.sql": "CREATE TABLE orders (id bigint primary key);\n"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := t.TempDir()
			configDir := filepath.Join(repoRoot, "schema")
			writeParityFile(t, filepath.Join(configDir, "schemabot.yaml"), "database: orders\ntype: mysql\n")
			tc.layout(t, configDir)

			prRoot, prFiles := pullRequestEnvironmentSchema(t, repoRoot, "schema", tc.environment)
			assert.Equal(t, tc.wantRoot, prRoot)
			assert.Equal(t, tc.want, prFiles)

			cliRoot, err := client.ResolveEnvironmentSchemaRoot(configDir, tc.environment)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(repoRoot, filepath.FromSlash(tc.wantRoot)), cliRoot)
			cliGrouped, _, err := client.ReadSchemaFiles(cliRoot, tc.environment, nil)
			require.NoError(t, err)
			cliFiles := make(map[string]map[string]string, len(cliGrouped))
			for namespace, files := range cliGrouped {
				cliFiles[namespace] = files.Files
			}
			assert.Equal(t, prFiles, cliFiles, "the CLI must plan the schema the pull request plans")
		})
	}
}

// Both flows refuse an environment symlink whose target is outside the config
// directory, whether the target names the outside path or reaches it through
// a symlinked directory that only looks inside.
func TestEnvironmentSchemaRootOutsideConfigDirectoryRefusedByPullRequestAndCLI(t *testing.T) {
	cases := []struct {
		name   string
		layout func(t *testing.T, configDir string)
		want   string
	}{
		{
			name: "target outside the config directory",
			layout: func(t *testing.T, configDir string) {
				require.NoError(t, os.Symlink(filepath.Join("..", "other"), filepath.Join(configDir, "production")))
			},
			want: "points outside schema directory",
		},
		{
			name: "target reached through a symlinked directory",
			layout: func(t *testing.T, configDir string) {
				require.NoError(t, os.Symlink(filepath.Join("bridge", "base"), filepath.Join(configDir, "production")))
				require.NoError(t, os.Symlink(filepath.Join("..", "other"), filepath.Join(configDir, "bridge")))
			},
			want: "schema/bridge, which is not a directory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := t.TempDir()
			configDir := filepath.Join(repoRoot, "schema")
			writeParityFile(t, filepath.Join(configDir, "schemabot.yaml"), "database: orders\ntype: mysql\n")
			writeParityFile(t, filepath.Join(repoRoot, "other", "base", "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")
			tc.layout(t, configDir)

			ic := diskBackedInstallationClient(t, repoRoot)
			_, _, prErr := ic.resolveSchemaRootForEnvironment(t.Context(), "octocat/hello-world", "abc123", "schema", "production")
			require.Error(t, prErr)
			assert.Contains(t, prErr.Error(), tc.want)

			_, cliErr := client.ResolveEnvironmentSchemaRoot(configDir, "production")
			require.Error(t, cliErr)
			assert.Contains(t, cliErr.Error(), tc.want)
		})
	}
}

// pullRequestEnvironmentSchema runs the pull request flow's schema discovery
// for environment against the tree at repoRoot: resolve the environment's
// schema root, fetch its files, and group them by namespace.
func pullRequestEnvironmentSchema(t *testing.T, repoRoot, configDir, environment string) (string, map[string]map[string]string) {
	t.Helper()
	ic := diskBackedInstallationClient(t, repoRoot)
	schemaRoot, _, err := ic.resolveSchemaRootForEnvironment(t.Context(), "octocat/hello-world", "abc123", configDir, environment)
	require.NoError(t, err)
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "abc123", schemaRoot, "mysql")
	require.NoError(t, err)
	grouped, _, err := groupFilesByNamespace(files, schemaRoot, environment, nil)
	require.NoError(t, err)
	result := make(map[string]map[string]string, len(grouped))
	for namespace, schemaFiles := range grouped {
		result[namespace] = schemaFiles.Files
	}
	return schemaRoot, result
}

// diskBackedInstallationClient serves the repository at repoRoot through a
// GitHub Contents API with GitHub's symlink behavior: a symlink fetched
// directly returns its own object and target, a directory listing names a
// symlink without its target, and a path that runs through a symlink is not
// found.
func diskBackedInstallationClient(t *testing.T, repoRoot string) *InstallationClient {
	t.Helper()
	ghClient, mux := setupConfigTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/{path...}", func(w http.ResponseWriter, r *http.Request) {
		repoPath := strings.Trim(r.PathValue("path"), "/")
		content, listing, found := diskRepositoryContent(t, repoRoot, repoPath)
		switch {
		case !found:
			w.WriteHeader(http.StatusNotFound)
			_, err := w.Write([]byte(`{"message":"Not Found"}`))
			require.NoError(t, err)
		case listing != nil:
			require.NoError(t, json.NewEncoder(w).Encode(listing))
		default:
			require.NoError(t, json.NewEncoder(w).Encode(content))
		}
	})
	return NewInstallationClient(ghClient, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func diskRepositoryContent(t *testing.T, repoRoot, repoPath string) (*gh.RepositoryContent, []*gh.RepositoryContent, bool) {
	t.Helper()
	segments := strings.Split(repoPath, "/")
	for i := 1; i < len(segments); i++ {
		info, err := os.Lstat(filepath.Join(repoRoot, filepath.FromSlash(path.Join(segments[:i]...))))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, false
		}
		require.NoError(t, err)
		if info.Mode()&fs.ModeSymlink != 0 {
			return nil, nil, false
		}
	}

	localPath := filepath.Join(repoRoot, filepath.FromSlash(repoPath))
	info, err := os.Lstat(localPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, false
	}
	require.NoError(t, err)
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(localPath)
		require.NoError(t, err)
		slashTarget := filepath.ToSlash(target)
		return &gh.RepositoryContent{Type: new("symlink"), Path: new(repoPath), Target: &slashTarget}, nil, true
	case info.IsDir():
		entries, err := os.ReadDir(localPath)
		require.NoError(t, err)
		listing := make([]*gh.RepositoryContent, 0, len(entries))
		for _, entry := range entries {
			entryType := "file"
			switch {
			case entry.Type()&fs.ModeSymlink != 0:
				entryType = "symlink"
			case entry.IsDir():
				entryType = "dir"
			}
			listing = append(listing, &gh.RepositoryContent{Type: new(entryType), Name: new(entry.Name()), Path: new(path.Join(repoPath, entry.Name()))})
		}
		return nil, listing, true
	default:
		data, err := os.ReadFile(localPath)
		require.NoError(t, err)
		encoded := base64.StdEncoding.EncodeToString(data)
		return &gh.RepositoryContent{Type: new("file"), Path: new(repoPath), Encoding: new("base64"), Content: &encoded}, nil, true
	}
}

// writeParityNamespaceSymlinkEnvironmentRoots lays out one shared table tree
// and two environment directories whose namespaces are symlinks into it:
//
//	schema/
//	├── schemabot.yaml
//	├── tables/app_{env}/items.sql
//	├── staging/
//	│   ├── app_{env}   -> ../tables/app_{env}
//	│   └── app_1_{env} -> ../tables/app_{env}
//	└── production/
//	    └── app_{env}   -> ../tables/app_{env}
func writeParityNamespaceSymlinkEnvironmentRoots(t *testing.T, configDir string) {
	t.Helper()
	writeParityFile(t, filepath.Join(configDir, "tables", "app_{env}", "items.sql"), parityItemsSQL)
	shared := filepath.Join("..", "tables", "app_{env}")
	require.NoError(t, os.MkdirAll(filepath.Join(configDir, "staging"), 0o755))
	require.NoError(t, os.Symlink(shared, filepath.Join(configDir, "staging", "app_{env}")))
	require.NoError(t, os.Symlink(shared, filepath.Join(configDir, "staging", "app_1_{env}")))
	require.NoError(t, os.MkdirAll(filepath.Join(configDir, "production"), 0o755))
	require.NoError(t, os.Symlink(shared, filepath.Join(configDir, "production", "app_{env}")))
}

func writeParityFile(t *testing.T, filePath, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
	require.NoError(t, os.WriteFile(filePath, []byte(content), 0o644))
}
