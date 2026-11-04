package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEntries is a schema source holding the listed paths; every other path
// is missing.
func fakeEntries(entries map[string]Entry) EntryLookup {
	return func(entryPath string) (Entry, error) {
		if entry, ok := entries[entryPath]; ok {
			return entry, nil
		}
		return Entry{Kind: EntryMissing}, nil
	}
}

func TestResolveEnvironmentSchemaRoot(t *testing.T) {
	cases := []struct {
		name        string
		configDir   string
		entries     map[string]Entry
		environment string
		wantRoot    string
		wantLink    string
	}{
		{
			name:        "no environment reads the config directory",
			configDir:   "schema",
			entries:     map[string]Entry{"schema/production": {Kind: EntryDirectory}},
			environment: "",
			wantRoot:    "schema",
		},
		{
			name:        "missing environment entry falls back to the config directory",
			configDir:   "schema",
			environment: "production",
			wantRoot:    "schema",
		},
		{
			name:        "real environment directory",
			configDir:   "schema",
			entries:     map[string]Entry{"schema/production": {Kind: EntryDirectory}},
			environment: "production",
			wantRoot:    "schema/production",
		},
		{
			name:      "environment symlink to a sibling directory",
			configDir: "schema",
			entries: map[string]Entry{
				"schema/production": {Kind: EntrySymlink, SymlinkTarget: "base"},
				"schema/base":       {Kind: EntryDirectory},
			},
			environment: "production",
			wantRoot:    "schema/base",
			wantLink:    "schema/production",
		},
		{
			name:      "repository-root config",
			configDir: ".",
			entries: map[string]Entry{
				"production": {Kind: EntrySymlink, SymlinkTarget: "./base/"},
				"base":       {Kind: EntryDirectory},
			},
			environment: "production",
			wantRoot:    "base",
			wantLink:    "production",
		},
		{
			name:      "absolute config directory",
			configDir: "/repo/schema",
			entries: map[string]Entry{
				"/repo/schema/production": {Kind: EntrySymlink, SymlinkTarget: "base"},
				"/repo/schema/base":       {Kind: EntryDirectory},
			},
			environment: "production",
			wantRoot:    "/repo/schema/base",
			wantLink:    "/repo/schema/production",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, link, err := ResolveEnvironmentSchemaRoot(tc.configDir, tc.environment, fakeEntries(tc.entries))
			require.NoError(t, err)
			assert.Equal(t, tc.wantRoot, root)
			assert.Equal(t, tc.wantLink, link)
		})
	}
}

// Every shape that cannot be read as a directory inside the config directory
// refuses the plan rather than falling back to another tree.
func TestResolveEnvironmentSchemaRootRefusals(t *testing.T) {
	lookupErr := errors.New("listing unavailable")
	cases := []struct {
		name    string
		entries map[string]Entry
		lookup  EntryLookup
		want    string
	}{
		{
			name:    "environment entry is a file",
			entries: map[string]Entry{"schema/production": {Kind: EntryOther}},
			want:    "is not a directory or symlink",
		},
		{
			name:    "symlink with an empty target",
			entries: map[string]Entry{"schema/production": {Kind: EntrySymlink, SymlinkTarget: " "}},
			want:    "has empty target",
		},
		{
			name:    "symlink to an absolute path",
			entries: map[string]Entry{"schema/production": {Kind: EntrySymlink, SymlinkTarget: "/srv/schema/base"}},
			want:    "points to absolute path",
		},
		{
			name:    "symlink leaving the config directory",
			entries: map[string]Entry{"schema/production": {Kind: EntrySymlink, SymlinkTarget: "../other"}},
			want:    "points outside schema directory",
		},
		{
			name:    "symlink to a missing target",
			entries: map[string]Entry{"schema/production": {Kind: EntrySymlink, SymlinkTarget: "base"}},
			want:    "points to non-directory path schema/base",
		},
		{
			name: "symlink to another symlink",
			entries: map[string]Entry{
				"schema/production": {Kind: EntrySymlink, SymlinkTarget: "base"},
				"schema/base":       {Kind: EntrySymlink, SymlinkTarget: "shared"},
			},
			want: "points to non-directory path schema/base",
		},
		{
			name: "symlink target reached through a symlinked directory",
			entries: map[string]Entry{
				"schema/production": {Kind: EntrySymlink, SymlinkTarget: "bridge/base"},
				"schema/bridge":     {Kind: EntrySymlink, SymlinkTarget: "../other"},
			},
			want: "target schema/bridge/base runs through schema/bridge, which is not a directory",
		},
		{
			name:   "lookup failure",
			lookup: func(string) (Entry, error) { return Entry{}, lookupErr },
			want:   "resolve schema root for environment production at schema/production: listing unavailable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lookup := tc.lookup
			if lookup == nil {
				lookup = fakeEntries(tc.entries)
			}
			_, _, err := ResolveEnvironmentSchemaRoot("schema", "production", lookup)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestResolveEnvironmentSchemaRootRejectsPathTraversal(t *testing.T) {
	for _, environment := range []string{"../other", "prod/blue", ".", "..", `prod\blue`} {
		t.Run(environment, func(t *testing.T) {
			_, _, err := ResolveEnvironmentSchemaRoot("schema", environment, fakeEntries(nil))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be a single path segment")
		})
	}
}
