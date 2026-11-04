package schema

import (
	"fmt"
	"path"
	"strings"
)

// EntryKind is what a schema source holds at one path, read without following
// a symlink at that path.
type EntryKind int

const (
	// EntryMissing means nothing exists at the path.
	EntryMissing EntryKind = iota
	// EntryDirectory is a real directory.
	EntryDirectory
	// EntrySymlink is a symbolic link; Entry.SymlinkTarget holds its target.
	EntrySymlink
	// EntryOther is anything else, such as a regular file.
	EntryOther
)

// Entry describes one path in a schema source.
type Entry struct {
	Kind          EntryKind
	SymlinkTarget string
}

// EntryLookup reports the entry at a slash-separated path. A pull request
// reads it from the repository at the PR's head commit and the CLI from the
// local filesystem; both resolve environment schema roots through
// ResolveEnvironmentSchemaRoot, so the two read the same schema for an
// environment from the same tree.
type EntryLookup func(entryPath string) (Entry, error)

// ResolveEnvironmentSchemaRoot returns the directory holding environment's
// schema files for the config in configDir, as a slash-separated path.
//
// When configDir has an entry named for the environment, that entry is the
// environment's schema root: a real directory is used as is, and a symlink is
// followed to its target, which must be a directory inside configDir. With no
// such entry, configDir itself is the schema root for every environment. An
// empty environment always reads configDir.
//
// linkPath is the environment symlink the root was resolved through, empty
// when it was not.
func ResolveEnvironmentSchemaRoot(configDir, environment string, lookup EntryLookup) (schemaRoot, linkPath string, err error) {
	if environment == "" {
		return configDir, "", nil
	}
	if err := ValidateEnvironmentPathSegment(environment); err != nil {
		return "", "", err
	}

	candidate := path.Join(configDir, environment)
	entry, err := lookup(candidate)
	if err != nil {
		return "", "", fmt.Errorf("resolve schema root for environment %s at %s: %w", environment, candidate, err)
	}
	switch entry.Kind {
	case EntryMissing:
		return configDir, "", nil
	case EntryDirectory:
		return candidate, "", nil
	case EntrySymlink:
	default:
		return "", "", fmt.Errorf("environment schema root %s is not a directory or symlink", candidate)
	}

	resolved, err := resolveEnvironmentSymlinkTarget(configDir, candidate, entry.SymlinkTarget)
	if err != nil {
		return "", "", err
	}
	if err := refuseSymlinkedIntermediates(configDir, candidate, resolved, lookup); err != nil {
		return "", "", err
	}
	target, err := lookup(resolved)
	if err != nil {
		return "", "", fmt.Errorf("resolve environment schema root symlink %s target %s: %w", candidate, resolved, err)
	}
	if target.Kind != EntryDirectory {
		return "", "", fmt.Errorf("environment schema root symlink %s points to non-directory path %s", candidate, resolved)
	}
	return resolved, candidate, nil
}

// refuseSymlinkedIntermediates requires every directory between configDir and
// an environment symlink's resolved target to be a real directory. The
// containment check on the target is lexical, so a symlink on the way would
// let the target land outside configDir while still reading as inside it. A
// pull request could not follow it either: the Contents API does not traverse
// symlinked directories.
func refuseSymlinkedIntermediates(configDir, symlinkPath, resolved string, lookup EntryLookup) error {
	relative := relativeWithinDirectory(configDir, resolved)
	if relative == "" {
		return nil
	}
	segments := strings.Split(relative, "/")
	current := path.Clean(configDir)
	for _, segment := range segments[:len(segments)-1] {
		current = path.Join(current, segment)
		entry, err := lookup(current)
		if err != nil {
			return fmt.Errorf("resolve environment schema root symlink %s target %s at %s: %w", symlinkPath, resolved, current, err)
		}
		if entry.Kind != EntryDirectory {
			return fmt.Errorf("environment schema root symlink %s target %s runs through %s, which is not a directory; point the symlink at the real directory", symlinkPath, resolved, current)
		}
	}
	return nil
}

// relativeWithinDirectory returns candidate's slash-separated path below
// directory, empty when candidate is directory itself. The caller has already
// established that candidate lies within directory.
func relativeWithinDirectory(directory, candidate string) string {
	directory = path.Clean(directory)
	candidate = path.Clean(candidate)
	switch {
	case candidate == directory:
		return ""
	case directory == ".":
		return candidate
	case directory == "/":
		return strings.TrimPrefix(candidate, "/")
	default:
		return strings.TrimPrefix(candidate, directory+"/")
	}
}

// ValidateEnvironmentPathSegment refuses an environment name that would
// address anything other than one entry directly inside the config directory.
func ValidateEnvironmentPathSegment(environment string) error {
	if environment == "." || environment == ".." || strings.ContainsAny(environment, `/\`) {
		return fmt.Errorf("environment %q must be a single path segment", environment)
	}
	return nil
}

// resolveEnvironmentSymlinkTarget resolves an environment symlink's target
// relative to the directory holding the symlink, and refuses a target that is
// absolute or leaves configDir: the environment's schema must be reviewed
// alongside the config that governs it.
func resolveEnvironmentSymlinkTarget(configDir, symlinkPath, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("environment schema root symlink %s has empty target", symlinkPath)
	}
	if strings.HasPrefix(target, "/") {
		return "", fmt.Errorf("environment schema root symlink %s points to absolute path %s", symlinkPath, target)
	}

	resolved := path.Clean(path.Join(path.Dir(symlinkPath), target))
	if !PathWithinDirectory(configDir, resolved) {
		return "", fmt.Errorf("environment schema root symlink %s points outside schema directory: %s", symlinkPath, target)
	}
	return resolved, nil
}

// PathWithinDirectory reports whether the slash-separated candidate is
// directory itself or lies beneath it. A directory of "." contains every
// relative path that does not climb out of it.
func PathWithinDirectory(directory, candidate string) bool {
	directory = path.Clean(directory)
	candidate = path.Clean(candidate)
	if directory == "." {
		return candidate == "." || candidate != ".." && !strings.HasPrefix(candidate, "../") && !strings.HasPrefix(candidate, "/")
	}
	if directory == "/" {
		return strings.HasPrefix(candidate, "/")
	}
	return candidate == directory || strings.HasPrefix(candidate, directory+"/")
}
