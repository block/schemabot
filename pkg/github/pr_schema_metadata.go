package github

import (
	"context"
	"fmt"
	"path"

	"github.com/block/schemabot/pkg/schema"
)

// schemaRevisionTree resolves schema layouts without reading SQL or VSchema
// blobs. Git tree levels are immutable and shared with other discovery calls
// on this installation. Only symlink blobs need a content read.
type schemaRevisionTree struct {
	client    *InstallationClient
	repo, ref string
	levels    map[string][]TreeEntry
}

func (t schemaRevisionTree) entry(ctx context.Context, name string) (schema.Entry, error) {
	entry, found, err := t.client.resolveGitTreeEntry(ctx, t.repo, t.ref, name, t.levels)
	if err != nil || !found {
		return schema.Entry{Kind: schema.EntryMissing}, err
	}
	switch {
	case entry.Type == "tree":
		return schema.Entry{Kind: schema.EntryDirectory}, nil
	case entry.Mode == gitSymlinkMode:
		target, err := t.client.FetchBlobContent(ctx, t.repo, entry.SHA)
		return schema.Entry{Kind: schema.EntrySymlink, SymlinkTarget: target}, err
	default:
		return schema.Entry{Kind: schema.EntryOther}, nil
	}
}

func (t schemaRevisionTree) directory(ctx context.Context, name string) ([]TreeEntry, error) {
	if name == "." {
		return t.client.treeLevel(ctx, t.repo, t.ref, t.levels)
	}
	sha, found, err := t.client.resolveGitTreeSHA(ctx, t.repo, t.ref, name, t.levels)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("schema input directory %s does not exist", name)
	}
	return t.client.treeLevel(ctx, t.repo, sha, t.levels)
}

func (t schemaRevisionTree) locators(ctx context.Context, root string) ([]schemaFileLocator, error) {
	entries, err := t.directory(ctx, root)
	if err != nil {
		return nil, err
	}
	var locators []schemaFileLocator
	for _, entry := range entries {
		logicalPath := path.Join(root, entry.Path)
		switch {
		case entry.Type == "tree", entry.Mode == gitSymlinkMode:
			source := logicalPath
			if entry.Mode == gitSymlinkMode {
				target, err := t.client.FetchBlobContent(ctx, t.repo, entry.SHA)
				if err != nil {
					return nil, err
				}
				source, err = resolveNamespaceSymlinkTarget(root, logicalPath, target)
				if err != nil {
					return nil, err
				}
			}
			children, err := t.directory(ctx, source)
			if err != nil {
				return nil, err
			}
			for _, child := range children {
				if child.Type == "blob" && child.Mode != gitSymlinkMode && isManagedSchemaFile(child.Path) {
					locators = append(locators, schemaFileLocator{
						fetchPath: path.Join(source, child.Path), displayPath: path.Join(logicalPath, child.Path),
					})
				}
			}
		case entry.Type == "blob" && isManagedSchemaFile(entry.Path):
			locators = append(locators, schemaFileLocator{fetchPath: logicalPath, displayPath: logicalPath})
		}
	}
	return locators, nil
}
