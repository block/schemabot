// Package schema contains embedded SQL files for LocalScale metadata tables.
// These are the source of truth for the metadata schema — changes here are
// automatically applied on server startup via ddl.NewDiffer().
package schema

import "embed"

// FS contains the embedded SQL schema files for LocalScale metadata tables.
//
//go:embed *.sql
var FS embed.FS
