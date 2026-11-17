package engine

import "regexp"

// ExemptReasonArchiveNaming is the exemption reason a plan carries for an
// undeclared live table whose name marks it as an archive: the table is left
// in place rather than dropped, and left out of the rollback baseline.
const ExemptReasonArchiveNaming = "archive naming"

// archiveTableRegexp matches table names following the archive convention:
// <name>_archive_YYYY, <name>_archive_YYYY_MM, or <name>_archive_YYYY_MM_DD.
var archiveTableRegexp = regexp.MustCompile(`^.*_archive_[0-9]{4}(_[0-9]{2}(_[0-9]{2})?)?$`)

// IsArchiveTable reports whether a table name follows the archive naming
// convention: <name>_archive_YYYY, <name>_archive_YYYY_MM, or
// <name>_archive_YYYY_MM_DD. An archive is a retired copy kept outside
// declarative schema files, so every engine leaves an undeclared one in place.
//
// The convention is the one Spirit's schema loader applies when the MySQL
// engine reads a target, which that engine keeps using so its own filter and
// the loader's never disagree. Engines that do not load schemas through Spirit
// use this predicate, and a test in the Spirit engine pins the two together.
func IsArchiveTable(name string) bool {
	return archiveTableRegexp.MatchString(name)
}
