// Package livee2e drives a live SchemaBot deployment through a real pull
// request: it opens a schema change, applies it with PR comments, exercises
// stop, start, cancel, and a deferred cutover, and checks the test database
// and the Check Run after each step. A deploy gate runs it against a freshly
// deployed SchemaBot to prove the release works end to end.
package livee2e
