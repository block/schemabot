// Package livee2e drives a live SchemaBot deployment through a real pull
// request: it opens a schema change, applies it with PR comments, exercises
// stop, start, cancel, and a deferred cutover, and checks the test database
// and the Check Run after each step. A deploy gate runs it against a freshly
// deployed SchemaBot to prove the release works end to end.
//
// Run in a pod, it can also count the GitHub API calls SchemaBot made during
// the run (GitHubCallCounter), so the gate can hold a release that works but
// multiplies its GitHub calls. The count needs the SchemaBot replicas to stay
// fixed for the run: two reads cannot see a pod that started and went away
// between them.
package livee2e
