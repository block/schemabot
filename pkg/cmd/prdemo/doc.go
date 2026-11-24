// Package prdemo holds the reproducible, fictional PR comments used by the PR
// workflow recordings: the multi-target workflow and the PostgreSQL workflow.
// The executable generators are excluded from normal builds;
// scripts/prepare-pr-multi-target-demo.py and scripts/prepare-pr-postgres-demo.py
// run them explicitly against the current production webhook templates and
// render the result.
package prdemo
