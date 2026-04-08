#!/bin/bash
set -euo pipefail

# Post-worktree setup: ensures hooks, dependencies, and tooling are ready.
# Run this after `git worktree add` to initialize a new worktree.

cd "$(git rev-parse --show-toplevel)"

make setup
