<!--
Write for a reviewer who has never read this code and has 30 seconds.
Short sentences, one physical line per paragraph (GitHub turns a newline
inside a paragraph into a line break). No test plans or checklists: CI
reports the tests. This is a public repo, so leave out internal names,
hosts, environments, and incident details. Delete these comments and any
section that does not apply. AGENTS.md (Git section) has the full rules.
-->

## Why

<!-- The problem: what SchemaBot does today and what is wrong with it. -->

## What

<!-- One line per change. Do not walk the diff file by file. -->

<!--
Diagram: required when the change has a shape (flow, state, lifecycle,
concurrency, ownership). Put it in a fenced code block. When it rewrites
behavior, draw Before and After in the same form, traced through one
concrete example named in the sentence above it, and mark where the old
behavior goes wrong. Delete this comment if the change has no shape.

UX changes (PR comments, check summaries, CLI output): add one collapsed
<details> block per scenario with the rendered output, plus the prior
rendering when it replaces existing output. Paste PR comment markdown bare,
not fenced; keep terminal output fenced.
-->

## How

<!-- Only the implementation choices a reviewer needs to judge the change. -->

## Risk

<!--
Blast radius in one or two sentences, and what an operator would see if it
went wrong. Name every invariant the change touches by ID (docs/invariants.md)
and say whether it upholds, extends, establishes, or weakens it. A change
that weakens one says so here, first, with the reason.
-->

## Upgrade notes

<!--
Anything an operator must do before or after upgrading: config keys added,
removed, or with a new default; new GitHub App permissions and the config key
that needs each; deploy ordering; storage schema changes. The release notes
copy this. Write "None" when the change asks nothing of operators.
-->

<!-- Agents: end with your disclosure line, after everything else. -->
