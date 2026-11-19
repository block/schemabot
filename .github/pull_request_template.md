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

### Before and after

<!--
Required when the change has a shape: a flow, a state change, a lifecycle,
concurrency, or ownership. Delete this section only for a change with no
shape (a comment, a test, a copy edit, a dependency bump). Trace one
concrete example through both halves, named in the sentence above the
diagram. Draw both halves in the same form and node order, and mark with
✗ where the old behavior goes wrong. Keep it in the fenced block, then
re-read it as a grid: borders, corners, and connectors line up. For a
change that only adds behavior, the After half alone is enough.
-->

<One sentence naming the example's inputs.>

```
Before                          After

┌────────────────────┐          ┌────────────────────┐
│ <state or step>    │          │ <state or step>    │
└─────────┬──────────┘          └─────────┬──────────┘
          │ <event>                       │ <event>
          ▼                               ▼
┌────────────────────┐          ┌────────────────────┐
│ <outcome>          │ ✗ <why>  │ <outcome>          │
└────────────────────┘          └────────────────────┘
```

<!--
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
