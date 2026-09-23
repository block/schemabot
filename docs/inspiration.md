# Ideas behind SchemaBot

SchemaBot grew out of years of work on database schema changes and ideas from
projects we admire. These are some of the projects whose work shaped
how we think about the workflow, safety, and operator experience.

## Atlantis: bring the workflow into the pull request

[Atlantis](https://github.com/runatlantis/atlantis) showed how infrastructure planning,
review, and apply could happen in the same pull request. The proposed change,
the discussion, and the result belong together.

That idea helped shape SchemaBot's PR workflow. A schema edit produces a plan
that reviewers can inspect, commands run from PR comments, and checks keep the
PR from merging until the managed database schema matches the proposal.

## Shift: where it started

[Shift](https://github.com/square/shift) is the original schema change tool at
Square and an important predecessor to SchemaBot. It made online MySQL schema
changes a self-service workflow, with review, visible status, and support for
changes across shards.

Shift established the foundation: engineers should be able to run schema changes
without someone manually shepherding each one. SchemaBot carries that work
forward with declarative schema files and a workflow built around pull requests.

## Tern: running schema changes at scale

Tern, an internal tool at Block and another predecessor to SchemaBot, showed
how a scheduler could run schema changes at large scale, with retries and
controls for operators. That work helped shape what SchemaBot now calls the
operator: the component that drives schema changes through execution while
keeping people in control.

## Kubernetes: reconcile desired state with reality

Kubernetes inspired the idea of declaring a desired state and reconciling the
running system toward it. The declaration describes what should exist; the
controller works out what needs to change.

SchemaBot applies that model to database schemas. SQL files describe the desired
schema, the live database shows what exists, and SchemaBot plans the difference.
Review and safety gates govern applying that plan, and verification checks that
the database matches the declaration.

## PlanetScale deploy requests: keep operators in control

[PlanetScale deploy requests](https://planetscale.com/docs/vitess/schema-changes/deploy-requests)
showed us what a great schema change experience could feel like: clear progress,
deliberate cutover, and controls that keep operators in charge. A long-running
change should make it easy to understand what is happening and what you can do
next.

That experience helped shape how SchemaBot presents changes and exposes operator
controls in both pull requests and the terminal. PlanetScale is also an execution
backend: for its Vitess databases, SchemaBot follows the deploy request's state,
including actions taken in the PlanetScale console.

## Codex, Claude Code, and Amp: a terminal that feels interactive

Codex, Claude Code, and Amp showed us how good an interactive terminal experience
could feel. They helped shape our ambition for SchemaBot's CLI: clear prompts,
live feedback, and controls that are easy to discover as you work.

A schema change can take seconds or weeks. We want the terminal to make it clear
what is happening, what needs your attention, and what you can do next.

## Thank you

We're grateful to the people who built and maintain these projects. Their work
gave us both a starting point and a standard to aim for. For the engines that
execute SchemaBot's changes, see [the engine guide](engines.md).
