# User stories

This directory is the durable backlog for product work. It complements
implementation notes and runbooks: a story describes the operator or user
outcome, the safety boundary, and how we will know the work is complete.

## Layout and lifecycle

- `backlog/` contains recommended work that has not begun.
- `active/` contains the one story currently being implemented.
- `completed/` contains delivered stories and their final validation notes.

Git does not retain empty directories, so only directories containing a story
will appear in a checkout. Move a story between those directories instead of
rewriting its history. If a story is superseded, retain it and add a short note
pointing to its replacement.

## Naming and required sections

Name each story `NNNN-short-kebab-case-title.md`, with numbers assigned in
creation order. Every story must include:

1. Status, priority, owner, and dependencies.
2. The user-story statement and the problem it solves.
3. Scope and explicit non-goals.
4. Acceptance criteria that can be observed or tested.
5. A validation plan and any operational rollout constraints.

Use one of these status values: `recommended`, `approved`, `in progress`,
`blocked`, `complete`, or `superseded`. A story is not complete merely because
its code exists; its required validation must also be recorded.

The current paper-pilot story remains in [note.md](../../note.md) while its
real forward observation period is pending. New work should be added here.
