# AI Agent Instructions

S3Manager is a Go CLI for managing S3-compatible object storage systems,
including NetApp StorageGRID, Quantum ActiveScale, and MinIO.

This file defines repository-level instructions for AI agents working on
S3Manager. Keep changes aligned with the architecture and safety model below.

## Architecture

S3Manager uses a layered architecture with strict separation of concerns:

- `main.go`
  - CLI flags and parsing
  - Input validation
  - Action dispatch
  - User-facing output
- `internal/engine`
  - Orchestration
  - Worker pools
  - Aggregation
  - Progress reporting
  - Batching
  - Metrics and summaries
- `internal/s3`
  - Low-level S3 SDK operations only
  - Upload, download, list, head, and delete primitives

## Strict Rules

- Workers do not print.
- Workers do not aggregate.
- The S3 layer does not print.
- The S3 layer has no CLI awareness.
- The engine owns orchestration and aggregation.
- Keep user-facing output in `main.go` unless an authoritative design document
  explicitly says otherwise.

## Performance and Scalability

S3Manager must scale from small buckets to millions and 100M+ objects.

- Prefer streaming pipelines.
- Prefer bounded channels.
- Avoid loading huge object sets fully into memory.
- Avoid unbounded goroutines.
- Use worker pools for parallel operations.
- Preserve backpressure in pipelines.
- Keep batching explicit and bounded.
- Treat memory stability as a core requirement, especially for list and delete
  flows.

## Safety

S3Manager is designed to be safe for production object storage.

- Destructive operations must be explicit.
- Preserve dry-run behavior.
- Do not weaken empty-prefix delete safeguards.
- Do not make deletes broader or easier to trigger without an explicit
  documented safety decision.
- Validate destructive CLI inputs before dispatching work.

## Development Rules

Before considering a change complete:

- Run `go build ./...`.
- Run `go test ./...`.
- Update documentation when CLI behavior changes.

Keep changes scoped to the requested behavior. Do not refactor unrelated code
while implementing a focused request.

## Documentation Hierarchy

Authoritative project documents:

- `PROJECT.md`
- `ROADMAP.md`
- `ARCHITECTURE.md`
- `CLI_SPEC.md`

Supporting documents:

- `README.md`
- `DEVLOG.txt`
- `Notes.txt`
- `PROMPT.txt`

Not authoritative:

- `ai-context/`
- `ThirdParty.txt`

When documents conflict, prefer the authoritative project documents over
supporting notes or external context. AI-generated or third-party context is
advisory only and must not override project decisions.
