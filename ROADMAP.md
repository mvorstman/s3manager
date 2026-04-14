# S3Manager Roadmap

## Phase 1 – CLI Foundation ✅

Completed:

- Basic CLI structure
- S3 client integration (AWS SDK v2)
- Commands:
  - list
  - upload
  - upload-folder (parallel)
  - download-prefix (parallel)
  - delete (parallel)
- Retry logic and progress reporting
- CLI contract standardization (`--prefix`)
- Architecture cleanup
- Documentation alignment

---

## Phase 2 – Performance & Scalability 🚧 (CURRENT)

### Primary focus: Delete Engine Redesign

Goals:

- Support millions to 100M+ objects
- Avoid full in-memory buffering
- Maximize throughput using batching
- Keep resource usage predictable

### Planned work:

1. Redesign delete pipeline
   - Streaming model:
     - Lister → batch builder → worker pool → S3 delete

2. Implement batch deletes
   - Use `DeleteObjects` (max 1000 keys per request)

3. Introduce backpressure
   - Bounded channels
   - Prevent memory explosion

4. Improve retry handling
   - Retry transient failures
   - Surface permanent errors clearly

5. Add performance metrics
   - Objects/sec
   - Batch throughput
   - Retry statistics

---

## Phase 3 – Observability & UX

Goals:

- Improve usability of CLI
- Provide better visibility into operations

Planned work:

- Progress bar (single-line updates)
- Structured output (`--json`)
- Logging modes:
  - verbose
  - quiet
- Better summaries

---

## Phase 4 – Advanced Features

- Metadata operations (HEAD, tagging, etc.)
- Partial operations (range downloads)
- Smarter defaults (auto worker scaling)

---

## Phase 5 – Transfer Portal

Goal:

Build a WeTransfer-style system on top of S3Manager.

Features:

- Upload → generate expiring download link
- Request uploads from external users
- Token-based access (no direct S3 exposure)
- Email notifications
- Audit logging

---

## Phase 6 – Web Application

- Go backend (API layer)
- Web UI (likely React)
- Multi-user support
- Integration with external systems (e.g. service desk)

---

## Guiding Principle

Each phase must:

- Maintain clean architecture
- Be production-safe
- Be benchmarked and validated