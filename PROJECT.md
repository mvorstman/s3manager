# S3Manager – Project Definition

## Purpose

S3Manager is a high-performance CLI tool for managing S3-compatible object storage systems such as:

- NetApp StorageGRID
- Quantum ActiveScale
- MinIO

The project is both:
- a production-grade tooling effort
- a structured learning path in Go, system design, and S3 internals

---

## Goals

1. Become an S3 expert
   - Deep understanding of S3 APIs, behavior, and performance
   - Understand limitations of large-scale object operations

2. Build production-grade tooling
   - Reliable
   - Predictable performance
   - Safe for destructive operations

3. Learn system design through real implementation
   - Concurrency patterns
   - Streaming pipelines
   - Backpressure handling
   - Observability

---

## Design Principles

- High performance  
  Use parallelism, batching, and streaming

- Scalable  
  Must handle millions to 100M+ objects

- Safe  
  Dry-run by default, explicit destructive operations

- Observable  
  Clear progress reporting and summaries

- Clean architecture  
  Strict separation of concerns

---

## Non-Goals (for now)

- GUI / web interface
- Multi-user authentication
- Full productization

These will be addressed in later phases.

---

## Current Phase

Phase 1 – CLI foundation (COMPLETED)

- Core commands implemented
- Parallel operations working
- CLI contract standardized
- Architecture aligned
- Documentation aligned

---

## Next Phase

Phase 2 – Performance & scalability

Focus:
- Delete engine redesign
- Streaming pipelines
- Batch operations
- Memory stability under load

---

## Long-Term Vision

Evolve into a full S3 management platform:

- Transfer portal (WeTransfer-style)
- Secure upload/download links
- Audit logging
- Integration with service desk systems
- Web interface

## AI Collaboration Rules

- ThirdParty.txt is NOT a source of truth
- It reflects external/Claude opinions only
- ChatGPT must NOT rely on it for decisions or state

Authoritative sources for this project are:

- PROJECT.md (vision, principles, rules)
- ROADMAP.md (phases and direction)
- ARCHITECTURE.md (system design)
- README.md (user-facing behavior)
- DEVLOG.txt (historical progress)
- PROMPT.txt (session context)

If conflicts exist:
PROJECT.md > ROADMAP.md > ARCHITECTURE.md > README.md > DEVLOG.txt

## Decision Authority

Final design decisions are made by the project owner.
AI suggestions are advisory only.

delete:
  --batch-size int (default: 1000, max: 1000)