# S3Manager Architecture

This document describes how the internal components of S3Manager interact, how data flows through the system, and the responsibilities of each layer.

---

## High-Level Overview

S3Manager follows a layered architecture:

- `main.go` → CLI interface and orchestration entry point
- `internal/engine/` → orchestration, worker pools, aggregation
- `internal/s3/` → low-level S3 operations

---

## Layer Responsibilities

### main.go
- Parses CLI flags
- Validates input
- Initializes S3 client
- Dispatches actions (upload, delete, etc.)
- Calls engine layer
- Prints final results

---

### internal/engine
- Builds jobs (e.g. file discovery)
- Manages worker pools
- Handles concurrency
- Aggregates results
- Handles progress reporting
- Computes metrics (throughput, retries, totals)

---

### internal/s3
- Performs actual S3 API calls
- Upload, download, list, delete primitives
- No orchestration
- No aggregation
- No CLI awareness

---

## Data Flow (upload-folder)

```
main.go
  → UploadFolder()
      → BuildUploadJobs()
      → jobsCh (UploadJob)
      → uploadWorker()
      → resultsCh (UploadFileResult)
      → aggregation loop
      → UploadFolderResult
      → PrintUploadFolderResult()
```