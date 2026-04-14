

# S3Manager

S3Manager is a Go-based CLI tool for managing S3-compatible object storage systems such as StorageGRID, ActiveScale, and MinIO.

The tool is designed as a learning project and evolving into a production-grade utility with clean architecture and high performance.

---

## Current Status

Version: v0.2.0  
Status: Stable CLI with parallel operations

### Implemented Features

- upload (single file)
- upload-folder (parallel, worker pool)
- list (prefix-based)
- download-prefix (parallel)
- delete (parallel, prefix-based)
- full-bucket delete (requires explicit flag)

### Implemented Features (continued)

- retry with exponential backoff (upload)
- context-aware retry (cancellable)
- retryable error filtering (smart retry)
- live progress reporting
- retry visibility in summary

### In Progress

- Improve progress output formatting (single-line updates)

### Planned

- Live progress reporting improvements
- Dry-run enhancements
- Smarter worker scaling

---

## Architecture

The project follows a layered architecture:

- `main.go` → CLI interface, flags, user interaction
- `internal/engine/` → orchestration, worker pools, aggregation
- `internal/s3/` → low-level S3 operations

Design principles:

- Separation of concerns
- No printing in worker threads
- Engine handles aggregation
- S3 layer is stateless and focused

For detailed internal design and data flow, see `ARCHITECTURE.md`.

---

## Environment Setup

Load environment variables:

```bash
source ~/.s3manager-env
```

Example variables:

```bash
export S3_ENDPOINT='https://s3-endpoint'
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY_ID='YOUR_KEY'
export AWS_SECRET_ACCESS_KEY='YOUR_SECRET'
```

---

## Usage

### Upload a single file

```bash
go run . \
  --action upload \
  --bucket temp-8155 \
  --file ./file.txt \
  --key "test/file.txt"
```

### Upload a folder (parallel)

```bash
go run . \
  --action upload-folder \
  --bucket temp-8155 \
  --folder ./data \
  --prefix "test/" \
  --workers 40
```

### List objects

```bash
go run . \
  --action list \
  --bucket temp-8155 \
  --prefix "test/"
```

### Download by prefix

```bash
go run . \
  --action download-prefix \
  --bucket temp-8155 \
  --prefix "test/" \
  --out-dir ./download \
  --workers 40
```

### Delete by prefix (safe)

```bash
go run . \
  --action delete \
  --bucket temp-8155 \
  --prefix "test/" \
  --workers 40 \
  --dry-run=true
```

### Delete entire bucket (DANGEROUS)

```bash
go run . \
  --action delete \
  --bucket temp-8155 \
  --prefix "" \
  --allow-empty-prefix-delete=true \
  --workers 40 \
  --dry-run=true
```

Remove `--dry-run=true` to perform actual deletion.

---

## Performance

Parallel upload benchmark (approximate):

| Workers | Duration | Throughput |
|--------|----------|------------|
| 1      | ~92s     | ~0.08 MiB/s |
| 8      | ~9.5s    | ~0.8 MiB/s  |
| 40     | ~2.1s    | ~3.4 MiB/s  |
| 50     | ~1.8s    | ~3.9 MiB/s  |
| 200    | ~2.7s    | ~2.6 MiB/s  |

Key insights:
- Workload is I/O bound
- Optimal worker range ~40–60
- More workers ≠ better performance
- Retry metrics provide visibility into reliability

---

## Safety

- Never delete with empty prefix unless explicitly intended
- Full-bucket delete requires `--allow-empty-prefix-delete=true`
- Always validate using `list` before delete
- Prefer dry-run before destructive operations

---

## Development Workflow

Typical loop:

```text
Code → Test → Benchmark → DEVLOG → Commit
```

### Build and validate

```bash
go mod tidy
go build ./...
```

---

## License

This project is currently for personal development and learning purposes.