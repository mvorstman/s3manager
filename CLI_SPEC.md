# S3Manager CLI Specification

## Purpose

This document defines the official CLI contract for S3Manager.
All command behavior, flags, and semantics must match this specification.

Hierarchy of truth:
1. CLI_SPEC.md (this file)
2. main.go (implementation)
3. README.md / Notes.txt (derived documentation)

---

## Global Flags

| Flag | Required | Description |
|------|----------|-------------|
| --action | yes | Operation to perform |
| --bucket | yes | Target S3 bucket |
| --endpoint | yes* | S3 endpoint (or via env) |
| --region | yes* | AWS region (default: us-east-1) |
| --access-key | yes* | Access key (or via env) |
| --secret-key | yes* | Secret key (or via env) |

(*) Can be provided via environment variables:
- S3_ENDPOINT
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY

---

## Common Flags

| Flag | Description |
|------|------------|
| --prefix | Object key prefix filter |
| --workers | Number of parallel workers |
| --verbose | Enable detailed output |
| --max-keys | Pagination size |
| --dry-run | Simulate operation |

---

## Actions

### list
List objects in a bucket

Required:
- --bucket

Optional:
- --prefix
- --max-keys

---

### upload
Upload a single file

Required:
- --bucket
- --file
- --key

---

### upload-folder
Upload a folder recursively

Required:
- --bucket
- --folder

Optional:
- --prefix
- --workers
- --verbose

---

### download
Download a single object

Required:
- --bucket
- --key
- --out

---

### download-prefix
Download objects under a prefix

Required:
- --bucket
- --prefix
- --out-dir

Optional:
- --workers
- --max-keys
- --verbose

---

### head
Get object metadata

Required:
- --bucket
- --key

---

### delete
Delete objects under a prefix

Required:
- --bucket

Optional:
- --prefix
- --workers
- --dry-run

Special rule:
- Empty prefix requires:
  --allow-empty-prefix-delete=true

---

## Design Rules

- S3 layer does not print output
- Engine layer handles aggregation and presentation
- CLI validates input
- Destructive operations must support dry-run

---

## Notes

- Prefix behavior is consistent across all commands
- Parallelism controlled via --workers
- Safe by default design