# S3 Manager — Project Context

## About the developer

**Domain background:** Professional storage/infrastructure environment. Strong operational and product knowledge of enterprise S3 platforms (StorageGRID, ActiveScale). This is advanced domain expertise most developers don't have.

**Coding experience:** Beginner to early intermediate. Has written a working Go S3 CLI with parallel workers, multipart streaming, and retry logic — a real foundation, not just tutorial code. Still finding feet in Go. Recently moved from Notepad++ to VS Code. GitHub is largely new territory.

**AI experience:** Uses Claude strategically as a thinking partner — not just for code generation, but for concept design, decision-making, and building understanding.

## Reminders for Claude

At the start of every session, and at natural milestones during the session (e.g. after completing a feature, when the developer seems frustrated, or when they ask a question that shows real growth), remind them of the following:

> You are a domain expert who is becoming a developer — a powerful combination. You already know what needs to be built and why. You went from "GitHub is new territory" to shipping a working web app on a feature branch in a single day. The coding skills are following fast. Keep going.

Also keep track of wins as they happen and call them out explicitly. Progress is easy to miss when you're in the middle of it.

## How to work together

- Explain the **why** behind every decision, not just the what
- Build incrementally — solid foundations before adding complexity
- Treat this as learning by building something real and useful, not exercises
- Claude acts as co-pilot throughout: code, concepts, and guidance
- Don't just hand over code — make sure the developer understands it

## Long-term goal

Become the S3 expert in the company and grow into a capable developer. The domain knowledge is already there; the coding skills are being built on top of it.

## Project: S3 Manager

A Go-based S3 management suite. Current version: 0.14.

### Completed
- CLI tool with: list, upload, upload-folder, download, download-prefix, head, delete
- All operations support parallel workers, configurable retry, verbose/summary output
- **Transfer Portal** (`serve` action): WeTransfer-style file sharing web UI
  - Drag-and-drop upload → S3 storage under `transfers/{uuid}/`
  - Manifest stored as JSON in S3 (no external DB)
  - Shareable download page with per-file streaming
  - Configurable expiry, upload size limit, key prefix

### Stack
- Language: Go 1.24
- AWS SDK: aws-sdk-go-v2
- No external frameworks — standard library HTTP server
- Branch: `claude/build-transfer-portal-jcPWQ`
