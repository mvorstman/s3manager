#!/usr/bin/env bash
set -euo pipefail

TARGET_ROOT="${S3MANAGER_TEST_ROOT:-$HOME/s3manager-test}"
MEDIUM_DIR="$TARGET_ROOT/datasets/medium"

mkdir -p \
  "$TARGET_ROOT/smoke" \
  "$TARGET_ROOT/datasets/small" \
  "$MEDIUM_DIR" \
  "$TARGET_ROOT/datasets/large" \
  "$TARGET_ROOT/benchmarks"

python3 - "$TARGET_ROOT" <<'PY'
from pathlib import Path
import hashlib
import random
import shutil
import sys

target_root = Path(sys.argv[1]).expanduser()
medium_dir = target_root / "datasets" / "medium"

file_count = 2000
sizes = [1024, 2048, 4096, 8192]
rng = random.Random(20260506)

if medium_dir.exists():
    shutil.rmtree(medium_dir)
medium_dir.mkdir(parents=True, exist_ok=True)

def deterministic_bytes(index: int, size: int) -> bytes:
    chunks = []
    produced = 0
    block = 0
    while produced < size:
        digest = hashlib.sha256(f"s3manager:{index}:{block}".encode()).digest()
        chunks.append(digest)
        produced += len(digest)
        block += 1
    return b"".join(chunks)[:size]

for index in range(file_count):
    size = sizes[index % len(sizes)]
    group = rng.randrange(20)
    shard = rng.randrange(25)
    depth = rng.randrange(4)
    prefix_parts = [
        f"group-{group:02d}",
        f"shard-{shard:02d}",
        f"depth-{depth}",
    ]
    if depth >= 1:
        prefix_parts.append(f"batch-{rng.randrange(10):02d}")
    if depth >= 2:
        prefix_parts.append(f"part-{rng.randrange(16):02x}")
    if depth >= 3:
        prefix_parts.append(f"leaf-{rng.randrange(32):02d}")

    relative_dir = Path(*prefix_parts)
    path = medium_dir / relative_dir / f"object-{index:04d}-{size // 1024}kib.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(deterministic_bytes(index, size))

total_size = sum(path.stat().st_size for path in medium_dir.rglob("*") if path.is_file())
print(f"Target path: {target_root}")
print(f"File count: {file_count}")
print(f"Total size: {total_size} bytes")
PY
