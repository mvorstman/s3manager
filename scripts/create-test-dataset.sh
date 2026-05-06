#!/usr/bin/env bash
set -euo pipefail

TARGET_ROOT="${S3MANAGER_TEST_ROOT:-$HOME/s3manager-test}"
MEDIUM_DIR="$TARGET_ROOT/datasets/medium"
LARGE_DIR="$TARGET_ROOT/datasets/large"

mkdir -p \
  "$TARGET_ROOT/smoke" \
  "$TARGET_ROOT/datasets/small" \
  "$MEDIUM_DIR" \
  "$LARGE_DIR" \
  "$TARGET_ROOT/benchmarks"

python3 - "$TARGET_ROOT" <<'PY'
from pathlib import Path
import hashlib
import random
import shutil
import sys

target_root = Path(sys.argv[1]).expanduser()
medium_dir = target_root / "datasets" / "medium"
large_dir = target_root / "datasets" / "large"

sizes = [1024, 2048, 4096, 8192]

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

def generate_dataset(dataset_dir: Path, file_count: int, seed: int) -> tuple[int, int]:
    rng = random.Random(seed)

    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)

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
        path = dataset_dir / relative_dir / f"object-{index:04d}-{size // 1024}kib.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(deterministic_bytes(index, size))

    total_size = sum(path.stat().st_size for path in dataset_dir.rglob("*") if path.is_file())
    return file_count, total_size

medium_count, medium_size = generate_dataset(medium_dir, 2000, 20260506)
large_count, large_size = generate_dataset(large_dir, 20000, 20260507)

print(f"Target path: {target_root}")
print("Medium dataset:")
print(f"  Path: {medium_dir}")
print(f"  File count: {medium_count}")
print(f"  Total size: {medium_size} bytes")
print("Large dataset:")
print(f"  Path: {large_dir}")
print(f"  File count: {large_count}")
print(f"  Total size: {large_size} bytes")
PY
