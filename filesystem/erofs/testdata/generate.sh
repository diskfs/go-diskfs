#!/bin/sh
set -eu

# Keep source permissions independent of the caller's umask.
umask 022
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
output_dir=${1:-$script_dir}
mkdir -p "$output_dir"
output_dir=$(CDPATH= cd -- "$output_dir" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' 0
trap 'exit 1' HUP INT TERM

python3 - "$work_dir" <<'PY'
import pathlib
import sys

work = pathlib.Path(sys.argv[1])
source = work / "source"
(source / "empty").mkdir(parents=True)
(source / "sub").mkdir()
(source / "empty.txt").touch()
(source / "hello.txt").write_bytes(b"hello world\n")
(source / "link").symlink_to("hello.txt")

payload = bytearray()
seed = 0x45524F46
for _ in range(96 * 1024 // 4):
    seed = (seed * 1664525 + 1013904223) & 0xFFFFFFFF
    payload += seed.to_bytes(4, "little")
(source / "sub" / "data.bin").write_bytes(payload)

full = work / "full"
full.mkdir()
payload = b"EROFS deterministic fixture payload\n" * 2622
(full / "data.bin").write_bytes(payload.ljust(96 * 1024, b"\0"))
PY

set -- -b4096 -x-1 --all-root -T1700000000 \
    -U00000000-0000-0000-0000-000000000000
mkfs.erofs "$@" "$output_dir/uncompressed.erofs" "$work_dir/source"
mkfs.erofs "$@" -zlzma -C65536 -Efragments \
    "$output_dir/compressed.erofs" "$work_dir/source"
mkfs.erofs "$@" -zlzma -Elegacy-compress \
    "$output_dir/full.erofs" "$work_dir/full"
