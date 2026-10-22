# EROFS Test Fixtures

The fixtures were generated with `erofs-utils` 1.9.4 and can be read by tests
without external tools.

The source tree contains an empty directory, an empty file, `hello.txt`
containing `hello world\n`, a `link` symlink to `hello.txt`, and
`sub/data.bin`. The 96 KiB data payload is the little-endian output of the
pseudo-random sequence `seed = seed * 1664525 + 1013904223`, starting with
`0x45524f46`.

`full.erofs` contains a single 96 KiB file filled with
`EROFS deterministic fixture payload\n` and truncated to length. It uses the
full compressed index layout without storing the whole file in the packed
inode.

Create `sub/data.bin` with:

```sh
python3 - <<'EOF'
import pathlib
payload = bytearray()
seed = 0x45524F46
for _ in range(96 * 1024 // 4):
    seed = (seed * 1664525 + 1013904223) & 0xFFFFFFFF
    payload += seed.to_bytes(4, "little")
pathlib.Path("sub/data.bin").write_bytes(payload)
EOF
```

Generate the fixtures with:

```sh
mkfs.erofs -b4096 -x-1 --all-root -T1700000000 \
  -U00000000-0000-0000-0000-000000000000 \
  uncompressed.erofs SOURCE
mkfs.erofs -b4096 -zlzma -x-1 --all-root -C65536 -Efragments -T1700000000 \
  -U00000000-0000-0000-0000-000000000000 compressed.erofs SOURCE
mkfs.erofs -b4096 -zlzma -x-1 --all-root -Elegacy-compress -T1700000000 \
  -U00000000-0000-0000-0000-000000000000 full.erofs SOURCE
```

SHA-256:

* `uncompressed.erofs`: `0528acf617874fc58673a6dc9cbba677b750c7e285fb40d353628bc7e319d55a`
* `compressed.erofs`: `22faedd65c01aa91baa76aca52c523dd746d5585ffb5b0c6a02b93d0b913f279`
* `full.erofs`: `352775b0e0b6561c6b51a8a9c5ca5924df7e4447325b183d8f1f670c77371e44`
