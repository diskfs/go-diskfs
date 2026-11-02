# EROFS Test Fixtures

The fixtures were generated with `erofs-utils` 1.9.4 and can be read by tests
without external tools. To regenerate them, install Python 3 and
`mkfs.erofs` from `erofs-utils` 1.9.4 with LZMA support, then run:

```sh
./filesystem/erofs/testdata/generate.sh
```

The script works from any directory, creates and cleans up its source trees,
and fixes permissions, ownership, timestamps, UUIDs, and block sizes. An optional
output directory lets you compare regenerated images with the committed ones:

```sh
./filesystem/erofs/testdata/generate.sh /tmp/erofs-fixtures
```

The source tree contains an empty directory, an empty file, `hello.txt`
containing `hello world\n`, a `link` symlink to `hello.txt`, and
`sub/data.bin`. The 96 KiB data payload is the little-endian output of the
pseudo-random sequence `seed = seed * 1664525 + 1013904223`, starting with
`0x45524f46`.

`full.erofs` contains a single 96 KiB file filled with
`EROFS deterministic fixture payload\n` and truncated to length. It uses the
full compressed index layout without storing the whole file in the packed
inode.

SHA-256:

* `uncompressed.erofs`: `0528acf617874fc58673a6dc9cbba677b750c7e285fb40d353628bc7e319d55a`
* `compressed.erofs`: `22faedd65c01aa91baa76aca52c523dd746d5585ffb5b0c6a02b93d0b913f279`
* `full.erofs`: `352775b0e0b6561c6b51a8a9c5ca5924df7e4447325b183d8f1f670c77371e44`

## Streaming benchmark

With `mkfs.erofs` available, run:

```sh
go test ./filesystem/erofs -run '^$' -bench BenchmarkLargeExtentStreaming -benchmem
```

The benchmark generates a deterministic 15 MiB payload with 1 MiB physical
compressed clusters, multi-megabyte decoded extents, and a fragment tail. Image
generation is outside the timed region. Each extraction opens a fresh file and
checks the output against the source; the 32 KiB buffer and whole-file buffer
cases both include initial decoding.
