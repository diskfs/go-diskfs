// Package erofs provides read-only access to EROFS filesystem images.
//
// It supports an EROFS on-disk-format subset with block sizes from 512 through
// 65536 bytes, compact and extended inodes, uncompressed FLAT_PLAIN and
// FLAT_INLINE data, and LZMA-compressed compact and full index layouts.
// Compressed reads support logical clusters larger than the filesystem block,
// fragment tails, and shifted or interlaced incompressible extents.
//
// LZ4, Zstd, DEFLATE, chunked files, multi-device images, compressed inline
// tails, partial compressed references, 48-bit layouts, metaboxes, and long
// xattr prefixes are unsupported. Recognizing the fragments/dedupe feature bit
// does not imply support for every deduplication encoding. Physical compressed
// extents are limited to 1 MiB and decoded extents to 12 MiB. Each open File
// caches one decoded extent, including packed-inode fragments, until replaced
// or closed. ReadAt calls on the same File may run concurrently.
//
// Symlinks are exposed as link objects: Stat reports ModeSymlink and ReadFile
// or Open reads the stored target name. Paths are not resolved through symlinks,
// including intermediate components. Consumers extracting a binary must use its
// regular-file deployment path, rather than a symlink to it.
//
// Extended attributes are skipped when locating inode data but are not exposed
// through this API. Unsupported layouts and compression algorithms return
// descriptive errors rather than returning invalid data.
//
// Read borrows its backend. The caller must keep it open and unchanged while
// using the filesystem. FileSystem.Close is a no-op; it does not close the
// backend or open files. File.Close releases that file's decoded extent.
//
// For an EROFS image embedded at a known offset, create an offset backend with
// backend.Sub, then pass that backend and the image size to Read. The EROFS
// reader enforces this size; backend.Sub only translates offsets:
//
//	nested := backend.Sub(backend, offset, size)
//	filesystem, err := erofs.Read(nested, size, 0, 0)
package erofs
