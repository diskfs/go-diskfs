// Package erofs provides read-only access to EROFS filesystem images.
//
// It supports an EROFS on-disk-format subset with block sizes from 512 through
// 65536 bytes, compact and extended inodes, uncompressed FLAT_PLAIN and
// FLAT_INLINE data, and LZMA-compressed compact and full index layouts.
// Compressed reads support logical clusters larger than the filesystem block,
// fragment tails, and shifted or interlaced incompressible extents.
//
// Extended attributes are skipped when locating inode data but are not exposed
// through this API. Unsupported layouts and compression algorithms return
// descriptive errors rather than returning invalid data.
//
// For an EROFS image embedded at a known offset, first create a restricted
// backend with backend.Sub, then pass that backend and its size to Read:
//
//	nested := backend.Sub(backend, offset, size)
//	filesystem, err := erofs.Read(nested, size, 0, 0)
package erofs
