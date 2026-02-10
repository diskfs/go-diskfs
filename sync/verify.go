package sync

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"path"

	"github.com/diskfs/go-diskfs/disk"
)

func verifyBlockCopy(d *disk.Disk, from, to int, expectedSize int64) error {
	// open both partitions for reading
	origPart, err := d.GetPartition(from)
	if err != nil {
		return err
	}
	targetPart, err := d.GetPartition(to)
	if err != nil {
		return err
	}
	// create a sha256sum of both partitions and compare
	// but limit it to expectedSize
	origHasher := sha256.New()
	size, err := origPart.ReadContents(d.Backend, origHasher)
	if err != nil {
		return err
	}
	if size != expectedSize {
		return fmt.Errorf("original partition size %d is different than expected size %d", size, expectedSize)
	}
	origResult := origHasher.Sum(nil)

	targetHasher := sha256.New()
	size, err = targetPart.ReadContents(d.Backend, NewLimitWriter(targetHasher, expectedSize))
	if err != nil {
		return err
	}
	if size != expectedSize {
		return fmt.Errorf("target partition size %d is different than expected size %d", size, expectedSize)
	}
	targetResult := targetHasher.Sum(nil)

	if !bytes.Equal(origResult, targetResult) {
		return fmt.Errorf("data mismatch between original and target partitions")
	}
	return nil
}

// CompareFS compares two fs.FS instances for identical structure and contents.
func CompareFS(origFS, targetFS fs.FS) error {
	seen := make(map[string]struct{})

	// Walk original FS
	err := fs.WalkDir(origFS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		seen[p] = struct{}{}

		// Check existence in target FS
		td, err := fs.Stat(targetFS, p)
		if err != nil {
			return fmt.Errorf("path %q missing in target FS: %w", p, err)
		}

		// Compare type
		if d.IsDir() != td.IsDir() {
			return fmt.Errorf("type mismatch at %q", p)
		}

		if d.IsDir() {
			return nil
		}

		// Compare file size
		od, err := d.Info()
		if err != nil {
			return err
		}
		if od.Size() != td.Size() {
			return fmt.Errorf("size mismatch at %q", p)
		}

		// Compare file contents
		return compareFileContents(origFS, targetFS, p)
	})
	if err != nil {
		return err
	}

	// Ensure target FS has no extra files
	//
	//nolint:revive // keeping args for clarity of intent.
	return fs.WalkDir(targetFS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if _, ok := seen[p]; !ok {
			return fmt.Errorf("extra path %q in target FS", p)
		}
		return nil
	})
}

func compareFileContents(a, b fs.FS, name string) error {
	af, err := a.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = af.Close() }()

	bf, err := b.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = bf.Close() }()

	const bufSize = 32 * 1024
	bufA := make([]byte, bufSize)
	bufB := make([]byte, bufSize)

	for {
		na, ea := af.Read(bufA)
		nb, eb := bf.Read(bufB)

		if na != nb || !bytes.Equal(bufA[:na], bufB[:nb]) {
			return fmt.Errorf("content mismatch at %q", path.Clean(name))
		}

		if ea == io.EOF && eb == io.EOF {
			return nil
		}
		if ea != nil && ea != io.EOF {
			return ea
		}
		if eb != nil && eb != io.EOF {
			return eb
		}
	}
}

// LimitedWriter writes to W but limits the total amount of data written to N bytes.
// Each call to Write updates N to reflect the new amount remaining.
type LimitedWriter struct {
	W io.Writer // underlying writer
	N int64     // max bytes remaining
}

func (l *LimitedWriter) Write(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF // Or another appropriate error
	}
	if int64(len(p)) > l.N {
		p = p[:l.N]
	}
	n, err = l.W.Write(p)
	l.N -= int64(n)
	return n, err
}

// NewLimitWriter creates a new LimitedWriter.
func NewLimitWriter(w io.Writer, n int64) io.Writer {
	return &LimitedWriter{W: w, N: n}
}
