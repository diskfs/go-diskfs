package sync

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"

	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/part"
)

var excludedPaths = map[string]bool{
	"lost+found":                true,
	".DS_Store":                 true,
	"System Volume Information": true,
}

type copyData struct {
	count int64
	err   error
}

// CopyFileSystem copies files from a source fs.FS to a destination filesystem.FileSystem, preserving structure and contents.
func CopyFileSystem(src fs.FS, dst filesystem.FileSystem) error {
	return fs.WalkDir(src, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// filter out special directories/files
		if excludedPaths[d.Name()] {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if path == "." || path == "/" || path == "\\" {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		// symlinks, when they exist
		if info.Mode()&os.ModeSymlink != 0 {
			// Check if your destination interface supports symlinks
			// Most custom 'filesystem.FileSystem' interfaces might not.
			return handleSymlink(src, dst, path)
		}

		if d.IsDir() {
			if path == "." {
				return nil
			}
			return dst.Mkdir(path)
		}

		if !info.Mode().IsRegular() {
			// FAT32 / ISO / SquashFS should not have others
			return nil
		}

		return copyOneFile(src, dst, path, info)
	})
}

func copyOneFile(src fs.FS, dst filesystem.FileSystem, path string, info fs.FileInfo) error {
	in, err := src.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := dst.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	// Restore timestamps *after* data is written (tar semantics)
	atime := getAccessTime(info)
	if atime.IsZero() {
		atime = info.ModTime() // fallback
	}
	return dst.Chtimes(
		path,
		info.ModTime(), // creation time fallback if not available
		atime,          // access time: optional / policy choice
		info.ModTime(),
	)
}

// handleSymlink handles copying a symlink from src to dst. It reads the link target
//
//nolint:revive,unparam // keeping args for clarity of intent.
func handleSymlink(src fs.FS, dst filesystem.FileSystem, path string) error {
	// Note: src must support ReadLink. If src is an os.DirFS,
	// you might need a type assertion or use os.Readlink directly.
	linkTarget, err := os.Readlink(path)
	if err != nil {
		return nil // Or handle error
	}

	// This assumes your 'dst' interface has a Symlink method
	return dst.Symlink(linkTarget, path)
}

// CopyPartitionRaw copies raw data from one partition to another and verifies the copy.
func CopyPartitionRaw(d *disk.Disk, from, to int) error {
	// copy raw data using a pipe so reads feed writes concurrently
	pr, pw := io.Pipe()
	ch := make(chan copyData, 1)

	go func() {
		defer func() { _ = pw.Close() }()
		read, err := d.ReadPartitionContents(from, pw)
		ch <- copyData{count: read, err: err}
	}()

	written, err := d.WritePartitionContents(to, pr)
	var ierr *part.IncompletePartitionWriteError
	if err != nil && !errors.As(err, &ierr) {
		return fmt.Errorf("failed to write raw data for partition %d: %v", to, err)
	}

	readData := <-ch
	if readData.err != nil {
		return fmt.Errorf("failed to read raw data for partition %d: %v", from, readData.err)
	}
	if readData.count != written {
		return fmt.Errorf("mismatched read/write sizes for partition %d: read %d bytes, wrote %d bytes", from, readData.count, written)
	}
	log.Printf("partition %d -> %d: contents copied byte for byte, %d bytes copied", from, to, written)
	if err := verifyBlockCopy(d, from, to, readData.count); err != nil {
		return fmt.Errorf("verification failed for partition %d: %v", from, err)
	}
	log.Printf("partition %d -> %d: block copy verified", from, to)
	return nil
}
