package sync

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"

	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/partition/part"
)

// excludedPaths these are excluded from any copy
var excludedPaths = map[string]bool{
	"lost+found":                true,
	".DS_Store":                 true,
	"System Volume Information": true,
}

const maxCopyAllSize = 64 * 1024 * 1024

type copyData struct {
	count int64
	err   error
}

// CopyFileSystem copies files from a source fs.FS to a destination filesystem.FileSystem, preserving structure and contents.
func CopyFileSystem(src fs.FS, dst filesystem.FileSystem) error {
	return copyDir(src, dst, ".")
}

func copyDir(src fs.FS, dst filesystem.FileSystem, dir string) error {
	entries, err := fs.ReadDir(src, dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		// filter out special directories/files
		if excludedPaths[name] {
			if entry.IsDir() {
				continue
			}
			continue
		}

		p := name
		if dir != "." {
			p = path.Join(dir, name)
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", p, err)
		}

		// symlinks, when they exist
		if info.Mode()&os.ModeSymlink != 0 {
			// Check if your destination interface supports symlinks
			// Most custom 'filesystem.FileSystem' interfaces might not.
			if err := handleSymlink(src, dst, p); err != nil {
				return fmt.Errorf("copy symlink %s: %w", p, err)
			}
			continue
		}

		if entry.IsDir() {
			if err := dst.Mkdir(p); err != nil {
				return fmt.Errorf("create dir %s: %w", p, err)
			}
			if err := copyDir(src, dst, p); err != nil {
				return fmt.Errorf("copy dir %s: %w", p, err)
			}
			continue
		}

		if !info.Mode().IsRegular() {
			// FAT32 / ISO / SquashFS should not have others
			continue
		}

		if err := copyOneFile(src, dst, p, info); err != nil {
			return fmt.Errorf("copy file %s: %w", p, err)
		}
	}

	return nil
}

func copyOneFile(src fs.FS, dst filesystem.FileSystem, p string, info fs.FileInfo) error {
	in, err := src.Open(p)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := dst.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if info.Size() <= maxCopyAllSize {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		n, err := out.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
	} else {
		buf := make([]byte, 32*1024)
		for {
			n, rerr := in.Read(buf)
			if n > 0 {
				written := 0
				for written < n {
					w, werr := out.Write(buf[written:n])
					if werr != nil {
						return werr
					}
					if w == 0 {
						return io.ErrShortWrite
					}
					written += w
				}
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				return rerr
			}
		}
	}

	// Restore timestamps *after* data is written (tar semantics)
	atime := getAccessTime(info)
	if atime.IsZero() {
		atime = info.ModTime() // fallback
	}
	if err := dst.Chtimes(
		p,
		info.ModTime(), // creation time fallback if not available
		atime,          // access time: optional / policy choice
		info.ModTime(),
	); err != nil {
		// Best-effort: copying content should still succeed even if timestamps cannot be set.
		return nil
	}
	return nil
}

// handleSymlink handles copying a symlink from src to dst. It reads the link target
func handleSymlink(src fs.FS, dst filesystem.FileSystem, p string) error {
	type readlinker interface {
		ReadLink(string) (string, error)
	}
	if rl, ok := src.(readlinker); ok {
		linkTarget, err := rl.ReadLink(p)
		if err != nil {
			return err
		}
		return dst.Symlink(linkTarget, p)
	}
	return fmt.Errorf("source filesystem does not support reading symlinks for %s", p)
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
