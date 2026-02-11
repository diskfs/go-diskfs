package ext4_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/ext4"
	diskfsync "github.com/diskfs/go-diskfs/sync"
)

const imgFile = "testdata/dist/ext4.img"

var excludedPaths = map[string]bool{
	"lost+found":                true,
	".DS_Store":                 true,
	"System Volume Information": true,
}

func testCreateEmptyFile(t *testing.T, size int64) (string, *os.File) {
	t.Helper()
	dir := t.TempDir()
	outfile := filepath.Join(dir, "ext4.img")
	f, err := os.Create(outfile)
	if err != nil {
		t.Fatalf("Error creating empty image file: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("Error truncating image file: %v", err)
	}
	return outfile, f
}

func TestCopyFileSystemIntegration(t *testing.T) {
	srcInfo, err := os.Stat(imgFile)
	if err != nil {
		t.Fatalf("Error stating test image: %v", err)
	}
	srcSize := srcInfo.Size()
	dstSize := srcSize + srcSize/5

	srcFile, err := os.Open(imgFile)
	if err != nil {
		t.Fatalf("Error opening test image: %v", err)
	}
	defer srcFile.Close()

	srcBackend := file.New(srcFile, true)
	srcFS, err := ext4.Read(srcBackend, srcSize, 0, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem: %v", err)
	}

	dstPath, dstFile := testCreateEmptyFile(t, dstSize)
	defer dstFile.Close()

	dstBackend := file.New(dstFile, false)
	dstFS, err := ext4.Create(dstBackend, dstSize, 0, 512, &ext4.Params{})
	if err != nil {
		t.Fatalf("Error creating destination filesystem: %v", err)
	}
	if dstFS == nil {
		t.Fatalf("Expected non-nil filesystem after creation")
	}

	if err := diskfsync.CopyFileSystem(srcFS, dstFS); err != nil {
		t.Fatalf("Error copying filesystem: %v", err)
	}

	if err := dstFile.Sync(); err != nil {
		t.Fatalf("Error syncing destination file: %v", err)
	}

	srcVerifyFile, srcVerifyFS, err := readExt4FS(imgFile, srcSize)
	if err != nil {
		t.Fatalf("Error reopening source filesystem: %v", err)
	}
	defer srcVerifyFile.Close()
	dstVerifyFile, dstVerifyFS, err := readExt4FS(dstPath, dstSize)
	if err != nil {
		t.Fatalf("Error reopening destination filesystem: %v", err)
	}
	defer dstVerifyFile.Close()

	cmd := exec.Command("e2fsck", "-f", "-n", "-vv", dstPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed: %v,\nstdout:\n%s,\n\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	if err := compareFileSystems(srcVerifyFS, dstVerifyFS); err != nil {
		t.Fatalf("Filesystem copy mismatch: %v", err)
	}
}

func readExt4FS(p string, size int64) (*os.File, fs.FS, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}
	b := file.New(f, true)
	ext4FS, err := ext4.Read(b, size, 0, 512)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, ext4FS, nil
}

func compareFileSystems(src, dst fs.FS) error {
	return compareDir(src, dst, ".")
}

func compareDir(src, dst fs.FS, dir string) error {
	srcEntries, err := fs.ReadDir(src, dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}
	dstEntries, err := fs.ReadDir(dst, dir)
	if err != nil {
		return fmt.Errorf("read dir %s (dst): %w", dir, err)
	}

	srcMap := make(map[string]fs.DirEntry, len(srcEntries))
	for _, entry := range srcEntries {
		if excludedPaths[entry.Name()] {
			continue
		}
		srcMap[entry.Name()] = entry
	}
	dstMap := make(map[string]fs.DirEntry, len(dstEntries))
	for _, entry := range dstEntries {
		if excludedPaths[entry.Name()] {
			continue
		}
		dstMap[entry.Name()] = entry
	}

	if len(srcMap) != len(dstMap) {
		return fmt.Errorf("entry count mismatch in %s: src=%d dst=%d", dir, len(srcMap), len(dstMap))
	}

	names := make([]string, 0, len(srcMap))
	for name := range srcMap {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		srcEntry, ok := srcMap[name]
		if !ok {
			return fmt.Errorf("missing source entry %s", name)
		}
		dstEntry, ok := dstMap[name]
		if !ok {
			return fmt.Errorf("missing destination entry %s in %s", name, dir)
		}

		srcInfo, err := srcEntry.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
		dstInfo, err := dstEntry.Info()
		if err != nil {
			return fmt.Errorf("stat %s (dst): %w", name, err)
		}

		srcIsDir := srcEntry.IsDir()
		dstIsDir := dstEntry.IsDir()
		if srcIsDir != dstIsDir {
			return fmt.Errorf("type mismatch for %s: dir=%v dst=%v", name, srcIsDir, dstIsDir)
		}

		srcIsSymlink := srcInfo.Mode()&fs.ModeSymlink != 0
		dstIsSymlink := dstInfo.Mode()&fs.ModeSymlink != 0
		if srcIsSymlink != dstIsSymlink {
			return fmt.Errorf("symlink mismatch for %s: src=%v dst=%v", name, srcIsSymlink, dstIsSymlink)
		}

		fullPath := name
		if dir != "." {
			fullPath = path.Join(dir, name)
		}

		switch {
		case srcIsSymlink:
			srcTarget, err := readlink(src, fullPath)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", fullPath, err)
			}
			dstTarget, err := readlink(dst, fullPath)
			if err != nil {
				return fmt.Errorf("readlink %s (dst): %w", fullPath, err)
			}
			if srcTarget != dstTarget {
				return fmt.Errorf("symlink target mismatch for %s: %q vs %q", fullPath, srcTarget, dstTarget)
			}
		case srcIsDir:
			if err := compareDir(src, dst, fullPath); err != nil {
				return err
			}
		default:
			if srcInfo.Size() != dstInfo.Size() {
				return fmt.Errorf("size mismatch for %s: src=%d dst=%d", fullPath, srcInfo.Size(), dstInfo.Size())
			}
			match, err := compareFileContents(src, dst, fullPath)
			if err != nil {
				return err
			}
			if !match {
				return fmt.Errorf("content mismatch for %s", fullPath)
			}
		}
	}

	return nil
}

func compareFileContents(src, dst fs.FS, p string) (bool, error) {
	srcFile, err := src.Open(p)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", p, err)
	}
	defer srcFile.Close()
	dstFile, err := dst.Open(p)
	if err != nil {
		return false, fmt.Errorf("open %s (dst): %w", p, err)
	}
	defer dstFile.Close()

	srcHash := sha256.New()
	dstHash := sha256.New()
	buf := make([]byte, 32*1024)
	if _, err := io.CopyBuffer(srcHash, srcFile, buf); err != nil {
		return false, fmt.Errorf("hash %s: %w", p, err)
	}
	if _, err := io.CopyBuffer(dstHash, dstFile, buf); err != nil {
		return false, fmt.Errorf("hash %s (dst): %w", p, err)
	}

	return bytes.Equal(srcHash.Sum(nil), dstHash.Sum(nil)), nil
}

func readlink(fsys fs.FS, p string) (string, error) {
	type readlinker interface {
		ReadLink(string) (string, error)
	}
	if rl, ok := fsys.(readlinker); ok {
		return rl.ReadLink(p)
	}
	return "", fmt.Errorf("filesystem does not support readlink: %T", fsys)
}
