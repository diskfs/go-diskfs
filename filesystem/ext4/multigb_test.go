package ext4

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// TestWriteMultiGBWithCopyN reproduces the 341-child internal-node overflow:
// io.CopyN's default 32 KiB writes create enough extents to split a full
// non-root internal node while writing a 2 GiB file to a 10 GiB image.
func TestWriteMultiGBWithCopyN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 2 GiB write regression in short mode")
	}
	const (
		diskSize = int64(10 << 30)
		fileSize = int64(2 << 30)
	)
	imagePath, f := testCreateEmptyFile(t, diskSize)
	t.Cleanup(func() { _ = f.Close() })
	storage := file.New(f, false)
	fs, err := Create(storage, diskSize, 0, 0, &Params{VolumeName: "TEST_DISK"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	fd, err := fs.OpenFile("BIGFILE.bin", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	t.Cleanup(func() { _ = fd.Close() })

	// Hash as we generate the deterministic input, without buffering 2 GiB
	// or changing the small write size that triggers the original panic.
	wantHash := sha256.New()
	source := io.TeeReader(rand.New(rand.NewSource(42)), wantHash) //nolint:gosec // Deterministic test payload; no cryptographic randomness needed.
	n, err := io.CopyN(fd, source, fileSize)
	if err != nil {
		t.Fatalf("CopyN failed after %d bytes: %v", n, err)
	}
	if n != fileSize {
		t.Fatalf("wrote %d bytes, want %d", n, fileSize)
	}
	if err := fd.Close(); err != nil {
		t.Fatalf("Close file: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close filesystem: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Reload all metadata from the image, including the extent tree.
	reopened, err := Read(storage, diskSize, 0, 0)
	if err != nil {
		t.Fatalf("Read filesystem: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	readFile, err := reopened.OpenFile("BIGFILE.bin", os.O_RDONLY)
	if err != nil {
		t.Fatalf("Reopen file: %v", err)
	}
	t.Cleanup(func() { _ = readFile.Close() })
	info, err := readFile.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != fileSize {
		t.Fatalf("file size %d, want %d", info.Size(), fileSize)
	}
	gotHash := sha256.New()
	n, err = io.Copy(gotHash, readFile)
	if err != nil {
		t.Fatalf("read back failed after %d bytes: %v", n, err)
	}
	if n != fileSize || !bytes.Equal(gotHash.Sum(nil), wantHash.Sum(nil)) {
		t.Fatalf("payload mismatch after 2 GiB round trip: read %d bytes", n)
	}

	out, err := exec.Command("e2fsck", "-f", "-n", imagePath).CombinedOutput()
	if err != nil {
		t.Fatalf("e2fsck failed: %v\n%s", err, out)
	}
}

// TestCreateMultiGB exercises Create on filesystems past the 512 MiB
// threshold at which recalculateBlocksize switches to 4 KiB blocks.
// 2 GiB is the size that exposed diskfs/go-diskfs#402 under the
// previous 1 KiB-blocks default (a 64 MiB journal then required 65536
// blocks, exceeding both the 32768-blocks-per-extent cap and the
// inode-root extent tree's 4-extent limit).
func TestCreateMultiGB(t *testing.T) {
	for _, sizeGiB := range []int64{1, 2} {
		t.Run(fmt.Sprintf("%dGiB", sizeGiB), func(t *testing.T) {
			tmp := t.TempDir()
			imgPath := filepath.Join(tmp, "fs.img")
			size := sizeGiB * 1024 * 1024 * 1024
			f, err := os.Create(imgPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(size); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			f, err = os.OpenFile(imgPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			fs, err := Create(file.New(f, false), size, 0, 512, &Params{})
			if err != nil {
				t.Fatalf("Create failed: %v", err)
			}
			if fs == nil {
				t.Fatalf("Create returned nil filesystem")
			}
			if err := f.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			cmd := exec.Command("e2fsck", "-f", "-n", imgPath)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s",
					err, stdout.String(), stderr.String())
			}
		})
	}
}
