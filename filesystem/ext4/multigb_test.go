package ext4

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
)

// TestCreateMultiGB exercises Create on filesystems past the 512 MiB
// threshold at which recalculateBlocksize switches to 4 KiB blocks.
// 2 GiB is the size that exposed diskfs/go-diskfs#402 under the
// previous 1 KiB-blocks default (a 64 MiB journal then required 65536
// blocks, exceeding both the 32768-blocks-per-extent cap and the
// inode-root extent tree's 4-extent limit).
func TestCreateMultiGB(t *testing.T) {
	for _, sizeGiB := range []int64{1, 2} {
		sizeGiB := sizeGiB
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
