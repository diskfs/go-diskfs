package ext4

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/google/uuid"
)

// TestCreateWithBlockSizes tests Create with various valid block sizes.
func TestCreateWithBlockSizes(t *testing.T) {
	tests := []struct {
		name            string
		sectorsPerBlock uint8
		size            int64
		features        []FeatureOpt
	}{
		{"1KB blocks (2 sectors)", 2, 100 * MB, nil},
		// Larger block sizes need resize_inode disabled since we don't yet
		// support reserved GDT blocks for non-1KB block sizes.
		{"2KB blocks (4 sectors)", 4, 100 * MB, []FeatureOpt{WithFeatureReservedGDTBlocksForExpansion(false)}},
		{"4KB blocks (8 sectors)", 8, 100 * MB, []FeatureOpt{WithFeatureReservedGDTBlocksForExpansion(false)}},
		{"8KB blocks (16 sectors)", 16, 100 * MB, []FeatureOpt{WithFeatureReservedGDTBlocksForExpansion(false)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outfile, f := testCreateEmptyFile(t, tt.size)
			defer f.Close()
			params := &Params{
				SectorsPerBlock: tt.sectorsPerBlock,
				Features:        tt.features,
			}
			fs, err := Create(file.New(f, false), tt.size, 0, 512, params)
			if err != nil {
				t.Fatalf("Create failed with %s: %v", tt.name, err)
			}
			if fs == nil {
				t.Fatalf("Expected non-nil filesystem")
			}
			if err := f.Sync(); err != nil {
				t.Fatalf("Error syncing: %v", err)
			}
			cmd := exec.Command("e2fsck", "-f", "-n", outfile)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("e2fsck failed for %s: %v\nstdout:\n%s\nstderr:\n%s",
					tt.name, err, stdout.String(), stderr.String())
			}
		})
	}
}

// TestCreateInvalidBlockSize verifies that invalid SectorsPerBlock values are rejected.
func TestCreateInvalidBlockSize(t *testing.T) {
	tests := []struct {
		name            string
		sectorsPerBlock uint8
	}{
		{"too small (1)", 1},
		{"too large (255)", 255},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, f := testCreateEmptyFile(t, 100*MB)
			defer f.Close()
			params := &Params{
				SectorsPerBlock: tt.sectorsPerBlock,
			}
			fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
			if err == nil {
				t.Fatalf("expected error for SectorsPerBlock=%d, got nil (fs=%v)", tt.sectorsPerBlock, fs)
			}
		})
	}
}

// TestCreateInvalidSectorSize verifies that invalid sector sizes are rejected.
func TestCreateInvalidSectorSize(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	_, err := Create(file.New(f, false), 100*MB, 0, 1024, &Params{})
	if err == nil {
		t.Fatalf("expected error for sectorsize=1024, got nil")
	}
}

// TestCreateNilParams verifies that Create works with nil Params (defaults).
func TestCreateNilParams(t *testing.T) {
	outfile, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, nil)
	if err != nil {
		t.Fatalf("Create with nil params failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}
	cmd := exec.Command("e2fsck", "-f", "-n", outfile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

// TestCreateWithCustomUUID verifies that Create uses a provided UUID.
func TestCreateWithCustomUUID(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	customUUID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	params := &Params{
		UUID: &customUUID,
	}
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
	if err != nil {
		t.Fatalf("Create with custom UUID failed: %v", err)
	}
	if fs.superblock.uuid == nil {
		t.Fatalf("expected UUID to be set")
	}
	if *fs.superblock.uuid != customUUID {
		t.Errorf("expected UUID %s, got %s", customUUID, *fs.superblock.uuid)
	}
}

// TestCreateWithCustomVolumeName verifies that Create uses a provided volume name.
func TestCreateWithCustomVolumeName(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	params := &Params{
		VolumeName: "MY_VOLUME",
	}
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
	if err != nil {
		t.Fatalf("Create with custom volume name failed: %v", err)
	}
	if fs.superblock.volumeLabel != "MY_VOLUME" {
		t.Errorf("expected volume label 'MY_VOLUME', got %q", fs.superblock.volumeLabel)
	}
	if fs.Label() != "MY_VOLUME" {
		t.Errorf("Label() returned %q, expected 'MY_VOLUME'", fs.Label())
	}
}

// TestCreateWithCustomInodeRatio verifies that Create respects custom InodeRatio.
func TestCreateWithCustomInodeRatio(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	// Use a larger inode ratio to reduce inode count
	params := &Params{
		InodeRatio: 32768,
	}
	fsLargeRatio, err := Create(file.New(f, false), 100*MB, 0, 512, params)
	if err != nil {
		t.Fatalf("Create with InodeRatio=32768 failed: %v", err)
	}

	// Create with default ratio
	_, f2 := testCreateEmptyFile(t, 100*MB)
	defer f2.Close()
	fsDefault, err := Create(file.New(f2, false), 100*MB, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create with default params failed: %v", err)
	}

	if fsLargeRatio.superblock.inodeCount >= fsDefault.superblock.inodeCount {
		t.Errorf("expected fewer inodes with larger inode ratio: got %d (ratio=32768) vs %d (default)",
			fsLargeRatio.superblock.inodeCount, fsDefault.superblock.inodeCount)
	}
}

// TestCreateWithCustomBlocksPerGroup tests custom BlocksPerGroup.
func TestCreateWithCustomBlocksPerGroup(t *testing.T) {
	t.Run("valid custom blocks per group", func(t *testing.T) {
		_, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			BlocksPerGroup: 8192,
		}
		fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err != nil {
			t.Fatalf("Create with BlocksPerGroup=8192 failed: %v", err)
		}
		if fs.superblock.blocksPerGroup != 8192 {
			t.Errorf("expected blocks per group 8192, got %d", fs.superblock.blocksPerGroup)
		}
	})

	t.Run("too small blocks per group", func(t *testing.T) {
		_, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			BlocksPerGroup: 100, // below minBlocksPerGroup (256)
		}
		_, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err == nil {
			t.Fatalf("expected error for BlocksPerGroup=100, got nil")
		}
	})

	t.Run("not divisible by 8", func(t *testing.T) {
		_, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			BlocksPerGroup: 1001,
		}
		_, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err == nil {
			t.Fatalf("expected error for BlocksPerGroup=1001 (not divisible by 8), got nil")
		}
	})
}

// TestCreateWithOffset tests Create with a non-zero start offset.
func TestCreateWithOffset(t *testing.T) {
	offset := int64(1024)
	size := 100 * MB
	outfile, f := testCreateEmptyFile(t, size+offset)
	defer f.Close()

	fs, err := Create(file.New(f, false), size, offset, 512, &Params{})
	if err != nil {
		t.Fatalf("Create with offset failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}

	// re-read from offset
	f2, err := os.Open(outfile)
	if err != nil {
		t.Fatalf("Error reopening file: %v", err)
	}
	defer f2.Close()

	b := file.New(f2, true)
	fs2, err := Read(b, size, offset, 512)
	if err != nil {
		t.Fatalf("Error reading filesystem from offset: %v", err)
	}
	if fs2 == nil {
		t.Fatalf("Expected non-nil filesystem after reading from offset")
	}
}

// TestCreateZeroSectorSize verifies that sectorsize=0 defaults to 512.
func TestCreateZeroSectorSize(t *testing.T) {
	outfile, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	fs, err := Create(file.New(f, false), 100*MB, 0, 0, &Params{})
	if err != nil {
		t.Fatalf("Create with sectorsize=0 failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}
	cmd := exec.Command("e2fsck", "-f", "-n", outfile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
}

// TestCreateWithFeatures tests Create with various feature flags.
func TestCreateWithFeatures(t *testing.T) {
	t.Run("without journal", func(t *testing.T) {
		outfile, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			Features: []FeatureOpt{
				WithFeatureHasJournal(false),
			},
		}
		fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err != nil {
			t.Fatalf("Create without journal failed: %v", err)
		}
		if fs.superblock.features.hasJournal {
			t.Errorf("expected hasJournal=false")
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("Error syncing: %v", err)
		}
		cmd := exec.Command("e2fsck", "-f", "-n", outfile)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	})

	t.Run("with metadata checksums", func(t *testing.T) {
		outfile, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			Features: []FeatureOpt{
				WithFeatureMetadataChecksums(true),
			},
		}
		fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err != nil {
			t.Fatalf("Create with metadata checksums failed: %v", err)
		}
		if !fs.superblock.features.metadataChecksums {
			t.Errorf("expected metadataChecksums=true")
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("Error syncing: %v", err)
		}
		cmd := exec.Command("e2fsck", "-f", "-n", outfile)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("e2fsck failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	})

	t.Run("sparse super v2", func(t *testing.T) {
		_, f := testCreateEmptyFile(t, 100*MB)
		defer f.Close()
		params := &Params{
			SparseSuperVersion: 2,
		}
		fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
		if err != nil {
			t.Fatalf("Create with SparseSuperVersion=2 failed: %v", err)
		}
		if fs == nil {
			t.Fatalf("Expected non-nil filesystem")
		}
	})
}

// TestCreateWithMountOptions tests Create with custom default mount options.
func TestCreateWithMountOptions(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	params := &Params{
		DefaultMountOpts: []MountOpt{
			WithDefaultMountOptionPOSIXACLs(true),
			WithDefaultMountOptionUserspaceXattrs(true),
		},
	}
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
	if err != nil {
		t.Fatalf("Create with mount options failed: %v", err)
	}
	if !fs.superblock.defaultMountOptions.posixACLs {
		t.Errorf("expected POSIX ACLs enabled")
	}
	if !fs.superblock.defaultMountOptions.userspaceExtendedAttributes {
		t.Errorf("expected userspace xattrs enabled")
	}
}

// TestCreateSmallFilesystem tests Create with a small filesystem size.
func TestCreateSmallFilesystem(t *testing.T) {
	// 10MB is small but should still work
	size := 10 * MB
	outfile, f := testCreateEmptyFile(t, size)
	defer f.Close()
	fs, err := Create(file.New(f, false), size, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create with 10MB size failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}
	cmd := exec.Command("e2fsck", "-f", "-n", outfile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed for small filesystem: %v\nstdout:\n%s\nstderr:\n%s",
			err, stdout.String(), stderr.String())
	}
}

// TestCreateLargeFilesystem tests Create with a larger filesystem (500MB).
func TestCreateLargeFilesystem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large filesystem test in short mode")
	}
	size := 500 * MB
	outfile, f := testCreateEmptyFile(t, size)
	defer f.Close()
	fs, err := Create(file.New(f, false), size, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create with 500MB size failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}
	cmd := exec.Command("e2fsck", "-f", "-n", outfile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("e2fsck failed for large filesystem: %v\nstdout:\n%s\nstderr:\n%s",
			err, stdout.String(), stderr.String())
	}
}

// TestCreateWithCustomReservedBlocks tests Create with a custom reserved blocks percent.
func TestCreateWithCustomReservedBlocks(t *testing.T) {
	_, f := testCreateEmptyFile(t, 100*MB)
	defer f.Close()
	params := &Params{
		ReservedBlocksPercent: 10,
	}
	fs, err := Create(file.New(f, false), 100*MB, 0, 512, params)
	if err != nil {
		t.Fatalf("Create with ReservedBlocksPercent=10 failed: %v", err)
	}
	if fs == nil {
		t.Fatalf("Expected non-nil filesystem")
	}
}

// TestCreateWriteReadRoundTrip creates a filesystem, writes files, re-reads, and verifies.
func TestCreateWriteReadRoundTrip(t *testing.T) {
	size := 100 * MB
	outfile, f := testCreateEmptyFile(t, size)
	defer f.Close()

	b := file.New(f, false)
	fs, err := Create(b, size, 0, 512, &Params{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write a file
	content := []byte("Hello, ext4 roundtrip test!")
	ext4File, err := fs.OpenFile("testfile.txt", os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile for write failed: %v", err)
	}
	n, err := ext4File.Write(content)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(content) {
		t.Fatalf("short write: %d vs %d", n, len(content))
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		t.Fatalf("Error syncing: %v", err)
	}

	// Re-open and re-read
	f2, err := os.Open(outfile)
	if err != nil {
		t.Fatalf("Error reopening: %v", err)
	}
	defer f2.Close()

	b2 := file.New(f2, true)
	fs2, err := Read(b2, size, 0, 512)
	if err != nil {
		t.Fatalf("Error re-reading filesystem: %v", err)
	}

	readBack, err := fs2.ReadFile("testfile.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(readBack, content) {
		t.Errorf("round-trip mismatch: wrote %q, read %q", content, readBack)
	}
}
