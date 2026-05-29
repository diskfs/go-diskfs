package squashfs_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	diskfs "github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/partition/gpt"
)

// TestSquashfsInPartition exercises a squashfs that does not begin at
// offset 0 of its backend, i.e. one that lives inside a partition.
//
// Both Finalize and Read must honor the partition's byte offset:
//   - Finalize must write the filesystem (starting with the superblock)
//     at the partition's offset, not at backend offset 0 where it would
//     clobber the protective MBR / GPT.
//   - Read must read the superblock and all of its follow-on metadata
//     tables (fragment, xattr, id) from that same offset.
//
// Before the offset fix, Finalize wrote the superblock to backend offset 0
// and Read mis-biased its table reads, so a squashfs created inside a
// partition was both misplaced and unreadable. The existing whole-disk
// tests never caught this because they always pass start == 0.
func TestSquashfsInPartition(t *testing.T) {
	const (
		sectorSize  = 4096 // squashfs requires a blocksize >= 4096
		diskSize    = 32 * 1024 * 1024
		partStart   = 256 // sectors => 1 MiB into the disk
		partSize    = 8 * 1024 * 1024
		filename    = "marker.txt"
		fileContent = "squashfs partition-offset round-trip\n"
	)

	imgPath := filepath.Join(t.TempDir(), "disk.img")

	d, err := diskfs.Create(imgPath, diskSize, sectorSize)
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}

	// Lay down a GPT with a single partition that starts well inside the
	// disk, so the filesystem's start offset is non-zero.
	table := &gpt.Table{
		LogicalSectorSize:  sectorSize,
		PhysicalSectorSize: sectorSize,
		ProtectiveMBR:      true,
		Partitions: []*gpt.Partition{
			{Index: 1, Start: partStart, Size: partSize, Type: gpt.LinuxFilesystem, Name: "rootfs"},
		},
	}
	if err := d.Partition(table); err != nil {
		t.Fatalf("write partition table: %v", err)
	}

	// Reopen so the partition is re-read from disk and carries the disk's
	// sector size; the in-memory partitions built above do not, so their
	// GetStart() would otherwise use the 512-byte default.
	d, err = diskfs.Open(imgPath, diskfs.WithSectorSize(sectorSize))
	if err != nil {
		t.Fatalf("reopen disk: %v", err)
	}
	if _, err := d.GetPartitionTable(); err != nil {
		t.Fatalf("re-read partition table: %v", err)
	}

	// Build a squashfs in partition 1 with a known marker file.
	fs, err := d.CreateFilesystem(disk.FilesystemSpec{Partition: 1, FSType: filesystem.TypeSquashfs})
	if err != nil {
		t.Fatalf("CreateFilesystem(squashfs): %v", err)
	}
	rw, err := fs.OpenFile(filename, os.O_CREATE|os.O_RDWR)
	if err != nil {
		t.Fatalf("OpenFile for write: %v", err)
	}
	if _, err := rw.Write([]byte(fileContent)); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	sqs, ok := fs.(*squashfs.FileSystem)
	if !ok {
		t.Fatalf("filesystem is %T, want *squashfs.FileSystem", fs)
	}
	if err := sqs.Finalize(squashfs.FinalizeOptions{
		NoCompressInodes:    true,
		NoCompressData:      true,
		NoCompressFragments: true,
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Finalize must not have written over the GPT, and the squashfs
	// superblock must sit at the partition's byte offset.
	raw, err := os.ReadFile(imgPath)
	if err != nil {
		t.Fatalf("read disk image: %v", err)
	}
	if !bytes.HasPrefix(raw[sectorSize:sectorSize+8], []byte("EFI PART")) {
		t.Fatal("GPT primary header was overwritten by Finalize")
	}
	partOffset := int64(partStart) * sectorSize
	if !bytes.HasPrefix(raw[partOffset:partOffset+4], []byte("hsqs")) {
		t.Fatalf("squashfs superblock did not land at partition offset %d", partOffset)
	}

	// Reopen and read the filesystem back through the partition, which
	// drives Read (and its metadata-table reads) at the non-zero start.
	d2, err := diskfs.Open(imgPath, diskfs.WithSectorSize(sectorSize))
	if err != nil {
		t.Fatalf("reopen disk: %v", err)
	}
	fs2, err := d2.GetFilesystem(1)
	if err != nil {
		t.Fatalf("GetFilesystem(1): %v", err)
	}
	if fs2.Type() != filesystem.TypeSquashfs {
		t.Fatalf("filesystem read back as %v, want squashfs", fs2.Type())
	}
	mf, err := fs2.OpenFile(filename, os.O_RDONLY)
	if err != nil {
		t.Fatalf("OpenFile for read: %v", err)
	}
	got, err := io.ReadAll(mf)
	if err != nil {
		t.Fatalf("read marker back: %v", err)
	}
	if string(got) != fileContent {
		t.Errorf("marker content mismatch: got %q, want %q", string(got), fileContent)
	}
}
