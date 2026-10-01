package squashfs_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/squashfs"
	"github.com/diskfs/go-diskfs/testhelper"
)

// Compressor options follow the superblock as an uncompressed metadata block,
// and the superblock flags them.
func TestFinalizeCompressorOptions(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "squashfs_options_test")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer f.Close()
	fs, err := squashfs.Create(file.New(f, false), 0, 0, 4096)
	if err != nil {
		t.Fatalf("Failed to squashfs.Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fs.Workspace(), "a"), []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fs.Finalize(squashfs.FinalizeOptions{Compression: &squashfs.CompressorGzip{CompressionLevel: 9, WindowSize: 15}}); err != nil {
		t.Fatalf("unexpected error fs.Finalize(): %v", err)
	}

	b := make([]byte, 98)
	if _, err := f.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	if flags := binary.LittleEndian.Uint16(b[24:26]); flags&0x0400 == 0 {
		t.Errorf("superblock flags %#x without compressor options", flags)
	}
	// gzip options are 8 bytes
	if header := binary.LittleEndian.Uint16(b[96:98]); header != 0x8000|8 {
		t.Errorf("compressor options metadata header %#x, expected %#x", header, 0x8000|8)
	}

	if intImage == "" {
		return
	}
	output := new(strings.Builder)
	mounts := map[string]string{f.Name(): "/file.sqs"}
	if err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "unsquashfs", "-s", "/file.sqs"); err != nil {
		t.Errorf("unsquashfs: %v\n%s", err, output)
	}
	if !strings.Contains(output.String(), "compression-level 9") {
		t.Errorf("unsquashfs does not report the compressor options:\n%s", output)
	}
}
