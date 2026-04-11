package iso9660_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/testhelper"
)

// createXorrisoJolietISO uses Docker to create a Joliet ISO with xorriso from a workspace directory.
func createXorrisoJolietISO(t *testing.T, workspace, outputPath string) {
	t.Helper()
	output := new(bytes.Buffer)
	outputDir := filepath.Dir(outputPath)
	outputName := filepath.Base(outputPath)

	script := "cd /workspace && xorriso -as mkisofs -J -V JOLIET_REF -o /output/" + outputName + " ."

	mounts := map[string]string{
		workspace: "/workspace",
		outputDir: "/output",
	}

	err := testhelper.DockerRun(
		strings.NewReader(script),
		output, false, true, mounts, intImage,
		"sh", "-c", "cat | sh",
	)
	if err != nil {
		t.Fatalf("xorriso Joliet ISO creation failed: %v\nOutput: %s", err, output.String())
	}
}

// createXorrisoJolietRockRidgeISO creates an ISO with both -J and -R flags.
func createXorrisoJolietRockRidgeISO(t *testing.T, workspace, outputPath string) {
	t.Helper()
	output := new(bytes.Buffer)
	outputDir := filepath.Dir(outputPath)
	outputName := filepath.Base(outputPath)

	script := "cd /workspace && xorriso -as mkisofs -J -R -V JOLIET_RR_REF -o /output/" + outputName + " ."

	mounts := map[string]string{
		workspace: "/workspace",
		outputDir: "/output",
	}

	err := testhelper.DockerRun(
		strings.NewReader(script),
		output, false, true, mounts, intImage,
		"sh", "-c", "cat | sh",
	)
	if err != nil {
		t.Fatalf("xorriso Joliet+RR ISO creation failed: %v\nOutput: %s", err, output.String())
	}
}

// TestJolietWriteReadRoundTrip creates a Joliet ISO with go-diskfs and reads it back
// to verify Joliet filenames (long names, mixed case, spaces) are preserved.
func TestJolietWriteReadRoundTrip(t *testing.T) {
	blocksize := int64(2048)

	dir, err := os.MkdirTemp("", "iso_joliet_roundtrip_test")
	if err != nil {
		t.Fatalf("Failed to create tmpdir: %v", err)
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	// Create files with Joliet-relevant names (long, mixed case, spaces)
	testFiles := []struct {
		name    string
		content string
	}{
		{"short.txt", "short content"},
		{"A Long Filename With Spaces.txt", "long name content"},
		{"MixedCase.Data", "mixed case"},
	}
	for _, tf := range testFiles {
		if err := os.WriteFile(filepath.Join(dir, tf.name), []byte(tf.content), 0o600); err != nil {
			t.Fatalf("Failed to write %s: %v", tf.name, err)
		}
	}

	sub := filepath.Join(dir, "SubDir")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatalf("Failed to write nested file: %v", err)
	}

	// Create ISO with go-diskfs
	f, err := os.CreateTemp("", "iso_joliet_roundtrip_test")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer os.Remove(f.Name())

	bk := file.New(f, false)
	fs, err := iso9660.Create(bk, 0, 0, blocksize, dir)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	err = fs.Finalize(iso9660.FinalizeOptions{Joliet: true})
	if err != nil {
		t.Fatalf("unexpected error fs.Finalize: %v", err)
	}
	f.Close()

	// Read it back with go-diskfs
	f2, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("Failed to reopen ISO: %v", err)
	}
	defer f2.Close()

	bk2 := file.New(f2, true)
	fsRead, err := iso9660.Read(bk2, 0, 0, blocksize)
	if err != nil {
		t.Fatalf("Failed to read ISO: %v", err)
	}

	entries, err := fsRead.ReadDir(".")
	if err != nil {
		t.Fatalf("Failed to ReadDir root: %v", err)
	}

	entryMap := make(map[string]os.FileInfo)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("Failed to get info for %s: %v", e.Name(), err)
		}
		entryMap[e.Name()] = info
	}

	// Verify Joliet filenames are preserved
	for _, tf := range testFiles {
		if _, ok := entryMap[tf.name]; !ok {
			t.Errorf("file %q not found in Joliet ISO readback", tf.name)
		}
	}
	if _, ok := entryMap["SubDir"]; !ok {
		t.Error("SubDir not found in Joliet ISO readback")
	}

	// Verify subdirectory contents
	subEntries, err := fsRead.ReadDir("SubDir")
	if err != nil {
		t.Fatalf("Failed to ReadDir SubDir: %v", err)
	}
	foundNested := false
	for _, e := range subEntries {
		if e.Name() == "nested.txt" {
			foundNested = true
		}
	}
	if !foundNested {
		t.Error("nested.txt not found in SubDir")
	}
}

// TestJolietGoReadXorrisoOutput creates a Joliet ISO with xorriso and reads it with go-diskfs.
// This verifies that go-diskfs can correctly parse Joliet extensions produced by
// the reference implementation.
//
// Gated by TEST_IMAGE environment variable — only runs during `make test`.
func TestJolietGoReadXorrisoOutput(t *testing.T) {
	if intImage == "" {
		t.Skip("skipping xorriso integration test (TEST_IMAGE not set)")
	}

	workspace, err := os.MkdirTemp("", "iso_joliet_xorriso_read")
	if err != nil {
		t.Fatalf("Failed to create workspace: %v", err)
	}
	defer os.RemoveAll(workspace)
	if err := os.Chmod(workspace, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	// Create files
	for _, tc := range []struct {
		name    string
		content string
	}{
		{"hello.txt", "hello world\n"},
		{"A Long Filename For Joliet.txt", "long joliet name\n"},
	} {
		fp := filepath.Join(workspace, tc.name)
		if err := os.WriteFile(fp, []byte(tc.content), 0o600); err != nil {
			t.Fatalf("Failed to write %s: %v", tc.name, err)
		}
	}

	sub := filepath.Join(workspace, "subdir")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("nested\n"), 0o600); err != nil {
		t.Fatalf("Failed to write nested file: %v", err)
	}

	// Create ISO with xorriso -J (Joliet only, no Rock Ridge)
	outputDir, err := os.MkdirTemp("", "iso_joliet_xorriso_out")
	if err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	isoPath := filepath.Join(outputDir, "joliet.iso")
	createXorrisoJolietISO(t, workspace, isoPath)

	info, err := os.Stat(isoPath)
	if err != nil {
		t.Fatalf("xorriso ISO not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("xorriso ISO is empty")
	}

	// Read with go-diskfs
	f, err := os.Open(isoPath)
	if err != nil {
		t.Fatalf("Failed to open xorriso ISO: %v", err)
	}
	defer f.Close()

	blocksize := int64(2048)
	bk := file.New(f, true)
	fs, err := iso9660.Read(bk, 0, 0, blocksize)
	if err != nil {
		t.Fatalf("Failed to read xorriso Joliet ISO: %v", err)
	}

	entries, err := fs.ReadDir(".")
	if err != nil {
		t.Fatalf("Failed to ReadDir root: %v", err)
	}

	foundNames := make(map[string]os.FileInfo)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("Failed to get info for %s: %v", e.Name(), err)
		}
		foundNames[e.Name()] = info
	}
	t.Logf("Found %d entries in xorriso Joliet ISO root:", len(foundNames))
	for name, fi := range foundNames {
		t.Logf("  %q (dir=%v, size=%d)", name, fi.IsDir(), fi.Size())
	}

	// Verify files
	for _, name := range []string{"hello.txt", "A Long Filename For Joliet.txt"} {
		if _, ok := foundNames[name]; !ok {
			t.Errorf("file %q not found in Joliet ISO root", name)
		}
	}

	// Verify subdirectory
	if _, ok := foundNames["subdir"]; !ok {
		t.Errorf("subdir not found in Joliet ISO root")
	} else {
		subEntries, err := fs.ReadDir("subdir")
		if err != nil {
			t.Fatalf("Failed to ReadDir subdir: %v", err)
		}
		found := false
		for _, e := range subEntries {
			if e.Name() == "nested.txt" {
				found = true
			}
		}
		if !found {
			t.Error("nested.txt not found in subdir")
		}
	}

	// Verify file content round-trip
	fh, err := fs.OpenFile("hello.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("Failed to open hello.txt: %v", err)
	}
	buf := make([]byte, 100)
	n, _ := fh.Read(buf)
	if string(buf[:n]) != "hello world\n" {
		t.Errorf("hello.txt content mismatch: got %q", string(buf[:n]))
	}
}

// TestJolietSVDComparison creates identical content with both go-diskfs and xorriso,
// then compares the raw Supplementary Volume Descriptor bytes.
// Verifies that escape sequences, root directory entry, and key SVD fields match.
//
// Gated by TEST_IMAGE environment variable — only runs during `make test`.
func TestJolietSVDComparison(t *testing.T) {
	if intImage == "" {
		t.Skip("skipping Joliet SVD comparison test (TEST_IMAGE not set)")
	}

	blocksize := int64(2048)

	// Create simple workspace
	workspace, err := os.MkdirTemp("", "iso_joliet_svd_compare")
	if err != nil {
		t.Fatalf("Failed to create workspace: %v", err)
	}
	defer os.RemoveAll(workspace)
	if err := os.Chmod(workspace, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	if err := os.WriteFile(filepath.Join(workspace, "hello.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Create go-diskfs Joliet ISO
	goISOFile, err := os.CreateTemp("", "iso_joliet_svd_go")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer os.Remove(goISOFile.Name())

	bk := file.New(goISOFile, false)
	fs, err := iso9660.Create(bk, 0, 0, blocksize, workspace)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	err = fs.Finalize(iso9660.FinalizeOptions{Joliet: true})
	if err != nil {
		t.Fatalf("Failed to finalize go-diskfs Joliet ISO: %v", err)
	}
	goISOFile.Close()

	// Create xorriso Joliet ISO
	xorrisoDir, err := os.MkdirTemp("", "iso_joliet_svd_xorriso_out")
	if err != nil {
		t.Fatalf("Failed to create xorriso output dir: %v", err)
	}
	defer os.RemoveAll(xorrisoDir)

	xorrisoISOPath := filepath.Join(xorrisoDir, "xorriso.iso")
	createXorrisoJolietISO(t, workspace, xorrisoISOPath)

	// Read both ISOs
	goISO, err := os.ReadFile(goISOFile.Name())
	if err != nil {
		t.Fatalf("Failed to read go-diskfs ISO: %v", err)
	}
	xorrisoISO, err := os.ReadFile(xorrisoISOPath)
	if err != nil {
		t.Fatalf("Failed to read xorriso ISO: %v", err)
	}

	// Find SVDs in both ISOs
	goSVD := findSVD(t, goISO, blocksize)
	xorrisoSVD := findSVD(t, xorrisoISO, blocksize)

	if goSVD == nil {
		t.Fatal("no SVD found in go-diskfs ISO")
	}
	if xorrisoSVD == nil {
		t.Fatal("no SVD found in xorriso ISO")
	}

	// Compare escape sequences (bytes 88-90) — must both be Joliet Level 3
	goEsc := goSVD[88:91]
	xorrisoEsc := xorrisoSVD[88:91]
	jolietLevel3 := []byte{0x25, 0x2F, 0x45}

	if !bytes.Equal(goEsc, jolietLevel3) {
		t.Errorf("go-diskfs SVD escape sequences: got %x, want %x", goEsc, jolietLevel3)
	}
	if !bytes.Equal(xorrisoEsc, jolietLevel3) {
		t.Errorf("xorriso SVD escape sequences: got %x, want %x", xorrisoEsc, jolietLevel3)
	}

	// Compare blocksize (bytes 128-129)
	goBlocksize := binary.LittleEndian.Uint16(goSVD[128:130])
	xorrisoBlocksize := binary.LittleEndian.Uint16(xorrisoSVD[128:130])
	if goBlocksize != xorrisoBlocksize {
		t.Errorf("blocksize mismatch: go=%d, xorriso=%d", goBlocksize, xorrisoBlocksize)
	}

	// Both SVDs must have non-zero root directory LBA and path table size
	goRootLBA := binary.LittleEndian.Uint32(goSVD[158:162])
	xorrisoRootLBA := binary.LittleEndian.Uint32(xorrisoSVD[158:162])
	if goRootLBA == 0 {
		t.Error("go-diskfs SVD root directory LBA is zero")
	}
	if xorrisoRootLBA == 0 {
		t.Error("xorriso SVD root directory LBA is zero")
	}

	goPTSize := binary.LittleEndian.Uint32(goSVD[132:136])
	xorrisoPTSize := binary.LittleEndian.Uint32(xorrisoSVD[132:136])
	if goPTSize == 0 {
		t.Error("go-diskfs SVD path table size is zero")
	}
	if xorrisoPTSize == 0 {
		t.Error("xorriso SVD path table size is zero")
	}

	// File structure version (byte 881) must be 1
	if goSVD[881] != 1 {
		t.Errorf("go-diskfs SVD file structure version: got %d, want 1", goSVD[881])
	}
	if xorrisoSVD[881] != 1 {
		t.Errorf("xorriso SVD file structure version: got %d, want 1", xorrisoSVD[881])
	}

	// Verify go-diskfs Joliet root directory contains UCS-2 encoded filenames.
	// (xorriso's internal Joliet directory layout is not checked here — xorriso
	// adds Rock Ridge by default which causes its Joliet tree to differ from a
	// pure-Joliet layout. Interop is covered by TestJolietGoReadXorrisoOutput.)
	goRootDir := readSVDRootDir(t, goISO, goSVD, blocksize)
	helloUCS2 := ucs2Encode("hello.txt")
	if !bytes.Contains(goRootDir, helloUCS2) {
		t.Errorf("go-diskfs SVD root directory does not contain UCS-2 encoded 'hello.txt'\n  looking for: %x\n  root dir (%d bytes): %x", helloUCS2, len(goRootDir), goRootDir)
	}
}

// findSVD scans volume descriptors starting at sector 16 and returns the first
// Supplementary Volume Descriptor (type 2) with Joliet escape sequences.
func findSVD(t *testing.T, iso []byte, blocksize int64) []byte {
	t.Helper()
	for i := 0; ; i++ {
		offset := (16 + int64(i)) * blocksize
		if offset+blocksize > int64(len(iso)) {
			return nil
		}
		vd := iso[offset : offset+blocksize]
		if vd[0] == 0xFF { // terminator
			return nil
		}
		if vd[0] == 0x02 && string(vd[1:6]) == "CD001" {
			// check for Joliet escape sequences
			esc := vd[88:91]
			if bytes.Equal(esc, []byte{0x25, 0x2F, 0x40}) ||
				bytes.Equal(esc, []byte{0x25, 0x2F, 0x43}) ||
				bytes.Equal(esc, []byte{0x25, 0x2F, 0x45}) {
				return vd
			}
		}
	}
}

// readSVDRootDir reads the root directory extent from an SVD.
func readSVDRootDir(t *testing.T, iso, svd []byte, blocksize int64) []byte {
	t.Helper()
	rootLBA := binary.LittleEndian.Uint32(svd[158:162])
	rootSize := binary.LittleEndian.Uint32(svd[166:170])
	offset := int64(rootLBA) * blocksize
	if offset+int64(rootSize) > int64(len(iso)) {
		t.Fatalf("SVD root directory extends beyond ISO (LBA=%d, size=%d)", rootLBA, rootSize)
	}
	return iso[offset : offset+int64(rootSize)]
}

// ucs2Encode converts a Go string to UCS-2 big-endian bytes.
func ucs2Encode(s string) []byte {
	b := make([]byte, len(s)*2)
	for i, r := range s {
		b[i*2] = byte(r >> 8)
		b[i*2+1] = byte(r & 0xFF)
	}
	return b
}
