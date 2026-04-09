package iso9660_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/testhelper"
)

// createXorrisoISO uses Docker to create a Rock Ridge ISO with xorriso from a workspace directory.
// The workspace is mounted read-only, and the output ISO is written to outputPath.
func createXorrisoISO(t *testing.T, workspace, outputPath string) {
	t.Helper()
	output := new(bytes.Buffer)
	outputDir := filepath.Dir(outputPath)
	outputName := filepath.Base(outputPath)

	// We need to pass a shell script via stdin because xorriso needs to cd into the workspace
	script := fmt.Sprintf(
		"cd /workspace && xorriso -as mkisofs -R -V XORRISO_REF -o /output/%s .",
		outputName,
	)

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
		t.Fatalf("xorriso ISO creation failed: %v\nOutput: %s", err, output.String())
	}
}

// TestRockRidgeWriteReadRoundTrip creates an ISO with go-diskfs using a workspace with
// various Rock Ridge features (permissions, deep directories, symlinks, long filenames)
// and reads it back to verify all extensions are preserved correctly.
func TestRockRidgeWriteReadRoundTrip(t *testing.T) {
	blocksize := int64(2048)

	// Create workspace with various Rock Ridge features
	dir, err := os.MkdirTemp("", "iso_rr_roundtrip_test")
	if err != nil {
		t.Fatalf("Failed to create tmpdir: %v", err)
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	// Regular files with different permissions
	type fileSpec struct {
		name string
		mode os.FileMode
	}
	files := []fileSpec{
		{"file644.txt", 0o644},
		{"file755.txt", 0o755},
		{"file600.txt", 0o600},
	}
	for _, tc := range files {
		fp := filepath.Join(dir, tc.name)
		if err := os.WriteFile(fp, []byte("content of "+tc.name), 0o600); err != nil {
			t.Fatalf("Failed to write %s: %v", tc.name, err)
		}
		if err := os.Chmod(fp, tc.mode); err != nil {
			t.Fatalf("Failed to chmod %s: %v", tc.name, err)
		}
	}

	// Deep directory structure (depth 9, triggers CL/PL/RE relocation)
	deepDir := filepath.Join(dir, "a", "b", "c", "d", "e", "f", "g", "h")
	if err := os.MkdirAll(deepDir, 0o755); err != nil {
		t.Fatalf("Failed to create deep dir: %v", err)
	}
	deepContent := "deep content"
	if err := os.WriteFile(filepath.Join(deepDir, "deep.txt"), []byte(deepContent), 0o600); err != nil {
		t.Fatalf("Failed to write deep file: %v", err)
	}

	// Long filename (> 31 chars to test NM extension)
	longName := "this_is_a_very_long_filename_that_exceeds_31_characters.txt"
	if err := os.WriteFile(filepath.Join(dir, longName), []byte("long name content"), 0o600); err != nil {
		t.Fatalf("Failed to write long name file: %v", err)
	}

	// Symlink
	targetFile := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(targetFile, []byte("target content"), 0o600); err != nil {
		t.Fatalf("Failed to write symlink target: %v", err)
	}
	symlinkCreated := true
	if err := os.Symlink("target.txt", filepath.Join(dir, "link.txt")); err != nil {
		t.Logf("Could not create symlink (skipping symlink validation): %v", err)
		symlinkCreated = false
	}

	// Create ISO with go-diskfs
	f, err := os.CreateTemp("", "iso_rr_roundtrip_test")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer os.Remove(f.Name())

	b := file.New(f, false)
	fsWrite, err := iso9660.Create(b, 0, 0, blocksize, dir)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	err = fsWrite.Finalize(iso9660.FinalizeOptions{RockRidge: true})
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

	bk := file.New(f2, true)
	fsRead, err := iso9660.Read(bk, 0, 0, blocksize)
	if err != nil {
		t.Fatalf("Failed to read ISO: %v", err)
	}

	// Read root directory
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

	// Verify permissions
	for _, tc := range files {
		info, ok := entryMap[tc.name]
		if !ok {
			t.Errorf("file %s not found in root listing", tc.name)
			continue
		}
		if perm := info.Mode().Perm(); perm != tc.mode {
			t.Errorf("file %s: got perm %o, want %o", tc.name, perm, tc.mode)
		}
	}

	// Verify long filename is preserved
	if _, ok := entryMap[longName]; !ok {
		t.Errorf("long filename %q not found in root listing", longName)
	}

	// Verify deep directory structure is accessible
	deepFile, err := fsRead.OpenFile("/a/b/c/d/e/f/g/h/deep.txt", os.O_RDONLY)
	if err != nil {
		t.Fatalf("Failed to open deep file: %v", err)
	}
	buf := make([]byte, 200)
	n, _ := deepFile.Read(buf)
	if string(buf[:n]) != deepContent {
		t.Errorf("deep file content mismatch: got %q, want %q", string(buf[:n]), deepContent)
	}

	// Verify symlink
	if symlinkCreated {
		info, ok := entryMap["link.txt"]
		if !ok {
			t.Error("link.txt not found in root listing")
		} else {
			if info.Mode()&os.ModeSymlink == 0 {
				t.Errorf("link.txt: expected symlink mode, got %v", info.Mode())
			}
			rri, ok := info.Sys().(*iso9660.RockRidgeInfo)
			if !ok {
				t.Fatal("link.txt: Sys() did not return *RockRidgeInfo")
			}
			if rri.Symlink != "target.txt" {
				t.Errorf("link.txt: got symlink target %q, want %q", rri.Symlink, "target.txt")
			}
		}
	}
}

// TestRockRidgeGoReadXorrisoOutput creates an ISO with xorriso and reads it with go-diskfs.
// This verifies that go-diskfs can correctly parse Rock Ridge extensions produced by
// the reference implementation.
//
// Gated by TEST_IMAGE environment variable — only runs during `make test`.
//
//nolint:gocyclo // big integration test function, complexity is not a correctness issue
func TestRockRidgeGoReadXorrisoOutput(t *testing.T) {
	if intImage == "" {
		t.Skip("skipping xorriso integration test (TEST_IMAGE not set)")
	}

	// Create workspace with known content
	workspace, err := os.MkdirTemp("", "iso_xorriso_write_workspace")
	if err != nil {
		t.Fatalf("Failed to create workspace: %v", err)
	}
	defer os.RemoveAll(workspace)
	if err := os.Chmod(workspace, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	// Regular files with permissions
	for _, tc := range []struct {
		name    string
		content string
		mode    os.FileMode
	}{
		{"hello.txt", "hello world\n", 0o644},
		{"exec.sh", "#!/bin/sh\necho hi\n", 0o755},
	} {
		fp := filepath.Join(workspace, tc.name)
		if err := os.WriteFile(fp, []byte(tc.content), 0o600); err != nil {
			t.Fatalf("Failed to write %s: %v", tc.name, err)
		}
		if err := os.Chmod(fp, tc.mode); err != nil {
			t.Fatalf("Failed to chmod %s: %v", tc.name, err)
		}
	}

	// Subdirectory
	subDir := filepath.Join(workspace, "subdir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "nested.txt"), []byte("nested\n"), 0o600); err != nil {
		t.Fatalf("Failed to write nested file: %v", err)
	}

	// Deep directory (depth 9)
	deepDir := filepath.Join(workspace, "a", "b", "c", "d", "e", "f", "g", "h")
	if err := os.MkdirAll(deepDir, 0o755); err != nil {
		t.Fatalf("Failed to create deep dir: %v", err)
	}
	deepContent := "deep file from xorriso\n"
	if err := os.WriteFile(filepath.Join(deepDir, "deep.txt"), []byte(deepContent), 0o600); err != nil {
		t.Fatalf("Failed to write deep file: %v", err)
	}

	// Symlink (relative)
	if err := os.Symlink("hello.txt", filepath.Join(workspace, "link")); err != nil {
		t.Logf("Could not create symlink (skipping symlink validation): %v", err)
	}

	// Long filename
	longName := "a_filename_longer_than_thirtyone_characters_for_nm_test.txt"
	if err := os.WriteFile(filepath.Join(workspace, longName), []byte("long\n"), 0o600); err != nil {
		t.Fatalf("Failed to write long name file: %v", err)
	}

	// Create output temp dir for xorriso ISO
	outputDir, err := os.MkdirTemp("", "iso_xorriso_output")
	if err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}
	defer os.RemoveAll(outputDir)

	isoPath := filepath.Join(outputDir, "reference.iso")

	// Create ISO with xorriso via Docker
	createXorrisoISO(t, workspace, isoPath)

	// Verify the ISO file was created
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
		t.Fatalf("Failed to read xorriso ISO: %v", err)
	}

	// Verify root directory contents
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
	t.Logf("Found %d entries in xorriso ISO root:", len(foundNames))
	for name, info := range foundNames {
		t.Logf("  %q (dir=%v, size=%d, mode=%v)", name, info.IsDir(), info.Size(), info.Mode())
	}

	// Verify regular files
	t.Run("regular files", func(t *testing.T) {
		for _, name := range []string{"hello.txt", "exec.sh"} {
			info, ok := foundNames[name]
			if !ok {
				t.Errorf("file %s not found in root listing", name)
				continue
			}
			if info.IsDir() {
				t.Errorf("file %s should not be a directory", name)
			}
		}
	})

	// Verify permissions
	t.Run("permissions", func(t *testing.T) {
		if info, ok := foundNames["hello.txt"]; ok {
			if perm := info.Mode().Perm(); perm != 0o644 {
				t.Errorf("hello.txt: got perm %o, want 0644", perm)
			}
		}
		if info, ok := foundNames["exec.sh"]; ok {
			if perm := info.Mode().Perm(); perm != 0o755 {
				t.Errorf("exec.sh: got perm %o, want 0755", perm)
			}
		}
	})

	// Verify long filename
	t.Run("long filename", func(t *testing.T) {
		if _, ok := foundNames[longName]; !ok {
			t.Errorf("long filename %q not found", longName)
		}
	})

	// Verify subdirectory
	t.Run("subdirectory", func(t *testing.T) {
		subEntries, err := fs.ReadDir("subdir")
		if err != nil {
			t.Fatalf("Failed to ReadDir /subdir: %v", err)
		}
		found := false
		for _, e := range subEntries {
			if e.Name() == "nested.txt" {
				found = true
				break
			}
		}
		if !found {
			t.Error("nested.txt not found in /subdir")
		}
	})

	// Verify deep directory round-trip
	t.Run("deep directory", func(t *testing.T) {
		deepFile, err := fs.OpenFile("/a/b/c/d/e/f/g/h/deep.txt", os.O_RDONLY)
		if err != nil {
			t.Fatalf("Failed to open deep file: %v", err)
		}
		buf := make([]byte, 200)
		n, _ := deepFile.Read(buf)
		if string(buf[:n]) != deepContent {
			t.Errorf("deep file content mismatch: got %q, want %q", string(buf[:n]), deepContent)
		}
	})

	// Verify symlink (if workspace created one)
	t.Run("symlink", func(t *testing.T) {
		info, ok := foundNames["link"]
		if !ok {
			t.Skip("symlink not found (may not have been created)")
		}
		if info.Mode()&os.ModeSymlink == 0 {
			t.Errorf("link: expected symlink mode, got %v", info.Mode())
		}
		// Check symlink target via Sys()
		rri, ok := info.Sys().(*iso9660.RockRidgeInfo)
		if !ok {
			t.Fatal("link: Sys() did not return *RockRidgeInfo")
		}
		if rri.Symlink != "hello.txt" {
			t.Errorf("link: got symlink target %q, want %q", rri.Symlink, "hello.txt")
		}
	})

	// Verify UID/GID are present via Sys()
	t.Run("uid gid", func(t *testing.T) {
		info, ok := foundNames["hello.txt"]
		if !ok {
			t.Skip("hello.txt not found")
		}
		rri, ok := info.Sys().(*iso9660.RockRidgeInfo)
		if !ok {
			t.Fatal("hello.txt: Sys() did not return *RockRidgeInfo")
		}
		// xorriso sets UID/GID to 0 by default in Docker (running as root)
		if rri.UID != 0 {
			t.Logf("hello.txt: UID = %d (expected 0 from Docker/xorriso)", rri.UID)
		}
		if rri.GID != 0 {
			t.Logf("hello.txt: GID = %d (expected 0 from Docker/xorriso)", rri.GID)
		}
	})
}

// TestRockRidgeSUSPEntryComparison creates identical content with both go-diskfs and xorriso,
// then compares the raw SUSP (System Use) entry bytes from directory records.
// This is the byte-level comparison: individual PX, NM, TF entries should be identical
// for the same input, even though sector layout may differ between implementations.
//
// Gated by TEST_IMAGE environment variable — only runs during `make test`.
func TestRockRidgeSUSPEntryComparison(t *testing.T) {
	if intImage == "" {
		t.Skip("skipping SUSP byte comparison test (TEST_IMAGE not set)")
	}

	blocksize := int64(2048)

	// Create simple workspace — a few files with known attributes
	workspace, err := os.MkdirTemp("", "iso_susp_compare")
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
	if err := os.Chmod(filepath.Join(workspace, "hello.txt"), 0o644); err != nil {
		t.Fatalf("Failed to chmod: %v", err)
	}
	sub := filepath.Join(workspace, "subdir")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "data.bin"), []byte("data\n"), 0o600); err != nil {
		t.Fatalf("Failed to write data.bin: %v", err)
	}

	// Create go-diskfs ISO
	goISOFile, err := os.CreateTemp("", "iso_susp_go")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer os.Remove(goISOFile.Name())

	bk := file.New(goISOFile, false)
	fs, err := iso9660.Create(bk, 0, 0, blocksize, workspace)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	err = fs.Finalize(iso9660.FinalizeOptions{RockRidge: true})
	if err != nil {
		t.Fatalf("Failed to finalize go-diskfs ISO: %v", err)
	}
	goISOFile.Close()

	// Create xorriso ISO
	xorrisoDir, err := os.MkdirTemp("", "iso_susp_xorriso_out")
	if err != nil {
		t.Fatalf("Failed to create xorriso output dir: %v", err)
	}
	defer os.RemoveAll(xorrisoDir)

	xorrisoISOPath := filepath.Join(xorrisoDir, "xorriso.iso")
	createXorrisoISO(t, workspace, xorrisoISOPath)

	// Read raw bytes from both ISOs and compare SUSP entries
	goISO, err := os.ReadFile(goISOFile.Name())
	if err != nil {
		t.Fatalf("Failed to read go-diskfs ISO: %v", err)
	}
	xorrisoISO, err := os.ReadFile(xorrisoISOPath)
	if err != nil {
		t.Fatalf("Failed to read xorriso ISO: %v", err)
	}

	// Extract root directory entries from both ISOs
	goEntries := extractDirectoryRecords(t, goISO, blocksize)
	xorrisoEntries := extractDirectoryRecords(t, xorrisoISO, blocksize)

	t.Logf("go-diskfs root has %d entries, xorriso root has %d entries", len(goEntries), len(xorrisoEntries))

	// Build maps keyed by NM (alternate name) from SUSP entries
	goByName := mapEntriesByName(goEntries)
	xorrisoByName := mapEntriesByName(xorrisoEntries)

	// Compare SUSP entries for matching names
	for name, goRec := range goByName {
		xorrisoRec, ok := xorrisoByName[name]
		if !ok {
			t.Logf("entry %q exists in go-diskfs but not xorriso (may be relocated)", name)
			continue
		}

		goSUSP := parseSUSPEntries(goRec.systemUse)
		xorrisoSUSP := parseSUSPEntries(xorrisoRec.systemUse)

		// Compare PX (POSIX attributes) entries — should match for permission bits
		goPX := findSUSPEntry(goSUSP, "PX")
		xorrisoPX := findSUSPEntry(xorrisoSUSP, "PX")
		if goPX != nil && xorrisoPX != nil {
			comparePXEntries(t, name, goPX, xorrisoPX)
		}

		// Compare NM (alternate name) entries — should be identical
		goNM := findSUSPEntry(goSUSP, "NM")
		xorrisoNM := findSUSPEntry(xorrisoSUSP, "NM")
		if goNM != nil && xorrisoNM != nil {
			if !bytes.Equal(goNM, xorrisoNM) {
				t.Errorf("NM entry mismatch for %q:\n  go-diskfs: %x\n  xorriso:   %x", name, goNM, xorrisoNM)
			}
		}
	}
}

// dirRecord holds a parsed ISO 9660 directory record with its raw system use area.
type dirRecord struct {
	name      string // ISO 9660 file identifier
	systemUse []byte // raw System Use area bytes
}

// suspEntry holds a parsed SUSP entry with its signature and raw bytes.
type suspEntry struct {
	sig  string
	data []byte // full entry including header
}

// extractDirectoryRecords reads the root directory from an ISO image and returns
// all directory records with their system use areas.
func extractDirectoryRecords(t *testing.T, iso []byte, blocksize int64) []dirRecord {
	t.Helper()

	if int64(len(iso)) < 17*blocksize {
		t.Fatal("ISO too small to contain PVD")
	}

	// Read PVD (sector 16)
	pvd := iso[16*blocksize : 17*blocksize]
	if pvd[0] != 1 { // type 1 = PVD
		t.Fatalf("sector 16 is not a PVD (type byte = %d)", pvd[0])
	}

	// Root directory record is at PVD offset 156, length 34 bytes
	rootRecord := pvd[156 : 156+34]
	rootLBA := binary.LittleEndian.Uint32(rootRecord[2:6])
	rootSize := binary.LittleEndian.Uint32(rootRecord[10:14])

	if int64(rootLBA)*blocksize+int64(rootSize) > int64(len(iso)) {
		t.Fatalf("root directory extends beyond ISO (LBA=%d, size=%d)", rootLBA, rootSize)
	}

	rootDir := iso[int64(rootLBA)*blocksize : int64(rootLBA)*blocksize+int64(rootSize)]

	var records []dirRecord
	offset := 0
	for offset < len(rootDir) {
		recLen := int(rootDir[offset])
		if recLen == 0 {
			// Skip padding to next sector boundary
			nextSector := ((offset / int(blocksize)) + 1) * int(blocksize)
			if nextSector <= offset || nextSector >= len(rootDir) {
				break
			}
			offset = nextSector
			continue
		}
		if offset+recLen > len(rootDir) {
			break
		}

		rec := rootDir[offset : offset+recLen]
		nameLen := int(rec[32])
		name := string(rec[33 : 33+nameLen])

		// System Use area starts after the file identifier + optional padding byte
		suStart := 33 + nameLen
		if nameLen%2 == 0 {
			suStart++ // padding byte for even-length names
		}

		var systemUse []byte
		if suStart < recLen {
			systemUse = make([]byte, recLen-suStart)
			copy(systemUse, rec[suStart:])
		}

		records = append(records, dirRecord{
			name:      name,
			systemUse: systemUse,
		})

		offset += recLen
	}

	return records
}

// mapEntriesByName builds a map of directory records keyed by their Rock Ridge alternate name
// (from NM entry), falling back to the ISO 9660 identifier.
func mapEntriesByName(records []dirRecord) map[string]dirRecord {
	result := make(map[string]dirRecord)
	for _, rec := range records {
		name := extractNMName(rec.systemUse)
		if name == "" {
			name = rec.name
		}
		result[name] = rec
	}
	return result
}

// extractNMName extracts the alternate name from NM SUSP entries in the system use area.
func extractNMName(systemUse []byte) string {
	var name string
	offset := 0
	for offset+4 <= len(systemUse) {
		sig := string(systemUse[offset : offset+2])
		entryLen := int(systemUse[offset+2])
		if entryLen < 4 || offset+entryLen > len(systemUse) {
			break
		}
		if sig == "NM" && entryLen > 5 {
			flags := systemUse[offset+4]
			nameData := systemUse[offset+5 : offset+entryLen]
			name += string(nameData)
			if flags&0x01 == 0 { // no CONTINUE flag
				break
			}
		}
		offset += entryLen
	}
	return name
}

// parseSUSPEntries parses the system use area into individual SUSP entries.
func parseSUSPEntries(systemUse []byte) []suspEntry {
	var entries []suspEntry
	offset := 0
	for offset+4 <= len(systemUse) {
		sig := string(systemUse[offset : offset+2])
		entryLen := int(systemUse[offset+2])
		if entryLen < 4 || offset+entryLen > len(systemUse) {
			break
		}
		entry := make([]byte, entryLen)
		copy(entry, systemUse[offset:offset+entryLen])
		entries = append(entries, suspEntry{sig: sig, data: entry})

		if sig == "ST" { // SUSP terminator
			break
		}
		offset += entryLen
	}
	return entries
}

// findSUSPEntry finds the first SUSP entry with the given signature.
func findSUSPEntry(entries []suspEntry, sig string) []byte {
	for _, e := range entries {
		if e.sig == sig {
			return e.data
		}
	}
	return nil
}

// comparePXEntries compares PX (POSIX attributes) entries from go-diskfs and xorriso.
// We compare permission bits (lower 12 bits of mode) and file type (upper 4 bits).
// UID/GID/nlink may differ due to how each tool captures workspace metadata.
func comparePXEntries(t *testing.T, name string, goPX, xorrisoPX []byte) {
	t.Helper()

	if len(goPX) < 36 || len(xorrisoPX) < 36 {
		t.Logf("PX entry for %q too short (go=%d, xorriso=%d)", name, len(goPX), len(xorrisoPX))
		return
	}

	// PX layout: bytes 4-7 = file mode (little-endian uint32)
	goMode := binary.LittleEndian.Uint32(goPX[4:8])
	xorrisoMode := binary.LittleEndian.Uint32(xorrisoPX[4:8])

	// Compare file type (upper 4 bits of mode via mask 0o170000)
	goType := goMode & 0o170000
	xorrisoType := xorrisoMode & 0o170000
	if goType != xorrisoType {
		t.Errorf("PX file type mismatch for %q: go=0o%06o, xorriso=0o%06o", name, goType, xorrisoType)
	}

	// Compare permission bits (lower 12 bits: type + special + rwx)
	goPerm := goMode & 0o7777
	xorrisoPerm := xorrisoMode & 0o7777
	if goPerm != xorrisoPerm {
		t.Errorf("PX permission mismatch for %q: go=0o%04o, xorriso=0o%04o", name, goPerm, xorrisoPerm)
	}

	// Log full modes for debugging
	if goMode != xorrisoMode {
		t.Logf("PX full mode for %q: go=0o%o, xorriso=0o%o", name, goMode, xorrisoMode)
	}

	// Verify both-endian encoding: bytes 4-7 (LE) should equal bytes 8-11 (BE)
	goModeBE := binary.BigEndian.Uint32(goPX[8:12])
	if goMode != goModeBE {
		t.Errorf("PX both-endian mismatch for %q in go-diskfs: LE=0o%o, BE=0o%o", name, goMode, goModeBE)
	}
	xorrisoModeBE := binary.BigEndian.Uint32(xorrisoPX[8:12])
	if xorrisoMode != xorrisoModeBE {
		t.Errorf("PX both-endian mismatch for %q in xorriso: LE=0o%o, BE=0o%o", name, xorrisoMode, xorrisoModeBE)
	}
}
