package iso9660_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/diskfs/go-diskfs/backend/file"
	"github.com/diskfs/go-diskfs/filesystem/iso9660"
	"github.com/diskfs/go-diskfs/partition/mbr"
	"github.com/diskfs/go-diskfs/testhelper"
)

// TestCombinedRockRidgeJolietElTorito exercises the "all of the above" case:
// a single ISO finalized with Rock Ridge, Joliet, and El Torito enabled at once.
// No other test covers the three-way combination, so this guards against one
// feature silently clobbering another's volume-descriptor slot, directory
// tree, or boot catalog placement.
//
// Verifies:
//   - Rock Ridge metadata (permissions, symlinks) survives readback.
//   - Joliet SVD is present in the raw volume descriptor sequence.
//   - El Torito boot catalog is detectable by xorriso.
//   - Boot file hide flag respected on readback.
//
// The xorriso-based checks are gated on TEST_IMAGE and only run under `make test`.
//
//nolint:gocyclo // integration test with multiple independent assertions
func TestCombinedRockRidgeJolietElTorito(t *testing.T) {
	blocksize := int64(2048)

	workspace, err := os.MkdirTemp("", "iso_combined_test")
	if err != nil {
		t.Fatalf("Failed to create workspace: %v", err)
	}
	defer os.RemoveAll(workspace)
	if err := os.Chmod(workspace, 0o755); err != nil {
		t.Fatalf("Failed to chmod workspace: %v", err)
	}

	// Rock Ridge: a file with non-default perms.
	rrFile := filepath.Join(workspace, "perm755.txt")
	if err := os.WriteFile(rrFile, []byte("rr perm content"), 0o600); err != nil {
		t.Fatalf("Failed to write rr file: %v", err)
	}
	if err := os.Chmod(rrFile, 0o755); err != nil {
		t.Fatalf("Failed to chmod rr file: %v", err)
	}

	// Rock Ridge: symlink.
	targetFile := filepath.Join(workspace, "target.txt")
	if err := os.WriteFile(targetFile, []byte("target"), 0o600); err != nil {
		t.Fatalf("Failed to write symlink target: %v", err)
	}
	symlinkCreated := true
	if err := os.Symlink("target.txt", filepath.Join(workspace, "link.txt")); err != nil {
		t.Logf("symlink not created (skipping symlink check): %v", err)
		symlinkCreated = false
	}

	// Joliet: a long mixed-case filename with spaces. Rock Ridge also preserves
	// this, but it's the kind of name where a Joliet/RR interaction is most
	// likely to misbehave.
	jolietName := "A Long Joliet Name.txt"
	if err := os.WriteFile(filepath.Join(workspace, jolietName), []byte("joliet"), 0o600); err != nil {
		t.Fatalf("Failed to write joliet file: %v", err)
	}

	// El Torito: two boot images, one to hide, one to leave visible.
	bootData := bytes.Repeat([]byte{0xAA}, 2048)
	if err := os.WriteFile(filepath.Join(workspace, "BOOT1.IMG"), bootData, 0o600); err != nil {
		t.Fatalf("Failed to write BOOT1.IMG: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "BOOT2.IMG"), bootData, 0o600); err != nil {
		t.Fatalf("Failed to write BOOT2.IMG: %v", err)
	}

	// Finalize with all three features enabled.
	f, err := os.CreateTemp("", "iso_combined_out")
	if err != nil {
		t.Fatalf("Failed to create tmpfile: %v", err)
	}
	defer os.Remove(f.Name())

	bk := file.New(f, false)
	fsWrite, err := iso9660.Create(bk, 0, 0, blocksize, workspace)
	if err != nil {
		t.Fatalf("Failed to iso9660.Create: %v", err)
	}
	err = fsWrite.Finalize(iso9660.FinalizeOptions{
		RockRidge: true,
		Joliet:    true,
		ElTorito: &iso9660.ElTorito{
			BootCatalog:     "/BOOT.CAT",
			HideBootCatalog: false,
			Entries: []*iso9660.ElToritoEntry{
				{Platform: iso9660.BIOS, Emulation: iso9660.NoEmulation, BootFile: "/BOOT1.IMG", HideBootFile: true, LoadSegment: 0, SystemType: mbr.Fat32LBA},
				{Platform: iso9660.EFI, Emulation: iso9660.NoEmulation, BootFile: "/BOOT2.IMG", HideBootFile: false, LoadSegment: 0, SystemType: mbr.Fat32LBA},
			},
		},
	})
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Read the ISO back with go-diskfs. With Rock Ridge enabled, the reader
	// should prefer RR names and expose RR metadata.
	fsRead, err := iso9660.Read(bk, 0, 0, blocksize)
	if err != nil {
		t.Fatalf("Failed to read ISO: %v", err)
	}

	entries, err := fsRead.ReadDir(".")
	if err != nil {
		t.Fatalf("Failed to ReadDir root: %v", err)
	}
	entryMap := make(map[string]os.FileInfo)
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			t.Fatalf("Failed to get info for %s: %v", e.Name(), ierr)
		}
		entryMap[e.Name()] = info
	}

	// Rock Ridge: permissions preserved.
	if info, ok := entryMap["perm755.txt"]; !ok {
		t.Error("perm755.txt not found in root listing")
	} else if perm := info.Mode().Perm(); perm != 0o755 {
		t.Errorf("perm755.txt: got perm %o, want %o", perm, 0o755)
	}

	// Rock Ridge: symlink preserved.
	if symlinkCreated {
		info, ok := entryMap["link.txt"]
		if !ok {
			t.Error("link.txt not found in root listing")
		} else {
			if info.Mode()&os.ModeSymlink == 0 {
				t.Errorf("link.txt: expected symlink mode, got %v", info.Mode())
			}
			if rri, ok := info.Sys().(*iso9660.RockRidgeInfo); !ok {
				t.Error("link.txt: Sys() did not return *RockRidgeInfo")
			} else if rri.Symlink != "target.txt" {
				t.Errorf("link.txt: symlink target %q, want %q", rri.Symlink, "target.txt")
			}
		}
	}

	// Joliet-style long name is also visible via the RR tree (RR preserves the
	// same long name). The critical Joliet check is the SVD presence below.
	if _, ok := entryMap[jolietName]; !ok {
		t.Errorf("%q not found in root listing", jolietName)
	}

	// El Torito: hidden boot file should not appear in the RR/primary directory;
	// the visible one should.
	if _, err := fsRead.OpenFile("/BOOT1.IMG", os.O_RDONLY); err == nil {
		t.Error("expected error opening hidden /BOOT1.IMG, got nil")
	}
	if _, err := fsRead.OpenFile("/BOOT2.IMG", os.O_RDONLY); err != nil {
		t.Errorf("error opening /BOOT2.IMG: %v", err)
	}

	// Joliet SVD present in the raw volume descriptor sequence.
	// findSVD() is defined in joliet_xorriso_test.go (same package).
	isoBytes, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("Failed to read ISO bytes: %v", err)
	}
	svd := findSVD(t, isoBytes, blocksize)
	if svd == nil {
		t.Fatal("Joliet SVD not found in ISO produced with RR+Joliet+ElTorito")
	}
	// Joliet Level 3 escape sequence.
	wantEsc := []byte{0x25, 0x2F, 0x45}
	if !bytes.Equal(svd[88:91], wantEsc) {
		t.Errorf("SVD escape sequences: got %x, want %x", svd[88:91], wantEsc)
	}

	// xorriso analysis — verifies an external tool agrees the boot catalog
	// is present and well-formed alongside RR and Joliet.
	if intImage == "" {
		t.Log("skipping xorriso analysis (TEST_IMAGE not set)")
		return
	}
	output := new(bytes.Buffer)
	mpath := "/file.iso"
	mounts := map[string]string{f.Name(): mpath}
	if err := testhelper.DockerRun(nil, output, false, true, mounts, intImage,
		"xorriso", "-indev", mpath, "-report_el_torito", "plain"); err != nil {
		t.Errorf("xorriso -report_el_torito failed: %v\n%s", err, output.String())
	}
	out := output.String()
	if !regexp.MustCompile(`Boot record\s*:\s*El Torito`).MatchString(out) {
		t.Errorf("xorriso did not report El Torito boot record:\n%s", out)
	}
	if !regexp.MustCompile(`El Torito cat path\s*:\s*(\S+)`).MatchString(out) {
		t.Errorf("xorriso did not report El Torito catalog path:\n%s", out)
	}
}
