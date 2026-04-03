package iso9660

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestRockRidgeID(t *testing.T) {
	id := "abc"
	rr := &rockRidgeExtension{id: id}
	if rr.ID() != id {
		t.Errorf("Mismatched signature, actual '%s' expected '%s'", rr.ID(), id)
	}
}

func TestRockRidgeGetFilename(t *testing.T) {
	tests := []struct {
		dirEntry *directoryEntry
		filename string
		err      error
	}{
		{&directoryEntry{filename: "ABC"}, "", fmt.Errorf("could not find Rock Ridge filename property")},
		{&directoryEntry{filename: "ABC", extensions: []directoryEntrySystemUseExtension{rockRidgeName{name: "abc"}}}, "abc", nil},
	}
	rr := &rockRidgeExtension{}
	for _, tt := range tests {
		name, err := rr.GetFilename(tt.dirEntry)
		if (err != nil && tt.err == nil) || (err == nil && tt.err != nil) {
			t.Errorf("Mismatched errors, actual then expected")
			t.Log(err)
			t.Log(tt.err)
		} else if name != tt.filename {
			t.Errorf("Mismatched filename actual %s expected %s", name, tt.filename)
		}
	}
}

func TestRockRidgeRelocated(t *testing.T) {
	tests := []struct {
		dirEntry  *directoryEntry
		relocated bool
	}{
		{&directoryEntry{filename: "ABC"}, false},
		{&directoryEntry{filename: "ABC", extensions: []directoryEntrySystemUseExtension{rockRidgeRelocatedDirectory{}}}, true},
	}
	rr := &rockRidgeExtension{}
	for _, tt := range tests {
		reloc := rr.Relocated(tt.dirEntry)
		if reloc != tt.relocated {
			t.Errorf("Mismatched relocated actual %v expected %v", reloc, tt.relocated)
		}
	}
}

func TestRockRidgeUsePathtable(t *testing.T) {
	rr := &rockRidgeExtension{}
	if rr.UsePathtable() {
		t.Errorf("Rock Ridge extension erroneously said to use pathtable")
	}
}

func TestRockRidgeRelocatedDirectoryRoundTrip(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	re := rockRidgeRelocatedDirectory{}
	b := re.Bytes()
	if len(b) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(b))
	}
	parsed, err := rr.parseRelocatedDirectory(b)
	if err != nil {
		t.Fatalf("unexpected error parsing RE: %v", err)
	}
	if _, ok := parsed.(rockRidgeRelocatedDirectory); !ok {
		t.Fatalf("parsed entry is not rockRidgeRelocatedDirectory")
	}
}

func TestRockRidgeSymlinkRoundTrip(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	tests := []struct {
		name   string
		target string
	}{
		{"absolute path", "/a/b/c"},
		{"relative dotdot", "../foo"},
		{"relative dot", "./bar"},
		{"root only", "/"},
		{"double dotdot", "../../foo"},
		{"triple dotdot", "../../../bar"},
		{"dotdot only", "../.."},
		{"dot then dotdot", "./../foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := rockRidgeSymlink{name: tt.target}
			b := sl.Bytes()
			// parse all SL records from the bytes
			reconstructed := ""
			for i := 0; i < len(b); {
				recLen := int(b[i+2])
				rec := b[i : i+recLen]
				parsed, err := rr.parseSymlink(rec)
				if err != nil {
					t.Fatalf("unexpected error parsing SL record: %v", err)
				}
				psl, ok := parsed.(rockRidgeSymlink)
				if !ok {
					t.Fatalf("parsed entry is not rockRidgeSymlink")
				}
				reconstructed += psl.name
				i += recLen
			}
			if reconstructed != tt.target {
				t.Errorf("symlink round-trip failed: got %q, want %q", reconstructed, tt.target)
			}
		})
	}
}

func TestRockRidgePosixAttributesRoundTrip(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	original := rockRidgePosixAttributes{
		mode:      os.ModeDir | 0o755,
		linkCount: 3,
		uid:       1000,
		gid:       1000,
		length:    rr.pxLength,
		serial:    42,
	}
	b := original.Bytes()
	parsed, err := rr.parsePosixAttributes(b)
	if err != nil {
		t.Fatalf("unexpected error parsing PX: %v", err)
	}
	px, ok := parsed.(rockRidgePosixAttributes)
	if !ok {
		t.Fatalf("parsed entry is not rockRidgePosixAttributes")
	}
	if px.serial != original.serial {
		t.Errorf("serial mismatch: got %d, want %d", px.serial, original.serial)
	}
}

func TestRockRidgePosixAttributesFileTypes(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	tests := []struct {
		name string
		mode os.FileMode
	}{
		{"regular file", 0o644},
		{"directory", os.ModeDir | 0o755},
		{"symlink", os.ModeSymlink | 0o777},
		{"named pipe", os.ModeNamedPipe | 0o644},
		{"socket", os.ModeSocket | 0o755},
		{"char device", os.ModeCharDevice | os.ModeDevice | 0o660},
		{"block device", os.ModeDevice | 0o660},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := rockRidgePosixAttributes{
				mode:      tt.mode,
				linkCount: 1,
				uid:       0,
				gid:       0,
				length:    rr.pxLength,
			}
			b := original.Bytes()
			parsed, err := rr.parsePosixAttributes(b)
			if err != nil {
				t.Fatalf("unexpected error parsing PX: %v", err)
			}
			px, ok := parsed.(rockRidgePosixAttributes)
			if !ok {
				t.Fatalf("parsed entry is not rockRidgePosixAttributes")
			}
			if px.mode != original.mode {
				t.Errorf("mode mismatch: got %v (%o), want %v (%o)", px.mode, px.mode, original.mode, original.mode)
			}
		})
	}
}

func TestRockRidgePosixAttributesSpecialBits(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	tests := []struct {
		name string
		mode os.FileMode
	}{
		{"setuid", os.ModeSetuid | 0o755},
		{"setgid", os.ModeSetgid | 0o755},
		{"sticky", os.ModeSticky | 0o755},
		{"setuid+setgid", os.ModeSetuid | os.ModeSetgid | 0o755},
		{"all special", os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o755},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := rockRidgePosixAttributes{
				mode:      tt.mode,
				linkCount: 1,
				uid:       0,
				gid:       0,
				length:    rr.pxLength,
			}
			b := original.Bytes()
			parsed, err := rr.parsePosixAttributes(b)
			if err != nil {
				t.Fatalf("unexpected error parsing PX: %v", err)
			}
			px, ok := parsed.(rockRidgePosixAttributes)
			if !ok {
				t.Fatalf("parsed entry is not rockRidgePosixAttributes")
			}
			if px.mode != original.mode {
				t.Errorf("mode mismatch: got %v (%o), want %v (%o)", px.mode, px.mode, original.mode, original.mode)
			}
		})
	}
}

func TestRockRidgePosixAttributesRRIP110(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge110)
	original := rockRidgePosixAttributes{
		mode:      os.ModeDir | 0o755,
		linkCount: 3,
		uid:       500,
		gid:       500,
		length:    rr.pxLength, // 36 bytes
	}
	b := original.Bytes()
	if len(b) != 36 {
		t.Fatalf("expected 36 bytes for RRIP 1.10 PX, got %d", len(b))
	}
	parsed, err := rr.parsePosixAttributes(b)
	if err != nil {
		t.Fatalf("unexpected error parsing PX: %v", err)
	}
	px, ok := parsed.(rockRidgePosixAttributes)
	if !ok {
		t.Fatalf("parsed entry is not rockRidgePosixAttributes")
	}
	if px.mode != original.mode {
		t.Errorf("mode mismatch: got %v, want %v", px.mode, original.mode)
	}
	if px.linkCount != original.linkCount {
		t.Errorf("linkCount mismatch: got %d, want %d", px.linkCount, original.linkCount)
	}
	if px.uid != original.uid {
		t.Errorf("uid mismatch: got %d, want %d", px.uid, original.uid)
	}
	if px.gid != original.gid {
		t.Errorf("gid mismatch: got %d, want %d", px.gid, original.gid)
	}
	// serial should be zero for 1.10 (no serial field)
	if px.serial != 0 {
		t.Errorf("serial should be 0 for RRIP 1.10, got %d", px.serial)
	}
}

func TestRockRidgeNameRoundTrip(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	tests := []struct {
		name     string
		filename string
	}{
		{"short name", "hello.txt"},
		{"exactly 249 bytes", string(make([]byte, 249))},
		{"over 249 bytes needs continuation", ""},
	}
	// fill the 249-byte name with valid chars
	name249 := make([]byte, 249)
	for i := range name249 {
		name249[i] = byte('a' + (i % 26))
	}
	tests[1].filename = string(name249)

	// fill a 500-byte name
	name500 := make([]byte, 500)
	for i := range name500 {
		name500[i] = byte('A' + (i % 26))
	}
	tests[2].filename = string(name500[:500])

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nm := rockRidgeName{name: tt.filename}
			b := nm.Bytes()
			// parse all NM records and merge
			reconstructed := ""
			for i := 0; i < len(b); {
				recLen := int(b[i+2])
				rec := b[i : i+recLen]
				parsed, err := rr.parseName(rec)
				if err != nil {
					t.Fatalf("unexpected error parsing NM record: %v", err)
				}
				pnm, ok := parsed.(rockRidgeName)
				if !ok {
					t.Fatalf("parsed entry is not rockRidgeName")
				}
				reconstructed += pnm.name
				i += recLen
			}
			if reconstructed != tt.filename {
				t.Errorf("name round-trip failed: got len %d, want len %d", len(reconstructed), len(tt.filename))
			}
		})
	}
}

func TestRockRidgeTimestampsRoundTrip(t *testing.T) {
	rr := getRockRidgeExtension(rockRidge112)
	now := time.Now()

	tests := []struct {
		name     string
		longForm bool
		stamps   []rockRidgeTimestamp
	}{
		{
			"short form modify+access+attribute",
			false,
			[]rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
		},
		{
			"short form with creation",
			false,
			[]rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampCreation, time: now},
				{timestampType: rockRidgeTimestampModify, time: now},
			},
		},
		{
			"long form modify+access",
			true,
			[]rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := rockRidgeTimestamps{
				longForm: tt.longForm,
				stamps:   tt.stamps,
			}
			b := original.Bytes()
			parsed, err := rr.parseTimestamps(b)
			if err != nil {
				t.Fatalf("unexpected error parsing TF: %v", err)
			}
			tf, ok := parsed.(rockRidgeTimestamps)
			if !ok {
				t.Fatalf("parsed entry is not rockRidgeTimestamps")
			}
			if tf.longForm != tt.longForm {
				t.Errorf("longForm mismatch: got %v, want %v", tf.longForm, tt.longForm)
			}
			if len(tf.stamps) != len(tt.stamps) {
				t.Fatalf("stamps count mismatch: got %d, want %d", len(tf.stamps), len(tt.stamps))
			}
			for i, s := range tf.stamps {
				if s.timestampType != tt.stamps[i].timestampType {
					t.Errorf("stamp %d type mismatch: got %d, want %d", i, s.timestampType, tt.stamps[i].timestampType)
				}
				// short form only has second precision
				if !tt.longForm {
					if s.time.Unix() != tt.stamps[i].time.Unix() {
						t.Errorf("stamp %d time mismatch: got %v, want %v", i, s.time, tt.stamps[i].time)
					}
				}
			}
		})
	}
}

func TestRockRidgeSymlinkMerge(t *testing.T) {
	tests := []struct {
		first        rockRidgeSymlink
		continuation []directoryEntrySystemUseExtension
		result       rockRidgeSymlink
	}{
		{rockRidgeSymlink{name: "/a/b", continued: true}, []directoryEntrySystemUseExtension{rockRidgeSymlink{name: "/c/d", continued: true}, rockRidgeSymlink{name: "/e/f", continued: false}}, rockRidgeSymlink{name: "/a/b/c/d/e/f", continued: false}},
		{rockRidgeSymlink{name: "/a/b", continued: true}, []directoryEntrySystemUseExtension{rockRidgeSymlink{name: "/c/d", continued: false}}, rockRidgeSymlink{name: "/a/b/c/d", continued: false}},
		{rockRidgeSymlink{name: "/a/b", continued: false}, nil, rockRidgeSymlink{name: "/a/b", continued: false}},
	}
	for _, tt := range tests {
		symlink := tt.first.Merge(tt.continuation)
		if symlink != tt.result {
			t.Errorf("Mismatched merge result actual %v expected %v", symlink, tt.result)
		}
	}
}

func TestRockRidgeNameMerge(t *testing.T) {
	tests := []struct {
		first        rockRidgeName
		continuation []directoryEntrySystemUseExtension
		result       rockRidgeName
	}{
		{rockRidgeName{name: "/a/b", continued: true}, []directoryEntrySystemUseExtension{rockRidgeName{name: "/c/d", continued: true}, rockRidgeName{name: "/e/f", continued: false}}, rockRidgeName{name: "/a/b/c/d/e/f", continued: false}},
		{rockRidgeName{name: "/a/b", continued: true}, []directoryEntrySystemUseExtension{rockRidgeName{name: "/c/d", continued: false}}, rockRidgeName{name: "/a/b/c/d", continued: false}},
		{rockRidgeName{name: "/a/b", continued: false}, nil, rockRidgeName{name: "/a/b", continued: false}},
	}
	for _, tt := range tests {
		name := tt.first.Merge(tt.continuation)
		if name != tt.result {
			t.Errorf("Mismatched merge result actual %v expected %v", name, tt.result)
		}
	}
}

func TestRockRidgeSortTimestamp(t *testing.T) {
	// these are ust sorted randomly
	tests := []rockRidgeTimestamp{
		{timestampType: rockRidgeTimestampExpiration},
		{timestampType: rockRidgeTimestampModify},
		{timestampType: rockRidgeTimestampEffective},
		{timestampType: rockRidgeTimestampAttribute},
		{timestampType: rockRidgeTimestampCreation},
		{timestampType: rockRidgeTimestampAccess},
		{timestampType: rockRidgeTimestampBackup},
	}
	expected := []uint8{rockRidgeTimestampCreation, rockRidgeTimestampModify, rockRidgeTimestampAccess,
		rockRidgeTimestampAttribute, rockRidgeTimestampBackup, rockRidgeTimestampExpiration, rockRidgeTimestampEffective}
	sort.Sort(rockRidgeTimestampByBitOrder(tests))
	for i, e := range tests {
		if e.timestampType != expected[i] {
			t.Errorf("At position %d, got %v instead of %v", i, e.timestampType, expected[i])
		}
	}
}

func TestGetExtensions(t *testing.T) {
	// create an extension object and test files
	rr := getRockRidgeExtension(rockRidge112)
	pxLength := rr.pxLength
	dir, err := os.MkdirTemp("", "rockridge")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	self, err := user.Current()
	if err != nil {
		t.Fatalf("unable to get current uid/gid: %v", err)
	}
	uidI, err := strconv.Atoi(self.Uid)
	if err != nil {
		t.Fatalf("unable to convert uid to int: %v", err)
	}
	gidI, err := strconv.Atoi(self.Gid)
	if err != nil {
		t.Fatalf("unable to convert gid to int: %v", err)
	}
	uid := uint32(uidI)
	gid := uint32(gidI)
	now := time.Now()

	// symlinks have fixed perms based on OS, so we just create a random symlink somewhere to get the OS-specific perms
	linkfile := path.Join(dir, "testb")
	if err = os.Symlink("testa", linkfile); err != nil {
		t.Fatalf("unable to create test symlink %s: %v", "testb", err)
	}
	defer os.Remove("testb")
	fi, err := os.Lstat(linkfile)
	if err != nil {
		t.Fatalf("unable to ready file info for test symlink: %v", err)
	}
	symMode := fi.Mode() & 0o777

	tests := []struct {
		name       string
		self       bool
		parent     bool
		extensions []directoryEntrySystemUseExtension
		createFile func(string)
	}{
		// regular file
		{"regular01", false, false, []directoryEntrySystemUseExtension{
			rockRidgePosixAttributes{mode: 0o764, linkCount: 1, uid: uid, gid: gid, length: pxLength},
			rockRidgeTimestamps{stamps: []rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
			},
			rockRidgeName{name: "regular01"},
		}, func(p string) {
			content := []byte("some data")
			if err := os.WriteFile(p, content, 0o600); err != nil {
				t.Fatalf("unable to create regular file %s: %v", p, err)
			}
			// because of umask, must set explicitly
			if err := os.Chmod(p, 0o764); err != nil {
				t.Fatalf("unable to chmod %s: %v", p, err)
			}
		},
		},
		// directory
		{"directory02", false, false, []directoryEntrySystemUseExtension{
			rockRidgePosixAttributes{mode: 0o754 | os.ModeDir, linkCount: 2, uid: uid, gid: gid, length: pxLength},
			rockRidgeTimestamps{stamps: []rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
			},
			rockRidgeName{name: "directory02"},
		}, func(p string) {
			if err := os.Mkdir(p, 0o754); err != nil {
				t.Fatalf("unable to create directory %s: %v", p, err)
			}
			// because of umask, must set explicitly
			if err := os.Chmod(p, 0o754); err != nil {
				t.Fatalf("unable to chmod %s: %v", p, err)
			}
		},
		},
		// symlink
		{"symlink03", false, false, []directoryEntrySystemUseExtension{
			rockRidgePosixAttributes{mode: symMode | os.ModeSymlink, linkCount: 1, uid: uid, gid: gid, length: pxLength},
			rockRidgeTimestamps{stamps: []rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
			},
			rockRidgeName{name: "symlink03"},
			rockRidgeSymlink{continued: false, name: "/a/b/c/d/efgh"},
		}, func(p string) {
			target := "/a/b/c/d/efgh"
			if err := os.Symlink(target, p); err != nil {
				t.Fatalf("unable to create symlink %s: %v", p, err)
			}
		},
		},
		// parent
		{"directoryparent", false, true, []directoryEntrySystemUseExtension{
			rockRidgePosixAttributes{mode: 0o754 | os.ModeDir, linkCount: 2, uid: uid, gid: gid, length: pxLength},
			rockRidgeTimestamps{stamps: []rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
			},
		}, func(p string) {
			if err := os.Mkdir(p, 0o754); err != nil {
				t.Fatalf("unable to create parent directory %s: %v", p, err)
			}
		},
		},
		// self
		{"directoryself", true, false, []directoryEntrySystemUseExtension{
			rockRidgePosixAttributes{mode: 0o754 | os.ModeDir, linkCount: 2, uid: uid, gid: gid, length: pxLength},
			rockRidgeTimestamps{stamps: []rockRidgeTimestamp{
				{timestampType: rockRidgeTimestampModify, time: now},
				{timestampType: rockRidgeTimestampAccess, time: now},
				{timestampType: rockRidgeTimestampAttribute, time: now},
			},
			},
		}, func(p string) {
			if err := os.Mkdir(p, 0o754); err != nil {
				t.Fatalf("unable to create self directory %s: %v", p, err)
			}
		},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// random filename
			fp := filepath.Join(dir, tt.name)
			// create the file
			tt.createFile(fp)
			fi, err := os.Lstat(fp)
			if err != nil {
				t.Fatalf("unable to os.Stat(%s): %v", fp, err)
			}
			ffi, err := finalizeFileInfoFromFile(fp, fp, fi)
			if err != nil {
				t.Fatalf("unable to create finalizeFileInfo from file %s: %v", fp, err)
			}

			// get the extensions
			ext, err := rr.GetFileExtensions(ffi, tt.self, tt.parent)
			if err != nil {
				t.Fatalf("%s: Unexpected error getting extensions for %s: %v", tt.name, fp, err)
			}
			if len(ext) != len(tt.extensions) {
				t.Fatalf("%s: rock ridge extensions gave %d extensions instead of expected %d", tt.name, len(ext), len(tt.extensions))
			}
			// loop through each attribute
			for i, e := range ext {
				if stamp, ok := e.(rockRidgeTimestamps); ok {
					if !stamp.Close(tt.extensions[i]) {
						t.Errorf("%s: Mismatched extension number %d for %s, actual then expected", tt.name, i, fp)
						t.Logf("%#v\n", e)
						t.Logf("%#v\n", tt.extensions[i])
					}
				} else if !e.Equal(tt.extensions[i]) {
					t.Errorf("%s: Mismatched extension number %d for %s, actual then expected", tt.name, i, fp)
					t.Logf("%#v\n", e)
					t.Logf("%#v\n", tt.extensions[i])
				}
			}
		})
	}
}
