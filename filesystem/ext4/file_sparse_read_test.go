package ext4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/diskfs/go-diskfs/backend"
	backendfile "github.com/diskfs/go-diskfs/backend/file"
)

const sparseTestBlock = 4096

func sparseReadFixture(t *testing.T, mapping extents, size int) (result *File, bytesOnDisk []byte) {
	t.Helper()
	data := make([]byte, 128*sparseTestBlock)
	for i := range data {
		data[i] = byte((i*17+29)%251 + 1)
	}
	name := filepath.Join(t.TempDir(), "backend.data")
	if err := os.WriteFile(name, data, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	model := &File{inode: &inode{size: uint64(size)}, filesystem: &FileSystem{superblock: &superblock{blockSize: sparseTestBlock}, backend: backendfile.New(f, true)}, extents: mapping, fileType: dirFileTypeRegular}
	return model, data
}

func sparseTestRead(t *testing.T, f *File, b []byte) (n int, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("reader panicked on a valid or bounded diagnostic input: %v", r)
		}
	}()
	return f.Read(b)
}

func TestSparseReadContract(t *testing.T) {
	cases := []struct {
		name     string
		mapping  extents
		size     int
		expected func([]byte) []byte
	}{
		{"dense", extents{{fileBlock: 0, startingBlock: 3, count: 2}}, 2 * sparseTestBlock, func(d []byte) []byte { return bytes.Clone(d[3*sparseTestBlock : 5*sparseTestBlock]) }},
		{"fragmented-no-hole", extents{{fileBlock: 0, startingBlock: 3, count: 1}, {fileBlock: 1, startingBlock: 8, count: 1}}, 2 * sparseTestBlock, func(d []byte) []byte {
			return append(bytes.Clone(d[3*sparseTestBlock:4*sparseTestBlock]), d[8*sparseTestBlock:9*sparseTestBlock]...)
		}},
		{"leading-hole", extents{{fileBlock: 2, startingBlock: 9, count: 1}}, 3 * sparseTestBlock, func(d []byte) []byte {
			return append(make([]byte, 2*sparseTestBlock), d[9*sparseTestBlock:10*sparseTestBlock]...)
		}},
		{"middle-hole", extents{{fileBlock: 0, startingBlock: 3, count: 1}, {fileBlock: 3, startingBlock: 8, count: 1}}, 4 * sparseTestBlock, func(d []byte) []byte {
			return append(append(bytes.Clone(d[3*sparseTestBlock:4*sparseTestBlock]), make([]byte, 2*sparseTestBlock)...), d[8*sparseTestBlock:9*sparseTestBlock]...)
		}},
		{"trailing-hole-partial-eof", extents{{fileBlock: 0, startingBlock: 3, count: 1}}, 3*sparseTestBlock + 17, func(d []byte) []byte {
			return append(bytes.Clone(d[3*sparseTestBlock:4*sparseTestBlock]), make([]byte, 2*sparseTestBlock+17)...)
		}},
		{"all-hole", nil, 2*sparseTestBlock + 3, func([]byte) []byte { return make([]byte, 2*sparseTestBlock+3) }},
		{"unwritten-not-physical-data", extents{{fileBlock: 0, startingBlock: 3, count: 32769}}, sparseTestBlock, func([]byte) []byte { return make([]byte, sparseTestBlock) }},
		{"initialized-32768-boundary", extents{{fileBlock: 0, startingBlock: 3, count: 32768}}, sparseTestBlock, func(d []byte) []byte { return bytes.Clone(d[3*sparseTestBlock : 4*sparseTestBlock]) }},
		{"real-libseccomp-layout", extents{{fileBlock: 0, startingBlock: 3, count: 42}, {fileBlock: 47, startingBlock: 45, count: 2}}, 199128, func(d []byte) []byte {
			want := make([]byte, 199128)
			copy(want[:42*sparseTestBlock], d[3*sparseTestBlock:45*sparseTestBlock])
			copy(want[47*sparseTestBlock:], d[45*sparseTestBlock:])
			return want
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, data := sparseReadFixture(t, tc.mapping, tc.size)
			want := tc.expected(data)
			if len(want) != tc.size {
				t.Fatal("invalid test oracle length")
			}
			for _, chunk := range []int{1, 7, 511, 4096, 4097, tc.size + 17} {
				if _, err := f.Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				var got []byte
				for len(got) < tc.size {
					b := bytes.Repeat([]byte{0xdd}, chunk)
					n, err := sparseTestRead(t, f, b)
					if n <= 0 || n > len(b) {
						t.Fatalf("no progress or invalid count: n=%d err=%v", n, err)
					}
					got = append(got, b[:n]...)
					if err != nil {
						if err != io.EOF || len(got) != tc.size {
							t.Fatalf("early read error: n=%d err=%v", n, err)
						}
						break
					}
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("sparse read content mismatch: chunk=%d", chunk)
				}
				if n, err := f.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Fatalf("EOF: n=%d err=%v", n, err)
				}
				if n, err := f.Read(nil); n != 0 || err != io.EOF {
					t.Fatalf("zero read at EOF: n=%d err=%v", n, err)
				}
			}
			for _, offset := range []int64{0, 1, 4095, 4096, 8191, 8192, 172031, 172032, 192511, 192512, int64(tc.size) - 1, int64(tc.size)} {
				if offset < 0 || offset > int64(tc.size) {
					continue
				}
				if _, err := f.Seek(offset, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				b := bytes.Repeat([]byte{0xcc}, 4097)
				n, err := sparseTestRead(t, f, b)
				end := min(tc.size, int(offset)+len(b))
				if n != end-int(offset) || !bytes.Equal(b[:n], want[offset:int64(end)]) {
					t.Fatalf("seeked content mismatch at %d: n=%d err=%v", offset, n, err)
				}
				if end == tc.size && err != io.EOF || end < tc.size && err != nil {
					t.Fatalf("seeked EOF mismatch: %v", err)
				}
			}
		})
	}
}

type sparseFaultStorage struct {
	backend.Storage
	count int
	fault error
	calls int
}

func (s *sparseFaultStorage) ReadAt(b []byte, off int64) (int, error) {
	s.calls++
	n := s.count
	if n >= 0 && n <= len(b) {
		if _, err := s.Storage.ReadAt(b[:n], off); err != nil {
			return 0, err
		}
	}
	return n, s.fault
}

func TestSparseReadBackendFailures(t *testing.T) {
	marker := errors.New("backend-fault")
	for _, tc := range []struct {
		name             string
		n                int
		fault, errorWant error
	}{
		{"short-nil", 3, nil, io.ErrUnexpectedEOF},
		{"short-eof", 3, io.EOF, io.ErrUnexpectedEOF},
		{"short-other", 3, marker, marker},
		{"short-joined-eof", 3, errors.Join(io.EOF, marker), marker},
		{"negative-count-with-error", -1, marker, marker},
		{"oversized-count-with-error", sparseTestBlock + 1, marker, marker},
		{"full-count-with-joined-error", sparseTestBlock, errors.Join(io.EOF, marker), marker},
		{"negative-count", -1, nil, nil},
		{"oversized-count", sparseTestBlock + 1, nil, nil},
		{"full-count-with-error", sparseTestBlock, marker, marker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, data := sparseReadFixture(t, extents{{fileBlock: 2, startingBlock: 3, count: 1}}, 3*sparseTestBlock)
			injected := &sparseFaultStorage{Storage: f.filesystem.backend, count: tc.n, fault: tc.fault}
			f.filesystem.backend = injected
			b := bytes.Repeat([]byte{0xaa}, 3*sparseTestBlock)
			n, err := sparseTestRead(t, f, b)
			wantN := 2 * sparseTestBlock
			if tc.n >= 0 && tc.n <= sparseTestBlock {
				wantN += tc.n
			}
			if n != wantN || f.offset != int64(wantN) || err == nil {
				t.Fatalf("invalid prefix/error/offset: n=%d offset=%d error=%v", n, f.offset, err)
			}
			if tc.errorWant != nil && !errors.Is(err, tc.errorWant) {
				t.Fatalf("backend error lost: %v", err)
			}
			if !bytes.Equal(b[:2*sparseTestBlock], make([]byte, 2*sparseTestBlock)) {
				t.Fatal("preceding hole not zero")
			}
			if tc.n > 0 && tc.n <= sparseTestBlock && !bytes.Equal(b[2*sparseTestBlock:wantN], data[3*sparseTestBlock:3*sparseTestBlock+tc.n]) {
				t.Fatal("short-read prefix differs")
			}
			if injected.calls != 1 {
				t.Fatalf("backend called %d times", injected.calls)
			}
		})
	}
}

func TestSparseReadInvalidMapping(t *testing.T) {
	for name, mapping := range map[string]extents{
		"zero-length":       {{fileBlock: 0, startingBlock: 3, count: 0}},
		"overlap":           {{fileBlock: 0, startingBlock: 3, count: 2}, {fileBlock: 1, startingBlock: 8, count: 1}},
		"unsorted":          {{fileBlock: 2, startingBlock: 3, count: 1}, {fileBlock: 0, startingBlock: 8, count: 1}},
		"physical-overflow": {{fileBlock: 0, startingBlock: ^uint64(0), count: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := sparseReadFixture(t, mapping, 4*sparseTestBlock)
			if n, err := sparseTestRead(t, f, make([]byte, sparseTestBlock)); n != 0 || err == nil || f.offset != 0 {
				t.Fatalf("invalid mapping was admitted: n=%d err=%v offset=%d", n, err, f.offset)
			}
		})
	}
}

func TestSparseReadDoesNotFetchHoles(t *testing.T) {
	f, _ := sparseReadFixture(t, extents{{fileBlock: 0, startingBlock: 3, count: 32770}}, 2*sparseTestBlock)
	injected := &sparseFaultStorage{Storage: f.filesystem.backend, count: 0, fault: fmt.Errorf("must-not-read")}
	f.filesystem.backend = injected
	b := bytes.Repeat([]byte{0xff}, 2*sparseTestBlock)
	if n, err := sparseTestRead(t, f, b); n != len(b) || err != io.EOF || injected.calls != 0 || !bytes.Equal(b, make([]byte, len(b))) {
		t.Fatalf("unwritten extent fetched or not cleared: n=%d err=%v calls=%d", n, err, injected.calls)
	}
}

func TestSparseReadFailureAfterInitializedData(t *testing.T) {
	f, data := sparseReadFixture(t, extents{{fileBlock: 0, startingBlock: 3, count: 1}, {fileBlock: 2, startingBlock: 8, count: 1}}, 3*sparseTestBlock)
	fault := &sparseSecondReadFault{Storage: f.filesystem.backend}
	f.filesystem.backend = fault
	b := bytes.Repeat([]byte{0xaa}, 3*sparseTestBlock)
	n, err := sparseTestRead(t, f, b)
	wantN := 2*sparseTestBlock + 3
	if n != wantN || f.offset != int64(wantN) || !errors.Is(err, io.ErrUnexpectedEOF) || fault.calls != 2 {
		t.Fatalf("prior data prefix/offset/error lost: n=%d offset=%d err=%v calls=%d", n, f.offset, err, fault.calls)
	}
	if !bytes.Equal(b[:sparseTestBlock], data[3*sparseTestBlock:4*sparseTestBlock]) || !bytes.Equal(b[sparseTestBlock:2*sparseTestBlock], make([]byte, sparseTestBlock)) || !bytes.Equal(b[2*sparseTestBlock:n], data[8*sparseTestBlock:8*sparseTestBlock+3]) {
		t.Fatal("prior data, hole or short suffix differs")
	}
}

type sparseSecondReadFault struct {
	backend.Storage
	calls int
}

func (s *sparseSecondReadFault) ReadAt(b []byte, off int64) (int, error) {
	s.calls++
	if s.calls == 1 {
		return s.Storage.ReadAt(b, off)
	}
	n, err := s.Storage.ReadAt(b[:3], off)
	if err != nil {
		return n, err
	}
	return n, io.EOF
}

func TestSparseReadAcceptsEOFWithFullRead(t *testing.T) {
	f, data := sparseReadFixture(t, extents{{fileBlock: 2, startingBlock: 3, count: 1}}, 4*sparseTestBlock)
	injected := &sparseFaultStorage{Storage: f.filesystem.backend, count: sparseTestBlock, fault: io.EOF}
	f.filesystem.backend = injected
	b := bytes.Repeat([]byte{0xaa}, 3*sparseTestBlock)
	n, err := sparseTestRead(t, f, b)
	if n != len(b) || err != nil || f.offset != int64(len(b)) || injected.calls != 1 {
		t.Fatalf("complete read reported with io.EOF was rejected: n=%d err=%v offset=%d calls=%d", n, err, f.offset, injected.calls)
	}
	if !bytes.Equal(b[:2*sparseTestBlock], make([]byte, 2*sparseTestBlock)) || !bytes.Equal(b[2*sparseTestBlock:], data[3*sparseTestBlock:4*sparseTestBlock]) {
		t.Fatal("hole or extent content differs")
	}
}

const (
	mkfsSparseBlock     = 4096
	mkfsSparseImageSize = 16 * 1024 * 1024
)

// mkfsSparseFixture builds an ext4 image with mkfs.ext4 -d from a sparse source file and
// adds an unwritten extent with debugfs. Logical layout in blocks: [0,2) data, [2,5) hole,
// [5,6) data, [6,8) unwritten, [8,9) hole; the size ends mid-block. It skips the test when
// the e2fsprogs tools are not installed.
func mkfsSparseFixture(t *testing.T) (image string, want []byte) {
	t.Helper()
	for _, tool := range []string{"mkfs.ext4", "debugfs", "e2fsck"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("skipping real-image sparse test: %v", err)
		}
	}
	want = make([]byte, 9*mkfsSparseBlock-100)
	for i := 0; i < 2*mkfsSparseBlock; i++ {
		want[i] = byte(i*7 + 1) // no all-zero block, or mke2fs would turn it into a hole
	}
	for i := 5 * mkfsSparseBlock; i < 6*mkfsSparseBlock; i++ {
		want[i] = byte(i*13 + 3)
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "root")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatal(err)
	}
	sf, err := os.Create(filepath.Join(source, "sparse.dat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []struct{ start, end int }{{0, 2 * mkfsSparseBlock}, {5 * mkfsSparseBlock, 6 * mkfsSparseBlock}} {
		if _, err := sf.WriteAt(want[chunk.start:chunk.end], int64(chunk.start)); err != nil {
			t.Fatal(err)
		}
	}
	if err := sf.Truncate(int64(len(want))); err != nil {
		t.Fatal(err)
	}
	if err := sf.Close(); err != nil {
		t.Fatal(err)
	}
	image = filepath.Join(dir, "sparse.img")
	if err := os.WriteFile(image, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(image, mkfsSparseImageSize); err != nil {
		t.Fatal(err)
	}
	runE2fsTool(t, exec.Command("mkfs.ext4", "-q", "-F", "-b", "4096", "-d", source, image))
	runE2fsTool(t, exec.Command("debugfs", "-w", "-R", "fallocate /sparse.dat 6 7", image))
	runE2fsTool(t, exec.Command("e2fsck", "-f", "-n", image))
	return image, want
}

func runE2fsTool(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %v\n%s", cmd.Args, err, out)
	}
}

// garbleUnwrittenExtents overwrites the disk blocks of every unwritten extent with garbage,
// so that only a reader that skips them returns zeros there, and reports how many holes and
// unwritten extents the mapping has.
func garbleUnwrittenExtents(t *testing.T, image *os.File, mapping extents) (holes, unwritten int) {
	t.Helper()
	var previousEnd uint64
	for _, e := range mapping {
		if uint64(e.fileBlock) > previousEnd {
			holes++
		}
		count := uint64(e.count)
		if count > 32768 {
			count -= 32768
			unwritten++
			if _, err := image.WriteAt(bytes.Repeat([]byte{0x5a}, int(count)*mkfsSparseBlock), int64(e.startingBlock)*mkfsSparseBlock); err != nil {
				t.Fatal(err)
			}
		}
		previousEnd = uint64(e.fileBlock) + count
	}
	return holes, unwritten
}

// TestSparseReadFromMkfsImage reads a real sparse file through the extent parser and Read.
func TestSparseReadFromMkfsImage(t *testing.T) {
	image, want := mkfsSparseFixture(t)
	f, err := os.OpenFile(image, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	fs, err := Read(backendfile.New(f, true), mkfsSparseImageSize, 0, 512)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := fs.OpenFile("/sparse.dat", os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	file, ok := opened.(*File)
	if !ok {
		t.Fatalf("unexpected file type %T", opened)
	}
	// the fixture must really contain a hole and an unwritten extent, or this test proves nothing
	if holes, unwritten := garbleUnwrittenExtents(t, f, file.extents); holes == 0 || unwritten == 0 {
		t.Fatalf("fixture is not sparse: extents=%+v holes=%d unwritten=%d", file.extents, holes, unwritten)
	}
	got, err := io.ReadAll(opened)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("sparse file content mismatch: len=%d want=%d extents=%+v", len(got), len(want), file.extents)
	}
	// a read that starts inside the hole, crosses the data block and ends inside the unwritten extent
	if _, err := opened.Seek(3*mkfsSparseBlock+5, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b := bytes.Repeat([]byte{0xcc}, 3*mkfsSparseBlock)
	if n, err := opened.Read(b); n != len(b) || err != nil || !bytes.Equal(b, want[3*mkfsSparseBlock+5:6*mkfsSparseBlock+5]) {
		t.Fatalf("read across hole/data/unwritten boundaries: n=%d err=%v", n, err)
	}
}
