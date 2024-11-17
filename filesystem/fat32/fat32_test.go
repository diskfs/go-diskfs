package fat32_test

/*
 These tests the exported functions
 We want to do full-in tests with files
*/

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	mathrandv2 "math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diskfs/go-diskfs"
	"github.com/diskfs/go-diskfs/disk"
	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/fat32"
	"github.com/diskfs/go-diskfs/testhelper"
	"github.com/diskfs/go-diskfs/util"
)

var (
	intImage     = os.Getenv("TEST_IMAGE")
	keepTmpFiles = os.Getenv("KEEPTESTFILES")
)

func getOpenMode(mode int) string {
	modes := make([]string, 0)
	if mode&os.O_CREATE == os.O_CREATE {
		modes = append(modes, "CREATE")
	}
	if mode&os.O_APPEND == os.O_APPEND {
		modes = append(modes, "APPEND")
	}
	if mode&os.O_RDWR == os.O_RDWR {
		modes = append(modes, "RDWR")
	} else {
		modes = append(modes, "RDONLY")
	}
	return strings.Join(modes, "|")
}

func tmpFat32(fill bool, embedPre, embedPost int64) (*os.File, error) {
	filename := "fat32_test"
	f, err := os.CreateTemp("", filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tempfile %s :%v", filename, err)
	}

	// either copy the contents of the base file over, or make a file of similar size
	b, err := os.ReadFile(fat32.Fat32File)
	if err != nil {
		return nil, fmt.Errorf("Failed to read contents of %s: %v", fat32.Fat32File, err)
	}
	if embedPre > 0 {
		empty := make([]byte, embedPre)
		written, err := f.Write(empty)
		if err != nil {
			return nil, fmt.Errorf("Failed to write %d zeroes at beginning of %s: %v", embedPre, filename, err)
		}
		if written != len(empty) {
			return nil, fmt.Errorf("wrote only %d zeroes at beginning of %s instead of %d", written, filename, len(empty))
		}
	}
	if fill {
		written, err := f.Write(b)
		if err != nil {
			return nil, fmt.Errorf("Failed to write contents of %s to %s: %v", fat32.Fat32File, filename, err)
		}
		if written != len(b) {
			return nil, fmt.Errorf("wrote only %d bytes of %s to %s instead of %d", written, fat32.Fat32File, filename, len(b))
		}
	} else {
		size := int64(len(b))
		empty := make([]byte, size)
		written, err := f.Write(empty)
		if err != nil {
			return nil, fmt.Errorf("Failed to write %d zeroes as content of %s: %v", size, filename, err)
		}
		if written != len(empty) {
			return nil, fmt.Errorf("wrote only %d zeroes as content of %s instead of %d", written, filename, len(empty))
		}
	}
	if embedPost > 0 {
		empty := make([]byte, embedPost)
		written, err := f.Write(empty)
		if err != nil {
			return nil, fmt.Errorf("Failed to write %d zeroes at end of %s: %v", embedPost, filename, err)
		}
		if written != len(empty) {
			return nil, fmt.Errorf("wrote only %d zeroes at end of %s instead of %d", written, filename, len(empty))
		}
	}

	return f, nil
}

func TestFat32Type(t *testing.T) {
	fs := &fat32.FileSystem{}
	fstype := fs.Type()
	expected := filesystem.TypeFat32
	if fstype != expected {
		t.Errorf("Type() returns %v instead of expected %v", fstype, expected)
	}
}

func TestFat32Mkdir(t *testing.T) {
	// only do this test if os.Getenv("TEST_IMAGE") contains a real image
	if intImage == "" {
		return
	}
	//nolint:thelper // this is not a helper function
	runTest := func(t *testing.T, post, pre int64, fatFunc func(util.File, int64, int64, int64) (*fat32.FileSystem, error)) {
		// create our directories
		tests := []string{
			"/",
			"/foo",
			"/foo/bar",
			"/a/b/c",
		}
		f, err := tmpFat32(true, pre, post)
		if err != nil {
			t.Fatal(err)
		}
		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}
		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}
		fs, err := fatFunc(f, fileInfo.Size()-pre-post, pre, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}
		for _, p := range tests {
			err := fs.Mkdir(p)
			switch {
			case err != nil:
				t.Errorf("Mkdir(%s): error %v", p, err)
			default:
				// check that the directory actually was created
				output := new(bytes.Buffer)
				mpath := "/file.img"
				mounts := map[string]string{
					f.Name(): mpath,
				}
				err := testhelper.DockerRun(nil, output, false, true, mounts, intImage, "mdir", "-i", fmt.Sprintf("%s@@%d", mpath, pre), fmt.Sprintf("::%s", p))
				if err != nil {
					t.Errorf("Mkdir(%s): Unexpected err: %v", p, err)
					t.Log(output.String())
				}
			}
		}
	}
	t.Run("read to Mkdir", func(t *testing.T) {
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0, fat32.Read)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000, fat32.Read)
		})
	})
	t.Run("Create to Mkdir", func(t *testing.T) {
		// This is to enable Create "fit" into the common testing logic
		createShim := func(file util.File, size int64, start int64, blocksize int64) (*fat32.FileSystem, error) {
			return fat32.Create(file, size, start, blocksize, "")
		}
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0, createShim)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000, createShim)
		})
	})
}

func TestFat32Create(t *testing.T) {
	tests := []struct {
		blocksize int64
		filesize  int64
		fs        *fat32.FileSystem
		err       error
	}{
		{500, 6000, nil, fmt.Errorf("blocksize for FAT32 must be")},
		{513, 6000, nil, fmt.Errorf("blocksize for FAT32 must be")},
		{512, fat32.Fat32MaxSize + 100000, nil, fmt.Errorf("requested size is larger than maximum allowed FAT32")},
		{512, 0, nil, fmt.Errorf("requested size is smaller than minimum allowed FAT32")},
		{512, 10000000, &fat32.FileSystem{}, nil},
	}
	//nolint:thelper // this is not a helper function
	runTest := func(t *testing.T, pre, post int64) {
		for _, t2 := range tests {
			tt := t2
			t.Run(fmt.Sprintf("blocksize %d filesize %d", tt.blocksize, tt.filesize), func(t *testing.T) {
				// get a temporary working file
				f, err := tmpFat32(false, pre, post)
				if err != nil {
					t.Fatal(err)
				}
				defer os.Remove(f.Name())
				// create the filesystem
				fs, err := fat32.Create(f, tt.filesize-pre-post, pre, tt.blocksize, "")
				switch {
				case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
					t.Errorf("Create(%s, %d, %d, %d): mismatched errors\nactual %v\nexpected %v", f.Name(), tt.filesize, 0, tt.blocksize, err, tt.err)
				case (fs == nil && tt.fs != nil) || (fs != nil && tt.fs == nil):
					t.Errorf("Create(%s, %d, %d, %d): mismatched fs\nactual %v\nexpected %v", f.Name(), tt.filesize, 0, tt.blocksize, fs, tt.fs)
				}
				// we do not match the filesystems here, only check functional accuracy
			})
		}
	}

	t.Run("entire image", func(t *testing.T) {
		runTest(t, 0, 0)
	})
	t.Run("embedded filesystem", func(t *testing.T) {
		runTest(t, 500, 1000)
	})
}

func TestFat32Read(t *testing.T) {
	// test cases:
	// - invalid blocksize
	// - invalid file size (0 and too big)
	// - invalid FSISBootSector
	// - valid file
	tests := []struct {
		blocksize  int64
		filesize   int64
		bytechange int64
		fs         *fat32.FileSystem
		err        error
	}{
		{500, 6000, -1, nil, fmt.Errorf("blocksize for FAT32 must be")},
		{513, 6000, -1, nil, fmt.Errorf("blocksize for FAT32 must be")},
		{512, fat32.Fat32MaxSize + 10000, -1, nil, fmt.Errorf("requested size is larger than maximum allowed FAT32 size")},
		{512, 0, -1, nil, fmt.Errorf("requested size is smaller than minimum allowed FAT32 size")},
		{512, 10000000, 512, nil, fmt.Errorf("error reading FileSystem Information Sector")},
		{512, 10000000, -1, &fat32.FileSystem{}, nil},
	}
	//nolint:thelper // this is not a helper function
	runTest := func(t *testing.T, pre, post int64) {
		for _, t2 := range tests {
			tt := t2
			t.Run(fmt.Sprintf("blocksize %d filesize %d bytechange %d", tt.filesize, tt.blocksize, tt.bytechange), func(t *testing.T) {
				// get a temporary working file
				f, err := tmpFat32(true, pre, post)
				if err != nil {
					t.Fatal(err)
				}
				defer os.Remove(f.Name())
				// make any changes needed to corrupt it
				corrupted := ""
				if tt.bytechange >= 0 {
					b := make([]byte, 1)
					_, _ = rand.Read(b)
					_, _ = f.WriteAt(b, tt.bytechange+pre)
					corrupted = fmt.Sprintf("corrupted %d", tt.bytechange+pre)
				}
				// create the filesystem
				fs, err := fat32.Read(f, tt.filesize-pre-post, pre, tt.blocksize)
				switch {
				case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
					t.Errorf("read(%s, %d, %d, %d) %s: mismatched errors, actual %v expected %v", f.Name(), tt.filesize, 0, tt.blocksize, corrupted, err, tt.err)
				case (fs == nil && tt.fs != nil) || (fs != nil && tt.fs == nil):
					t.Errorf("read(%s, %d, %d, %d) %s: mismatched fs, actual then expected", f.Name(), tt.filesize, 0, tt.blocksize, corrupted)
					t.Logf("%v", fs)
					t.Logf("%v", tt.fs)
				}
				// we do not match the filesystems here, only check functional accuracy
			})
		}
	}
	t.Run("entire image", func(t *testing.T) {
		runTest(t, 0, 0)
	})
	t.Run("embedded filesystem", func(t *testing.T) {
		runTest(t, 500, 1000)
	})
}

func TestFat32ReadDir(t *testing.T) {
	//nolint:thelper // this is not a helper function
	runTest := func(t *testing.T, pre, post int64) {
		// get a temporary working file
		f, err := tmpFat32(true, pre, post)
		if err != nil {
			t.Fatal(err)
		}
		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}
		// determine entries from the actual data
		rootEntries, _, err := fat32.GetValidDirectoryEntries()
		if err != nil {
			t.Fatalf("error getting valid directory entries: %v", err)
		}
		// ignore volume entry when public-facing root entries
		rootEntries = rootEntries[:len(rootEntries)-1]
		fooEntries, _, err := fat32.GetValidDirectoryEntriesExtended("/foo")
		if err != nil {
			t.Fatalf("error getting valid directory entries for /foo: %v", err)
		}
		tests := []struct {
			path  string
			count int
			name  string
			isDir bool
			err   error
		}{
			{"/", len(rootEntries), "foo", true, nil},
			{"/foo", len(fooEntries), ".", true, nil},
			// 0 entries because the directory does not exist
			{"/a/b/c", 0, "", false, fmt.Errorf("error reading directory /a/b/c")},
		}
		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}
		fs, err := fat32.Read(f, fileInfo.Size()-pre-post, pre, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}
		for _, tt := range tests {
			output, err := fs.ReadDir(tt.path)
			switch {
			case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
				t.Errorf("readDir(%s): mismatched errors, actual: %v , expected: %v", tt.path, err, tt.err)
			case output == nil && tt.err == nil:
				t.Errorf("readDir(%s): Unexpected nil output", tt.path)
			case len(output) != tt.count:
				t.Errorf("readDir(%s): output gave %d entries instead of expected %d", tt.path, len(output), tt.count)
			case len(output) > 0 && output[0].IsDir() != tt.isDir:
				t.Errorf("readDir(%s): output gave directory %t expected %t", tt.path, output[0].IsDir(), tt.isDir)
			case len(output) > 0 && output[0].Name() != tt.name:
				t.Errorf("readDir(%s): output gave name %s expected %s", tt.path, output[0].Name(), tt.name)
			}
		}
	}
	t.Run("entire image", func(t *testing.T) {
		runTest(t, 0, 0)
	})
	t.Run("embedded filesystem", func(t *testing.T) {
		runTest(t, 500, 1000)
	})
}

//nolint:gocyclo // we really do not care about the cyclomatic complexity of a test function. Maybe someday we will improve it.
func TestFat32OpenFile(t *testing.T) {
	// opening directories and files for reading
	t.Run("read", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTest := func(t *testing.T, pre, post int64) {
			// get a temporary working file
			f, err := tmpFat32(true, pre, post)
			if err != nil {
				t.Fatal(err)
			}
			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}
			tests := []struct {
				path     string
				mode     int
				expected string
				err      error
			}{
				// error opening a directory
				{"/", os.O_RDONLY, "", fmt.Errorf("cannot open directory %s as file", "/")},
				{"/", os.O_RDWR, "", fmt.Errorf("cannot open directory %s as file", "/")},
				{"/", os.O_CREATE, "", fmt.Errorf("cannot open directory %s as file", "/")},
				// open non-existent file for read or read write
				{"/abcdefg", os.O_RDONLY, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				{"/abcdefg", os.O_RDWR, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				{"/abcdefg", os.O_APPEND, "", fmt.Errorf("target file %s does not exist", "/abcdefg")},
				// open file for read or read write and check contents
				{"/CORTO1.TXT", os.O_RDONLY, "Tenemos un archivo corto\n", nil},
				{"/CORTO1.TXT", os.O_RDWR, "Tenemos un archivo corto\n", nil},
				// open file for create that already exists
				// {"/CORTO1.TXT", os.O_CREATE | os.O_RDWR, "Tenemos un archivo corto\n", nil},
				// {"/CORTO1.TXT", os.O_CREATE | os.O_RDONLY, "Tenemos un archivo corto\n", nil},
			}
			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}
			fs, err := fat32.Read(f, fileInfo.Size()-pre-post, pre, 512)
			if err != nil {
				t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
			}
			for _, tt := range tests {
				header := fmt.Sprintf("OpenFile(%s, %s)", tt.path, getOpenMode(tt.mode))
				reader, err := fs.OpenFile(tt.path, tt.mode)
				switch {
				case (err == nil && tt.err != nil) || (err != nil && tt.err == nil) || (err != nil && tt.err != nil && !strings.HasPrefix(err.Error(), tt.err.Error())):
					t.Errorf("%s: mismatched errors, actual: %v , expected: %v", header, err, tt.err)
				case reader == nil && (tt.err == nil || tt.expected != ""):
					t.Errorf("%s: Unexpected nil output", header)
				case reader != nil:
					b, err := io.ReadAll(reader)
					if err != nil {
						t.Errorf("%s: io.ReadAll(reader) unexpected error: %v", header, err)
					}
					if string(b) != tt.expected {
						t.Errorf("%s: mismatched contents, actual then expected", header)
						t.Log(string(b))
						t.Log(tt.expected)
					}
				}
			}
		}
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000)
		})
	})

	// write / create-and-write files and check contents
	// *** Write - writes right after last write or read
	// *** Read - reads right after last write or read
	// ** WriteAt - writes at specific location in file
	// ** ReadAt - reads at specific location in file
	t.Run("Write", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTest := func(t *testing.T, pre, post int64) {
			tests := []struct {
				path      string
				mode      int
				beginning bool // true means "Seek() to beginning of file before writing"; false means "read entire file then write"
				contents  string
				expected  string
				err       error
			}{
				//  - open for create file that does not exist (write contents, check that written)
				{"/abcdefg", os.O_RDWR | os.O_CREATE, false, "This is a test", "This is a test", nil},
				//  - open for readwrite file that does exist (write contents, check that overwritten)
				{"/CORTO1.TXT", os.O_RDWR, true, "This is a very long replacement string", "This is a very long replacement string", nil},
				{"/CORTO1.TXT", os.O_RDWR, true, "Two", "Twoemos un archivo corto\n", nil},
				{"/CORTO1.TXT", os.O_RDWR, false, "This is a very long replacement string", "Tenemos un archivo corto\nThis is a very long replacement string", nil},
				{"/CORTO1.TXT", os.O_RDWR, false, "Two", "Tenemos un archivo corto\nTwo", nil},
				//  - open for append file that does exist (write contents, check that appended)
				{"/CORTO1.TXT", os.O_APPEND, false, "More", "", filesystem.ErrReadonlyFilesystem},
				{"/CORTO1.TXT", os.O_APPEND | os.O_RDWR, false, "More", "Tenemos un archivo corto\nMore", nil},
				{"/CORTO1.TXT", os.O_APPEND, true, "More", "", filesystem.ErrReadonlyFilesystem},
				{"/CORTO1.TXT", os.O_APPEND | os.O_RDWR, true, "More", "Moremos un archivo corto\n", nil},
			}
			for _, t2 := range tests {
				tt := t2
				t.Run(fmt.Sprintf("path %s mode %v beginning %v", tt.path, tt.mode, tt.beginning), func(t *testing.T) {
					header := fmt.Sprintf("OpenFile(%s, %s, %t)", tt.path, getOpenMode(tt.mode), tt.beginning)
					// get a temporary working file
					f, err := tmpFat32(true, pre, post)
					if err != nil {
						t.Fatal(err)
					}
					if keepTmpFiles == "" {
						defer os.Remove(f.Name())
					} else {
						fmt.Println(f.Name())
					}
					fileInfo, err := f.Stat()
					if err != nil {
						t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
					}
					fs, err := fat32.Read(f, fileInfo.Size()-pre-post, pre, 512)
					if err != nil {
						t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
					}
					readWriter, err := fs.OpenFile(tt.path, tt.mode)
					switch {
					case err != nil:
						t.Errorf("%s: unexpected error: %v", header, err)
					case readWriter == nil:
						t.Errorf("%s: Unexpected nil output", header)
					default:
						// write and then read
						bWrite := []byte(tt.contents)
						if tt.beginning {
							offset, err := readWriter.Seek(0, 0)
							if err != nil {
								t.Errorf("%s: Seek(0,0) unexpected error: %v", header, err)
								return
							}
							if offset != 0 {
								t.Errorf("%s: Seek(0,0) reset to %d instead of %d", header, offset, 0)
								return
							}
						} else {
							b := make([]byte, 512)
							_, err := readWriter.Read(b)
							if err != nil && err != io.EOF {
								t.Errorf("%s: io.ReadAll(readWriter) unexpected error: %v", header, err)
								return
							}
						}
						written, writeErr := readWriter.Write(bWrite)
						_, _ = readWriter.Seek(0, 0)
						bRead, readErr := io.ReadAll(readWriter)

						switch {
						case readErr != nil:
							t.Errorf("%s: io.ReadAll() unexpected error: %v", header, readErr)
						case (writeErr == nil && tt.err != nil) || (writeErr != nil && tt.err == nil) || (writeErr != nil && tt.err != nil && !strings.HasPrefix(writeErr.Error(), tt.err.Error())):
							t.Errorf("%s: readWriter.Write(b) mismatched errors, actual: %v , expected: %v", header, writeErr, tt.err)
						case written != len(bWrite) && tt.err == nil:
							t.Errorf("%s: readWriter.Write(b) wrote %d bytes instead of expected %d", header, written, len(bWrite))
						case string(bRead) != tt.expected && tt.err == nil:
							t.Errorf("%s: mismatched contents, actual then expected", header)
							t.Log(string(bRead))
							t.Log(tt.expected)
						}
					}
				})
			}
		}
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000)
		})
	})

	// write many files to exceed the first cluster, then read back
	t.Run("Write Many", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTest := func(t *testing.T, pre, post int64) {
			f, err := tmpFat32(false, pre, post)
			if err != nil {
				t.Fatal(err)
			}
			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}
			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}
			fs, err := fat32.Create(f, fileInfo.Size()-pre-post, pre, 512, " NO NAME")
			if err != nil {
				t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
			}

			pathPrefix := "/f"
			fileCount := 32
			for fileNumber := 1; fileNumber <= fileCount; fileNumber++ {
				fileName := fmt.Sprintf("%s%d", pathPrefix, fileNumber)
				fileContent := []byte(fileName)
				readWriter, err := fs.OpenFile(fileName, os.O_RDWR|os.O_CREATE)
				switch {
				case err != nil:
					t.Errorf("write many: unexpected error writing %s: %v", fileName, err)
				case readWriter == nil:
					t.Errorf("write many: unexpected nil output writing %s", fileName)
				default:
					_, _ = readWriter.Seek(0, 0)
					written, writeErr := readWriter.Write(fileContent)
					_, _ = readWriter.Seek(0, 0)
					readFileContent, readErr := io.ReadAll(readWriter)
					switch {
					case readErr != nil:
						t.Errorf("write many: io.ReadAll() unexpected error on %s: %v", fileName, readErr)
					case writeErr != nil:
						t.Errorf("write many: readWriter.Write(b) error on %s: %v", fileName, writeErr)
					case written != len(fileContent):
						t.Errorf("write many: readWriter.Write(b) wrote %d bytes instead of expected %d on %s", written, len(fileContent), fileName)
					case string(readFileContent) != fileName:
						t.Errorf("write many: mismatched contents on %s, expected: %s, got: %s", fileName, fileName, string(readFileContent))
					}
				}
			}

			dir, err := fs.ReadDir("/")
			if err != nil {
				t.Errorf("write many: error reading /: %v", err)
			}
			if len(dir) != fileCount {
				t.Errorf("write many: entry count mismatch on /: expected %d, got %d -- %v", fileCount, len(dir), dir)
			}
		}
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000)
		})
	})

	// large file should cross multiple clusters
	// out cluster size is 512 bytes, so make it 10+ clusters
	t.Run("Large File", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTest := func(t *testing.T, pre, post int64) {
			// get a temporary working file
			f, err := tmpFat32(true, pre, post)
			if err != nil {
				t.Fatal(err)
			}
			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}
			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}
			fs, err := fat32.Read(f, fileInfo.Size()-pre-post, pre, 512)
			if err != nil {
				t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
			}
			path := "/abcdefghi"
			mode := os.O_RDWR | os.O_CREATE
			// each cluster is 512 bytes, so use 10 clusters and a bit of another
			size := 10*512 + 22
			bWrite := make([]byte, size)
			header := fmt.Sprintf("OpenFile(%s, %s)", path, getOpenMode(mode))
			readWriter, err := fs.OpenFile(path, mode)
			switch {
			case err != nil:
				t.Errorf("%s: unexpected error: %v", header, err)
			case readWriter == nil:
				t.Errorf("%s: Unexpected nil output", header)
			default:
				// write and then read
				_, _ = rand.Read(bWrite)
				written, writeErr := readWriter.Write(bWrite)
				_, _ = readWriter.Seek(0, 0)
				bRead, readErr := io.ReadAll(readWriter)

				switch {
				case readErr != nil:
					t.Errorf("%s: io.ReadAll() unexpected error: %v", header, readErr)
				case writeErr != nil:
					t.Errorf("%s: readWriter.Write(b) unexpected error: %v", header, writeErr)
				case written != len(bWrite):
					t.Errorf("%s: readWriter.Write(b) wrote %d bytes instead of expected %d", header, written, len(bWrite))
				case !bytes.Equal(bWrite, bRead):
					t.Errorf("%s: mismatched contents, read %d expected %d, actual data then expected:", header, len(bRead), len(bWrite))
				}
			}
		}
		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000)
		})
	})

	// large file should cross multiple clusters
	// out cluster size is 512 bytes, so make it 10+ clusters
	t.Run("Truncate File", func(t *testing.T) {
		// get a temporary working file
		f, err := tmpFat32(true, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}
		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}
		fs, err := fat32.Read(f, fileInfo.Size(), 0, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}
		p := "/abcdefghi"
		mode := os.O_RDWR | os.O_CREATE
		// each cluster is 512 bytes, so use 10 clusters and a bit of another
		size := 10*512 + 22
		bWrite := make([]byte, size)
		header := fmt.Sprintf("OpenFile(%s, %s)", p, getOpenMode(mode))
		readWriter, err := fs.OpenFile(p, mode)
		switch {
		case err != nil:
			t.Fatalf("%s: unexpected error: %v", header, err)
		case readWriter == nil:
			t.Fatalf("%s: Unexpected nil output", header)
		default:
			// write and then read
			_, _ = rand.Read(bWrite)
			written, writeErr := readWriter.Write(bWrite)
			_, _ = readWriter.Seek(0, 0)

			switch {
			case writeErr != nil:
				t.Fatalf("%s: readWriter.Write(b) unexpected error: %v", header, writeErr)
			case written != len(bWrite):
				t.Fatalf("%s: readWriter.Write(b) wrote %d bytes instead of expected %d", header, written, len(bWrite))
			}
		}
		// we now have written lots of data to the file. Close it, then reopen it to truncate
		if err := readWriter.Close(); err != nil {
			t.Fatalf("error closing file: %v", err)
		}
		// and open to truncate
		mode = os.O_RDWR | os.O_TRUNC
		readWriter, err = fs.OpenFile(p, mode)
		if err != nil {
			t.Fatalf("could not reopen file: %v", err)
		}
		// read the data
		bRead, readErr := io.ReadAll(readWriter)
		switch {
		case readErr != nil:
			t.Fatalf("%s: io.ReadAll() unexpected error: %v", header, readErr)
		case len(bRead) != 0:
			t.Fatalf("%s: readWriter.ReadAll(b) read %d bytes after truncate instead of expected %d", header, len(bRead), 0)
		}
	})

	// large files are often written in multiple passes
	t.Run("Streaming Large File", func(t *testing.T) {
		//nolint:thelper // this is not a helper function
		runTest := func(t *testing.T, pre, post int64) {
			// get a temporary working file
			f, err := tmpFat32(true, pre, post)
			if err != nil {
				t.Fatal(err)
			}
			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}
			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}
			fs, err := fat32.Read(f, fileInfo.Size()-pre-post, pre, 512)
			if err != nil {
				t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
			}
			path := "/abcdefghi"
			mode := os.O_RDWR | os.O_CREATE
			// each cluster is 512 bytes, so use 10 clusters and a bit of another
			size := 10*512 + 22
			bWrite := make([]byte, size)
			header := fmt.Sprintf("OpenFile(%s, %s)", path, getOpenMode(mode))
			readWriter, err := fs.OpenFile(path, mode)
			switch {
			case err != nil:
				t.Errorf("%s: unexpected error: %v", header, err)
			case readWriter == nil:
				t.Errorf("%s: Unexpected nil output", header)
			default:
				// success
			}

			_, _ = rand.Read(bWrite)
			writeSizes := []int{512, 1024, 256}
			low := 0
			for i := 0; low < len(bWrite); i++ {
				high := low + writeSizes[i%len(writeSizes)]
				if high > len(bWrite) {
					high = len(bWrite)
				}
				written, err := readWriter.Write(bWrite[low:high])
				if err != nil {
					t.Errorf("%s: readWriter.Write(b) unexpected error: %v", header, err)
				}
				if written != high-low {
					t.Errorf("%s: readWriter.Write(b) wrote %d bytes instead of expected %d", header, written, high-low)
				}
				low = high
			}

			_, _ = readWriter.Seek(0, 0)
			bRead, readErr := io.ReadAll(readWriter)

			switch {
			case readErr != nil:
				t.Errorf("%s: io.ReadAll() unexpected error: %v", header, readErr)
			case !bytes.Equal(bWrite, bRead):
				t.Errorf("%s: mismatched contents, read %d expected %d, actual data then expected:", header, len(bRead), len(bWrite))
			}
		}

		t.Run("entire image", func(t *testing.T) {
			runTest(t, 0, 0)
		})
		t.Run("embedded filesystem", func(t *testing.T) {
			runTest(t, 500, 1000)
		})
	})
}

func TestFat32Label(t *testing.T) {
	t.Run("read-label", func(t *testing.T) {
		// get a mock filesystem image
		f, err := tmpFat32(true, 0, 0)
		if err != nil {
			t.Fatal(err)
		}

		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}

		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}

		// read the filesystem
		fs, err := fat32.Read(f, fileInfo.Size(), 0, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}

		// validate the label
		label := fs.Label()
		if label != "go-diskfs" {
			t.Errorf("Unexpected label '%s', expected '%s'", label, "go-diskfs")
		}
	})

	t.Run("create-label", func(t *testing.T) {
		// get a mock filesystem image
		f, err := tmpFat32(false, 0, 0)
		if err != nil {
			t.Fatal(err)
		}

		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}

		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}

		// create an empty filesystem
		fs, err := fat32.Create(f, fileInfo.Size(), 0, 512, "go-diskfs")
		if err != nil {
			t.Fatalf("error creating fat32 filesystem: %v", err)
		}

		// read the label back
		label := fs.Label()
		if label != "go-diskfs" {
			t.Errorf("Unexpected label '%s', expected '%s'", label, "go-diskfs")
		}

		// re-open the filesystem
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file %s: %v", f.Name(), err)
		}
		f, err = os.Open(f.Name())
		if err != nil {
			t.Fatalf("error re-opening file %s: %v", f.Name(), err)
		}

		// read the filesystem
		fs, err = fat32.Read(f, fileInfo.Size(), 0, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}

		// read-back the label
		label = fs.Label()
		if label != "go-diskfs" {
			t.Errorf("Unexpected label '%s', expected '%s'", label, "go-diskfs")
		}
	})

	t.Run("write-label", func(t *testing.T) {
		// get a mock filesystem image
		f, err := tmpFat32(false, 0, 0)
		if err != nil {
			t.Fatal(err)
		}

		if keepTmpFiles == "" {
			defer os.Remove(f.Name())
		} else {
			fmt.Println(f.Name())
		}

		fileInfo, err := f.Stat()
		if err != nil {
			t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
		}

		// create an empty filesystem
		fs, err := fat32.Create(f, fileInfo.Size(), 0, 512, "go-diskfs")
		if err != nil {
			t.Fatalf("error creating fat32 filesystem: %v", err)
		}

		// set the label
		err = fs.SetLabel("Other Label")
		if err != nil {
			t.Fatalf("error setting label: %v", err)
		}

		// read the label back
		label := fs.Label()
		if label != "Other Label" {
			t.Errorf("Unexpected label '%s', expected '%s'", label, "Other Label")
		}

		// re-open the filesystem
		if err := f.Close(); err != nil {
			t.Fatalf("error closing file %s: %v", f.Name(), err)
		}
		f, err = os.Open(f.Name())
		if err != nil {
			t.Fatalf("error re-opening file %s: %v", f.Name(), err)
		}

		// read the filesystem
		fs, err = fat32.Read(f, fileInfo.Size(), 0, 512)
		if err != nil {
			t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
		}

		// read-back the label
		label = fs.Label()
		if label != "Other Label" {
			t.Errorf("Unexpected label '%s', expected '%s'", label, "Other Label")
		}
	})
}

func TestFat32MkdirCases(t *testing.T) {
	f, err := tmpFat32(false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	fs, err := fat32.Create(f, 1048576, 0, 512, "")
	if err != nil {
		t.Error(err.Error())
	}
	err = fs.Mkdir("/EFI/BOOT")
	if err != nil {
		t.Error(err.Error())
	}
	// Make the same folders but now lowercase ... I expect it not to create anything new,
	// these folders exist but are named /EFI/BOOT
	err = fs.Mkdir("/efi/boot")
	if err != nil {
		t.Error(err.Error())
	}
	files, err := fs.ReadDir("/")
	if err != nil {
		t.Error(err.Error())
	}
	if len(files) != 1 {
		for _, file := range files {
			fmt.Printf("file: %s\n", file.Name())
		}
		t.Fatalf("expected 1 file, found %d", len(files))
	}
}

func Test83Lowercase(t *testing.T) {
	// get a temporary working file
	f, err := tmpFat32(true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if keepTmpFiles == "" {
		defer os.Remove(f.Name())
	} else {
		fmt.Println(f.Name())
	}
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
	}
	fs, err := fat32.Read(f, fileInfo.Size(), 0, 512)
	if err != nil {
		t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
	}

	// Ensure using correct masks for lowercase shortname and extension (bits 3 and 4, zero-based)
	files, err := fs.ReadDir("/lower83")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"lower.low", "lower.UPP", "UPPER.low"}
	i := 0
	for _, file := range files {
		if file.Name() == "." || file.Name() == ".." {
			continue
		}
		if file.Name() != expected[i] {
			t.Errorf("got %q, expected %q", file.Name(), expected[i])
		}
		i++
	}
}

func TestOpenFileCaseInsensitive(t *testing.T) {
	// get a temporary working file
	f, err := tmpFat32(true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if keepTmpFiles == "" {
		defer os.Remove(f.Name())
	} else {
		fmt.Println(f.Name())
	}
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
	}
	fs, err := fat32.Read(f, fileInfo.Size(), 0, 512)
	if err != nil {
		t.Fatalf("error reading fat32 filesystem from %s: %v", f.Name(), err)
	}

	// Ensure openfile is case-insensitive for the 8.3 name as well as the long name
	paths := []string{
		// The actual name
		"/lower83/lower.low",
		// Same name but different extension case
		"/lower83/lower.LOW",
		// Same name but different base case
		"/lower83/LOWER.LOW",
		// Actual name/case of non-8.3 file
		"/tercer_archivo",
		// Same name but uppercase
		"/TERCER_ARCHIVO",
	}
	for _, path := range paths {
		file, err := fs.OpenFile(path, os.O_RDONLY)
		if err != nil {
			t.Errorf("error opening %s: %v\n", path, err)
		} else {
			file.Close()
		}
	}
}

func testMkFile(fs filesystem.FileSystem, p string, size int) error {
	rw, err := fs.OpenFile(p, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return err
	}
	smallFile := make([]byte, size)
	_, err = rw.Write(smallFile)
	if err != nil {
		return err
	}
	return nil
}

func TestCreateFileTree(t *testing.T) {
	filename := "fat32_test"
	tmpDir := t.TempDir()
	tmpImgPath := filepath.Join(tmpDir, filename)

	// 6GB to test large disk
	size := int64(6 * 1024 * 1024 * 1024)
	d, err := diskfs.Create(tmpImgPath, size, diskfs.Raw, diskfs.SectorSizeDefault)
	if err != nil {
		t.Fatalf("error creating disk: %v", err)
	}

	spec := disk.FilesystemSpec{
		Partition: 0,
		FSType:    filesystem.TypeFat32,
	}
	fs, err := d.CreateFilesystem(spec)
	if err != nil {
		t.Fatalf("error creating filesystem: %v", err)
	}

	if err := fs.Mkdir("/A"); err != nil {
		t.Errorf("Error making dir /A in root: %v", err)
	}
	if err := fs.Mkdir("/b"); err != nil {
		t.Errorf("Error making dir /b in root: %v", err)
	}
	if err := testMkFile(fs, "/rootfile", 11); err != nil {
		t.Errorf("Error making microfile in root: %v", err)
	}
	for i := 0; i < 100; i++ {
		dir := fmt.Sprintf("/b/sub%d", i)
		if err := fs.Mkdir(dir); err != nil {
			t.Errorf("Error making directory %s: %v", dir, err)
		}
		blobdir := path.Join(dir, "blob")
		if err := fs.Mkdir(blobdir); err != nil {
			t.Errorf("Error making directory %s: %v", blobdir, err)
		}
		file := path.Join(blobdir, "microfile")
		if err := testMkFile(fs, file, 11); err != nil {
			t.Errorf("Error making microfile %s: %v", file, err)
		}
		file = path.Join(blobdir, "randfile")
		size := mathrandv2.IntN(73) // #nosec G404
		if err := testMkFile(fs, file, size); err != nil {
			t.Errorf("Error making random file %s: %v", file, err)
		}
		file = path.Join(blobdir, "smallfile")
		if err := testMkFile(fs, file, 5*1024*1024); err != nil {
			t.Errorf("Error making small file %s: %v", file, err)
		}
	}
	file := "/b/sub49/blob/gigfile1"
	gb := 1024 * 1024 * 1024
	if err := testMkFile(fs, file, gb); err != nil {
		t.Errorf("Error making gigfile1 %s: %v", file, err)
	}
	file = "/b/sub50/blob/gigfile1"
	if err := testMkFile(fs, file, gb); err != nil {
		t.Errorf("Error making gigfile1 %s: %v", file, err)
	}
	file = "/b/sub51/blob/gigfile1"
	if err := testMkFile(fs, file, gb); err != nil {
		t.Errorf("Error making gigfile1 %s: %v", file, err)
	}
}

func Test_Rename(t *testing.T) {
	workingPath := "/"
	srcFile := "old.txt"
	dstFile := "new.txt"
	createFile := func(t *testing.T, fs *fat32.FileSystem, name, content string) {
		t.Helper()
		origFile, err := fs.OpenFile(filepath.Join(workingPath, name), os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Could not create file %s: %+v", name, err)
		}
		defer origFile.Close()
		// write test file
		_, err = origFile.Write([]byte(content))
		if err != nil {
			t.Fatalf("Could not Write file %s, %+v", name, err)
		}
	}
	readFile := func(t *testing.T, fs *fat32.FileSystem, name string) string {
		t.Helper()
		file, err := fs.OpenFile(filepath.Join(workingPath, name), os.O_RDONLY)
		if err != nil {
			t.Fatalf("file %s does not exist: %+v", name, err)
		}
		defer file.Close()
		buf := &bytes.Buffer{}
		_, err = io.Copy(buf, file)
		if err != nil {
			t.Fatalf("Could not read file %s: %+v", name, err)
		}
		return buf.String()
	}
	tests := []struct {
		name     string
		hasError bool
		pre      func(t *testing.T, fs *fat32.FileSystem)
		post     func(t *testing.T, fs *fat32.FileSystem)
	}{
		{
			name:     "simple renaming works without errors",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				createFile(t, fs, srcFile, "FooBar")
			},
			post: func(_ *testing.T, fs *fat32.FileSystem) {
				// check if original file is there -> should not be the case
				origFile, err := fs.OpenFile(srcFile, os.O_RDONLY)
				if err == nil {
					defer origFile.Close()
					t.Fatal("Original file is still there")
				}
				// check if new file is there -> should be the case
				content := readFile(t, fs, dstFile)
				if content != "FooBar" {
					t.Fatalf("Content should be '%s', but is '%s'", "FooBar", content)
				}
			},
		},
		{
			name:     "destination file already exists and gets overwritten",
			hasError: false,
			pre: func(_ *testing.T, fs *fat32.FileSystem) {
				createFile(t, fs, srcFile, "FooBar")
				// create destination file
				createFile(t, fs, dstFile, "This should be overwritten")
			},
			post: func(_ *testing.T, fs *fat32.FileSystem) {
				origFile, err := fs.OpenFile(filepath.Join(workingPath, srcFile), os.O_RDONLY)
				if err == nil {
					defer origFile.Close()
					t.Fatal("Original file is still there")
				}
				// check if new file is there -> should be the case
				content := readFile(t, fs, dstFile)
				if content != "FooBar" {
					t.Fatalf("Content should be '%s', but is '%s'", "FooBar", content)
				}
			},
		},
		{
			name:     "source file does not exist",
			hasError: true,
			pre: func(_ *testing.T, _ *fat32.FileSystem) {
				// do not create orig file
			},
			post: func(_ *testing.T, _ *fat32.FileSystem) {

			},
		},
		{
			name:     "renaming long file to short file",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				var s string
				for i := 0; i < 255; i++ {
					s += "a"
				}
				srcFile = s
				createFile(t, fs, s, "orig")
			},
			post: func(_ *testing.T, _ *fat32.FileSystem) {
				srcFile = "old.txt"
			},
		},
		{
			name:     "renaming short file to long file",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				var s string
				for i := 0; i < 255; i++ {
					s += "a"
				}
				dstFile = s
				createFile(t, fs, srcFile, "orig")
			},
			post: func(_ *testing.T, _ *fat32.FileSystem) {
				dstFile = "new.txt"
			},
		},
		{
			name:     "rename a non empty directory",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				err := fs.Mkdir(filepath.Join(workingPath, srcFile))
				if err != nil {
					t.Fatalf("Could not create directory %s: %+v", srcFile, err)
				}
				// create file in directory which is going to be moved
				createFile(t, fs, filepath.Join(srcFile, "test.txt"), "FooBar")
			},
			post: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				_, err := fs.ReadDir(filepath.Join(workingPath, srcFile))
				if err == nil {
					t.Fatalf("source directory does exist: %+v", err)
				}
				_, err = fs.ReadDir(filepath.Join(workingPath, dstFile))
				if err != nil {
					t.Fatalf("destination directory does not exist: %+v", err)
				}
				content := readFile(t, fs, filepath.Join(dstFile, "test.txt"))
				if content != "FooBar" {
					t.Fatalf("Content should be '%s', but is '%s'", "FooBar", content)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// get a mock filesystem image
			f, err := tmpFat32(false, 0, 0)
			if err != nil {
				t.Fatal(err)
			}

			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}

			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}

			// create an empty filesystem
			fs, err := fat32.Create(f, fileInfo.Size(), 0, 512, "go-diskfs")
			if err != nil {
				t.Fatalf("error creating fat32 filesystem: %v", err)
			}

			test.pre(t, fs)

			err = fs.Rename(filepath.Join(workingPath, srcFile), filepath.Join(workingPath, dstFile))

			if test.hasError {
				if err == nil {
					t.Fatal("No Error renaming file", err)
				}
			} else {
				if err != nil {
					t.Fatal("Error renaming file", err)
				}
			}

			test.post(t, fs)
		})
	}
}

func Test_Remove(t *testing.T) {
	workingPath := "/"
	fileToRemove := "fileToRemove.txt"
	createFile := func(t *testing.T, fs *fat32.FileSystem, name, content string) {
		t.Helper()
		origFile, err := fs.OpenFile(filepath.Join(workingPath, name), os.O_CREATE|os.O_RDWR)
		if err != nil {
			t.Fatalf("Could not create file %s: %+v", name, err)
		}
		defer origFile.Close()
		// write test file
		_, err = origFile.Write([]byte(content))
		if err != nil {
			t.Fatalf("Could not Write file %s, %+v", name, err)
		}
	}
	tests := []struct {
		name     string
		hasError bool
		errorMsg string
		pre      func(t *testing.T, fs *fat32.FileSystem)
		post     func(t *testing.T, fs *fat32.FileSystem)
	}{
		{
			name:     "simple remove works without",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				createFile(t, fs, fileToRemove, "FooBar")
			},
			post: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				// check if original file is there -> should not be the case
				origFile, err := fs.OpenFile(fileToRemove, os.O_RDONLY)
				if err == nil {
					defer origFile.Close()
					t.Fatal("Original file is still there")
				}
			},
		},
		{
			name:     "file to remove does not exist",
			hasError: true,
			errorMsg: "target file /fileToRemove.txt does not exist",
			pre: func(_ *testing.T, _ *fat32.FileSystem) {
				// do not create any file
			},
			post: func(_ *testing.T, _ *fat32.FileSystem) {

			},
		},
		{
			name:     "removing multiple files",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				var s string
				for i := 0; i < 10240; i++ {
					s += "this is a big file\n"
				}
				for i := 0; i < 50; i++ {
					createFile(t, fs, fmt.Sprintf("file%d.txt", i), "small file")
				}
				createFile(t, fs, fileToRemove, s)
				for i := 50; i < 100; i++ {
					createFile(t, fs, fmt.Sprintf("file%d.txt", i), "small file")
				}
			},
			post: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				for i := 0; i < 100; i++ {
					err := fs.Remove(fmt.Sprintf("/file%d.txt", i))
					if err != nil {
						t.Fatalf("expected no error, but got %v", err)
					}
				}
			},
		},
		{
			name:     "removing empty dir",
			hasError: false,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				if err := fs.Mkdir(filepath.Join(workingPath, fileToRemove)); err != nil {
					t.Fatalf("could not create test directory: %+v", err)
				}
			},
			post: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				_, err := fs.ReadDir(filepath.Join(workingPath, fileToRemove))
				if err == nil {
					t.Fatalf("Expected that dir cannot be read, but is still there")
				}
			},
		},
		{
			name:     "cannot delete dir with content",
			hasError: true,
			pre: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				if err := fs.Mkdir(filepath.Join(workingPath, fileToRemove)); err != nil {
					t.Fatalf("could not create test directory: %+v", err)
				}
				// file within dir to remove
				createFile(t, fs, filepath.Join(workingPath, fileToRemove, "test"), "foo")
			},
			post: func(t *testing.T, fs *fat32.FileSystem) {
				t.Helper()
				_, err := fs.ReadDir(filepath.Join(workingPath, fileToRemove))
				if err != nil {
					t.Fatalf("Expected that dir can be read, but has error: %+v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// get a mock filesystem image
			f, err := tmpFat32(false, 0, 0)
			if err != nil {
				t.Fatal(err)
			}

			if keepTmpFiles == "" {
				defer os.Remove(f.Name())
			} else {
				fmt.Println(f.Name())
			}

			fileInfo, err := f.Stat()
			if err != nil {
				t.Fatalf("error getting file info for tmpfile %s: %v", f.Name(), err)
			}

			// create an empty filesystem
			fs, err := fat32.Create(f, fileInfo.Size(), 0, 512, "go-diskfs")
			if err != nil {
				t.Fatalf("error creating fat32 filesystem: %v", err)
			}

			test.pre(t, fs)

			err = fs.Remove(filepath.Join(workingPath, fileToRemove))

			if test.hasError {
				if err == nil {
					t.Fatal("No Error renaming file", err)
				} else if !strings.Contains(err.Error(), test.errorMsg) {
					t.Fatalf("Error does not contain expected msg: %s. Original error: %v", test.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Fatal("Error removing file", err)
				}
			}

			test.post(t, fs)
		})
	}
}
