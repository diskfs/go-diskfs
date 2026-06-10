package sync

import (
	"io/fs"
	"testing"
	"testing/fstest"
)

func TestCompareFS(t *testing.T) {
	tests := []struct {
		name     string
		origFS   fstest.MapFS
		targetFS fstest.MapFS
		wantErr  bool
	}{
		{
			name: "identical filesystems",
			origFS: fstest.MapFS{
				"file.txt":       {Data: []byte("hello")},
				"dir/nested.txt": {Data: []byte("world")},
			},
			targetFS: fstest.MapFS{
				"file.txt":       {Data: []byte("hello")},
				"dir/nested.txt": {Data: []byte("world")},
			},
			wantErr: false,
		},
		{
			name: "different file contents",
			origFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{
				"file.txt": {Data: []byte("HELLO")},
			},
			wantErr: true,
		},
		{
			name: "missing file in target",
			origFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{},
			wantErr:  true,
		},
		{
			name: "extra file in target",
			origFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{
				"file.txt":  {Data: []byte("hello")},
				"extra.txt": {Data: []byte("extra")},
			},
			wantErr: true,
		},
		{
			// CopyFileSystem excludes lost+found, so a source that has one
			// (e.g. created by mke2fs) must not be required in the target.
			name: "excluded lost+found in source only",
			origFS: fstest.MapFS{
				"file.txt":            {Data: []byte("hello")},
				"lost+found":          {Mode: fs.ModeDir},
				"lost+found/#1234567": {Data: []byte("orphan")},
			},
			targetFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			wantErr: false,
		},
		{
			// Symmetric case: a target with its own lost+found must not be
			// reported as having an extra path.
			name: "excluded lost+found in target only",
			origFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{
				"file.txt":   {Data: []byte("hello")},
				"lost+found": {Mode: fs.ModeDir},
			},
			wantErr: false,
		},
		{
			// The exclusion covers the whole list, not just lost+found.
			name: "excluded .DS_Store in source only",
			origFS: fstest.MapFS{
				"file.txt":  {Data: []byte("hello")},
				".DS_Store": {Data: []byte("mac metadata")},
			},
			targetFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			wantErr: false,
		},
		{
			name: "directory vs file mismatch",
			origFS: fstest.MapFS{
				"dir/file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{
				"dir": {Data: []byte("not a dir")},
			},
			wantErr: true,
		},
		{
			name: "different file size",
			origFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello")},
			},
			targetFS: fstest.MapFS{
				"file.txt": {Data: []byte("hello world")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CompareFS(tt.origFS, tt.targetFS)

			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
