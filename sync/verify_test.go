package sync

import (
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
