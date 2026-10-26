package fat12

import (
	"strings"
)

const (
	// KB represents one KB
	KB int64 = 1024
	// MB represents one MB
	MB int64 = 1024 * KB
	// GB represents one GB
	GB int64 = 1024 * MB

	// Fat12MaxSize is the maximum size of a FAT12 filesystem in bytes.
	// FAT12 supports at most 4084 clusters; at 64 sectors/cluster × 512 bytes = 32 KB/cluster
	// that gives ~128 MB, but in practice FAT12 is used only on small volumes.
	Fat12MaxSize int64 = 128 * MB
	// Fat16MaxSize is the maximum size of a FAT16 filesystem in bytes (2 GB).
	Fat16MaxSize int64 = 2 * GB
)

func universalizePath(p string) string {
	ps := strings.ReplaceAll(p, "\\", "/")
	if ps[0] == '/' {
		ps = ps[1:]
	}
	return ps
}

// slashPath turns '\' into '/', so that path.Dir and path.Base split a path
// the same way splitPath does.
func slashPath(p string) string {
	return strings.ReplaceAll(p, "\\", "/")
}

func splitPath(p string) []string {
	ps := universalizePath(p)
	parts := strings.Split(ps, "/")
	ret := make([]string, 0)
	for _, sub := range parts {
		if sub != "" {
			ret = append(ret, sub)
		}
	}
	return ret
}
