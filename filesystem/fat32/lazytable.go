package fat32

import (
	"encoding/binary"

	"github.com/diskfs/go-diskfs/filesystem/fat12"
)

// format32 is how FAT32 stores its entries: what fat12's lazy table reads and
// writes, and what the in-memory table encodes when it writes only the entries
// that changed.
var format32 = fat12.EntryFormat{
	Bits:      32,
	FATID:     func(head []byte) uint32 { return binary.LittleEndian.Uint32(head[0:4]) },
	EOCMarker: func(head []byte) uint32 { return binary.LittleEndian.Uint32(head[4:8]) },
	IsEOC:     isEOC,
}

// isEOC reports whether val ends a FAT32 cluster chain.
func isEOC(val uint32) bool { return val&0xFFFFFF8 == 0xFFFFFF8 }
