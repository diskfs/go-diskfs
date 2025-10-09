package fat32

import (
	"encoding/binary"
	"slices"
)

// table a FAT32 table
type table struct {
	fatID          uint32
	eocMarker      uint32
	unusedMarker   uint32
	clusters       []uint32
	rootDirCluster uint32
	size           uint32
	maxCluster     uint32
}

func (t *table) equal(a *table) bool {
	if (t == nil && a != nil) || (t != nil && a == nil) {
		return false
	}
	if t == nil && a == nil {
		return true
	}
	return t.fatID == a.fatID &&
		t.eocMarker == a.eocMarker &&
		t.rootDirCluster == a.rootDirCluster &&
		t.size == a.size &&
		t.maxCluster == a.maxCluster &&
		slices.Equal(a.clusters, t.clusters)
}

func tableFromBytes(b []byte, fatType int) *table {
	switch fatType {
	case 12:
		return tableFromBytes12(b)
	case 16:
		return tableFromBytes16(b)
	default:
		return tableFromBytes32(b)
	}
}

/*
when reading from disk, remember that *any* of the following is a valid eocMarker:
0x?ffffff8 - 0x?fffffff
*/
func tableFromBytes32(b []byte) *table {
	maxCluster := uint32(len(b) / 4)

	t := table{
		fatID:          binary.LittleEndian.Uint32(b[0:4]),
		eocMarker:      binary.LittleEndian.Uint32(b[4:8]),
		size:           uint32(len(b)),
		clusters:       make([]uint32, maxCluster+1),
		maxCluster:     maxCluster,
		rootDirCluster: 2, // always 2 for FAT32
	}
	// just need to map the clusters in
	for i := uint32(2); i < t.maxCluster; i++ {
		bStart := i * 4
		bEnd := bStart + 4
		val := binary.LittleEndian.Uint32(b[bStart:bEnd])
		// 0 indicates an empty cluster, so we can ignore
		if val != 0 {
			t.clusters[i] = val
		}
	}
	return &t
}

func tableFromBytes16(b []byte) *table {
	maxCluster := uint32(len(b) / 2)

	t := table{
		fatID:      uint32(binary.LittleEndian.Uint16(b[0:2])),
		eocMarker:  uint32(binary.LittleEndian.Uint16(b[2:4])),
		size:       uint32(len(b)),
		clusters:   make([]uint32, maxCluster+1),
		maxCluster: maxCluster,
		// for fat16 the root dir is stored in a separate location
		rootDirCluster: 0,
	}

	// Parse clusters starting from 2
	for i := uint32(2); i < t.maxCluster; i++ {
		bStart := i * 2
		bEnd := bStart + 2
		val := uint32(binary.LittleEndian.Uint16(b[bStart:bEnd]))

		// 0 indicates an empty cluster, so we can ignore
		if val != 0 {
			t.clusters[i] = val
		}
	}
	return &t
}

func tableFromBytes12(b []byte) *table {
	maxCluster := uint32(len(b) * 2 / 3) // 1.5 bytes (12 bits) per entry

	t := table{
		fatID:      uint32(getFAT12Entry(b, 0)),
		eocMarker:  uint32(getFAT12Entry(b, 1)),
		size:       uint32(len(b)),
		clusters:   make([]uint32, maxCluster+1),
		maxCluster: maxCluster,
		// for fat12 the root dir is stored in a separate location
		rootDirCluster: 0,
	}

	// Parse clusters starting from 2
	for i := uint32(2); i < t.maxCluster; i++ {
		val := uint32(getFAT12Entry(b, i))

		// 0 indicates an empty cluster, so we can ignore
		if val != 0 {
			t.clusters[i] = val
		}
	}
	return &t
}

func getFAT12Entry(b []byte, cluster uint32) uint16 {
	bytePos := (cluster * 3) / 2

	if cluster%2 == 0 {
		// even cluster numbers take 12 bits: 8 from first byte and 4 from second byte
		if bytePos+1 >= uint32(len(b)) {
			return 0
		}
		return uint16(b[bytePos]) | ((uint16(b[bytePos+1]) & 0x0F) << 8)
	} else {
		// odd cluster numbers take 12 bits: 4 from first byte and 8 from second byte
		if bytePos+1 >= uint32(len(b)) {
			return 0
		}
		return uint16(b[bytePos]>>4) | (uint16(b[bytePos+1]) << 4)
	}
}

// bytes returns a FAT32 table as bytes ready to be written to disk
func (t *table) bytes(fatType int) []byte {
	switch fatType {
	case 12:
		return t.bytes12()
	case 16:
		return t.bytes16()
	default:
		return t.bytes32()
	}
}

func (t *table) bytes32() []byte {
	b := make([]byte, t.size)

	// FAT ID and fixed values
	binary.LittleEndian.PutUint32(b[0:4], t.fatID)
	// End-of-Cluster marker
	binary.LittleEndian.PutUint32(b[4:8], t.eocMarker)
	// now just clusters
	numClusters := t.maxCluster
	for i := uint32(2); i < numClusters; i++ {
		bStart := i * 4
		bEnd := bStart + 4
		val := t.clusters[i]
		binary.LittleEndian.PutUint32(b[bStart:bEnd], val)
	}

	return b
}

func (t *table) bytes16() []byte {
	b := make([]byte, t.size)

	// FAT ID and fixed values
	binary.LittleEndian.PutUint16(b[0:2], uint16(t.fatID))
	// End-of-Cluster marker
	binary.LittleEndian.PutUint16(b[2:4], uint16(t.eocMarker))
	// now just clusters
	numClusters := t.maxCluster
	for i := uint32(2); i < numClusters; i++ {
		bStart := i * 2
		bEnd := bStart + 2
		val := t.clusters[i]
		binary.LittleEndian.PutUint16(b[bStart:bEnd], uint16(val))
	}

	return b
}

func (t *table) bytes12() []byte {
	b := make([]byte, t.size)

	// Write FAT ID and EOC marker using helper function
	setFat12Entry(b, 0, uint16(t.fatID))
	setFat12Entry(b, 1, uint16(t.eocMarker))

	// now just clusters
	numClusters := t.maxCluster
	for i := uint32(2); i < numClusters; i++ {
		setFat12Entry(b, i, uint16(t.clusters[i]))
	}

	return b
}

func setFat12Entry(b []byte, cluster uint32, value uint16) {
	bytePos := (cluster * 3) / 2

	if cluster%2 == 0 {
		// Even cluster numbers: 8 bits to first byte and 4 bits to second byte
		if bytePos+1 >= uint32(len(b)) {
			return
		}
		b[bytePos] = byte(value & 0xFF)
		b[bytePos+1] = (b[bytePos+1] & 0xF0) | byte((value>>8)&0x0F)
	} else {
		// Odd cluster numbers: 4 bits to first byte and 8 bits to second byte
		if bytePos+1 >= uint32(len(b)) {
			return
		}
		b[bytePos] = (b[bytePos] & 0x0F) | byte((value&0x0F)<<4)
		b[bytePos+1] = byte(value >> 4)
	}
}

// http://elm-chan.org/docs/fat_e.html#file_cluster
func (t *table) isEoc(cluster uint32, fatType int) bool {
	switch fatType {
	case 12:
		return cluster&0xFF8 == 0xFF8
	case 16:
		return cluster&0xFFF8 == 0xFFF8
	default:
		return cluster&0xFFFFFF8 == 0xFFFFFF8
	}
}
