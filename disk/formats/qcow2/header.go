package qcow2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	headerMagic uint32 = 0x514649fb
)

const (
	header2Size             = 72
	header3MinSize          = 104
	preferredVersion uint32 = 3
)

type header struct {
	version           uint32
	backingFileOffset uint64
	backingFileSize   uint32
	clusterBits       uint32
	clusterSize       uint32
	fileSize          uint64
	encryptMethod     encryptionMethod
	l1Size            uint32
	l1Offset          uint64
	refCountOffset    uint64
	refcountBits      uint32
	refCountClusters  uint32
	refCountOrder     uint32
	snapshotsCount    uint32
	snapshotsOffset   uint64
	headerSize        uint32
	compressionType   compression
	// features in bytes 72-95, set via bit flags, for version 3 only
	dirty                  bool
	corrupt                bool
	externalData           bool
	nonStandardCompression bool
	extendedL2             bool
	lazyRefcounts          bool
	bitmapsExtension       bool
	rawExternalData        bool
	// extensions for v3
	extensions []headerExtension
}

func (h *header) toBytes() []byte {
	// we start by making a header as big as the ninimum v3 header
	// if it is v2, we will just send back the first 72 bytes
	b := make([]byte, 112)
	binary.BigEndian.PutUint32(b[0:4], headerMagic)
	binary.BigEndian.PutUint32(b[4:8], preferredVersion)
	binary.BigEndian.PutUint64(b[8:16], h.backingFileOffset)
	binary.BigEndian.PutUint32(b[16:20], h.backingFileSize)
	binary.BigEndian.PutUint32(b[20:24], h.clusterBits)
	binary.BigEndian.PutUint64(b[24:32], h.fileSize)
	binary.BigEndian.PutUint32(b[32:36], uint32(h.encryptMethod))
	binary.BigEndian.PutUint32(b[36:40], h.l1Size)
	binary.BigEndian.PutUint64(b[40:48], h.l1Offset)
	binary.BigEndian.PutUint64(b[48:56], h.refCountOffset)
	binary.BigEndian.PutUint32(b[56:60], h.refCountClusters)
	binary.BigEndian.PutUint32(b[60:64], h.snapshotsCount)
	binary.BigEndian.PutUint64(b[64:72], h.snapshotsOffset)
	if h.version == 2 {
		return b[:header2Size]
	}

	binary.BigEndian.PutUint32(b[96:100], h.refCountOrder)

	// read the various features
	var incompatibleFeatures, compatibleFeatures, autoclearFeatures uint64
	if h.dirty {
		incompatibleFeatures |= 0x80000000
	}
	if h.corrupt {
		incompatibleFeatures |= 0x40000000
	}
	if h.externalData {
		incompatibleFeatures |= 0x20000000
	}

	if h.nonStandardCompression {
		incompatibleFeatures |= 0x10000000
	}
	if h.extendedL2 {
		incompatibleFeatures |= 0x8000000
	}

	if h.lazyRefcounts {
		compatibleFeatures |= 0x80000000
	}

	if h.bitmapsExtension {
		autoclearFeatures |= 0x80000000
	}
	if h.rawExternalData {
		autoclearFeatures |= 0x40000000
	}

	binary.BigEndian.PutUint64(b[72:80], incompatibleFeatures)
	binary.BigEndian.PutUint64(b[80:88], compatibleFeatures)
	binary.BigEndian.PutUint64(b[88:96], autoclearFeatures)

	if h.nonStandardCompression {
		b[104] = byte(h.compressionType)
	}

	// now save the header size
	binary.BigEndian.PutUint32(b[100:104], uint32(len(b)))

	// add all of the header extensions
	for _, extension := range h.extensions {
		b = append(b, extension.toBytes()...)
	}

	return b
}

func (h *header) equal(o *header) bool {
	if o == nil {
		return false
	}
	if h.version != o.version || h.backingFileSize != o.backingFileSize || h.backingFileOffset != o.backingFileOffset ||
		h.clusterBits != o.clusterBits || h.clusterSize != o.clusterSize || h.fileSize != o.fileSize ||
		h.encryptMethod != o.encryptMethod || h.l1Size != o.l1Size || h.l1Offset != o.l1Offset ||
		h.refCountOffset != o.refCountOffset || h.refcountBits != o.refcountBits || h.refCountClusters != o.refCountClusters ||
		h.refCountOrder != o.refCountOrder || h.snapshotsCount != o.snapshotsCount || h.snapshotsOffset != o.snapshotsOffset ||
		h.headerSize != o.headerSize || h.compressionType != o.compressionType ||
		h.dirty != o.dirty || h.corrupt != o.corrupt || h.externalData != o.externalData || h.nonStandardCompression != o.nonStandardCompression ||
		h.extendedL2 != o.extendedL2 || h.lazyRefcounts != o.lazyRefcounts || h.bitmapsExtension != o.bitmapsExtension || h.rawExternalData != o.rawExternalData {
		return false
	}
	if len(h.extensions) != len(o.extensions) {
		return false
	}
	// extensions for v3
	for i, ext := range h.extensions {
		if !ext.equal(o.extensions[i]) {
			return false
		}
	}
	return true

}

func parseHeader(b []byte) (*header, error) {
	if len(b) < header2Size {
		return nil, fmt.Errorf("header had %d bytes instead of minimum %d", len(b), header2Size)
	}
	magic := binary.BigEndian.Uint32(b[0:4])
	if magic != headerMagic {
		return nil, fmt.Errorf("Header had magic of %d instead of expected %d", magic, headerMagic)
	}
	version := binary.BigEndian.Uint32(b[4:8])
	if version != 2 && version != 3 {
		return nil, fmt.Errorf("version number %d incompatible, supporting only versions %v", version, []int{2, 3})
	}
	clusterBits := binary.BigEndian.Uint32(b[20:24])
	h := &header{
		version:           version,
		backingFileOffset: binary.BigEndian.Uint64(b[8:16]),
		backingFileSize:   binary.BigEndian.Uint32(b[16:20]),
		clusterSize:       2 << (clusterBits - 1),
		clusterBits:       clusterBits,
		fileSize:          binary.BigEndian.Uint64(b[24:32]),
		encryptMethod:     encryptionMethod(binary.BigEndian.Uint32(b[32:36])),
		l1Size:            binary.BigEndian.Uint32(b[36:40]),
		l1Offset:          binary.BigEndian.Uint64(b[40:48]),
		refCountOffset:    binary.BigEndian.Uint64(b[48:56]),
		refCountClusters:  binary.BigEndian.Uint32(b[56:60]),
		refcountBits:      16,
		refCountOrder:     4,
		snapshotsCount:    binary.BigEndian.Uint32(b[60:64]),
		snapshotsOffset:   binary.BigEndian.Uint64(b[64:72]),
		compressionType:   compressionZlib, // default is zlib, to be adjusted later if v3 and has flag
	}
	// version 2 header has exactly 72 bytes
	if version == 2 || len(b) < header3MinSize {
		h.headerSize = 72
		return h, nil
	}

	// if we are here, version is 3 and we have more bytes
	// read the header size, and make sure we have enough bytes to read the whole thing
	h.headerSize = binary.BigEndian.Uint32(b[100:104])
	if len(b) < int(h.headerSize) {
		return h, nil
	}

	// we have enough bytes, so parse the rest of the header
	h.refCountOrder = binary.BigEndian.Uint32(b[96:100])

	// read the various features
	incompatibleFeatures := binary.BigEndian.Uint64(b[72:80])
	h.dirty = incompatibleFeatures&0x80000000 != 0
	h.corrupt = incompatibleFeatures&0x40000000 != 0
	h.externalData = incompatibleFeatures&0x20000000 != 0
	h.nonStandardCompression = incompatibleFeatures&0x10000000 != 0
	h.extendedL2 = incompatibleFeatures&0x8000000 != 0

	compatibleFeatures := binary.BigEndian.Uint64(b[80:88])
	h.lazyRefcounts = compatibleFeatures&0x80000000 != 0

	autoclearFeatures := binary.BigEndian.Uint64(b[88:96])
	h.bitmapsExtension = autoclearFeatures&0x80000000 != 0
	h.rawExternalData = autoclearFeatures&0x40000000 != 0

	if h.nonStandardCompression {
		if h.headerSize < 105 {
			return nil, errors.New("non-standard compression but header size must be >= 105 to have compression type flag")
		}
		h.compressionType = compression(b[104])
	}
	// pad to end of header
	extensionsStart := int(h.headerSize)
	padding := 8 - extensionsStart%8
	if padding == 8 {
		padding = 0
	}
	extensionsStart += padding

	// extensions
	// there is a minimum size of 8 to every header
	i := extensionsStart
	for {
		extension, err := parseHeaderExtension(b[i:])
		if err != nil {
			return nil, fmt.Errorf("header extension at %d: %v", i, err)
		}
		if _, ok := extension.(headerExtensionEnd); ok {
			break
		}
		i += extension.size()
		h.extensions = append(h.extensions, extension)
	}

	// for version 2, where refcount order is not explicit, refcount order is 4, i.e. bits = 16
	if h.refCountOrder == 0 {
		h.refCountOrder = 4
	}
	h.refcountBits = 1 << h.refCountOrder

	return h, nil
}

type headerExtension interface {
	size() int
	toBytes() []byte
	equal(o headerExtension) bool
}

type headerExtensionEnd struct{}

func (h headerExtensionEnd) size() int {
	return 8
}
func (h headerExtensionEnd) toBytes() []byte {
	return make([]byte, 8)
}
func (h headerExtensionEnd) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionEnd)
	if !ok {
		return false
	}
	return h == oe
}

type headerExtensionBackingFileFormatName struct {
	name string
}

func (h headerExtensionBackingFileFormatName) size() int {
	return 8 + len(h.name)
}
func (h headerExtensionBackingFileFormatName) toBytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], 0xe2792aca)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(h.name)))
	b = append(b, []byte(h.name)...)
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h headerExtensionBackingFileFormatName) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionBackingFileFormatName)
	if !ok {
		return false
	}
	return h == oe
}

type featureType byte

const (
	featureIncompatible featureType = 0
	featureCompatible   featureType = 1
	featureAutoclear    featureType = 2
)

type featureName struct {
	featureType featureType
	bitNumber   uint8
	name        string
}

func (f featureName) toBytes() []byte {
	var b []byte
	b = append(b, byte(f.featureType))
	b = append(b, byte(f.bitNumber))
	b = append(b, []byte(f.name)...)
	b = append(b, zeropad(len(b), 48)...)
	return b
}

type headerExtensionFeatureNameTable []featureName

func (h headerExtensionFeatureNameTable) size() int {
	return 8 + len(h)*48
}
func (h headerExtensionFeatureNameTable) toBytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], 0x6803f857)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(h)*48))
	for _, name := range h {
		b = append(b, name.toBytes()...)
	}
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h headerExtensionFeatureNameTable) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionFeatureNameTable)
	if !ok {
		return false
	}
	if len(h) != len(oe) {
		return false
	}
	for i, e := range h {
		if e != oe[i] {
			return false
		}
	}
	return true
}

type headerExtensionBitmaps struct {
	count     uint32
	dirSize   uint64
	dirOffset uint64
}

func (h headerExtensionBitmaps) size() int {
	return 8 + 24
}
func (h headerExtensionBitmaps) toBytes() []byte {
	b := make([]byte, 8+24)
	binary.BigEndian.PutUint32(b[0:4], 0x23852875)
	binary.BigEndian.PutUint32(b[4:8], 24)
	binary.BigEndian.PutUint32(b[8:12], h.count)
	binary.BigEndian.PutUint64(b[16:24], h.dirSize)
	binary.BigEndian.PutUint64(b[24:32], h.dirOffset)
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h headerExtensionBitmaps) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionBitmaps)
	if !ok {
		return false
	}
	return h == oe
}

type headerExtensionFullDiskEncryption struct {
	offset uint64
	length uint64
	header []byte
}

func (h headerExtensionFullDiskEncryption) size() int {
	return 8 + 16
}
func (h headerExtensionFullDiskEncryption) toBytes() []byte {
	b := make([]byte, 8+16)
	binary.BigEndian.PutUint32(b[0:4], 0x0537be77)
	binary.BigEndian.PutUint32(b[4:8], 16)
	binary.BigEndian.PutUint64(b[8:16], h.offset)
	binary.BigEndian.PutUint64(b[16:24], h.length)
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h *headerExtensionFullDiskEncryption) parseEncryptionHeader(b []byte) error {
	h.header = make([]byte, len(b))
	copy(h.header, b)
	// TODO: process the luks header
	return nil
}
func (h headerExtensionFullDiskEncryption) encryptionHeaderToBytes() []byte {
	b := make([]byte, len(h.header))
	copy(b, h.header)
	return b
}
func (h headerExtensionFullDiskEncryption) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionFullDiskEncryption)
	if !ok {
		return false
	}
	return h.offset == oe.offset && h.length == oe.length && bytes.Equal(h.header, oe.header)
}

type headerExtensionExternalDataFilename struct {
	name string
}

func (h headerExtensionExternalDataFilename) size() int {
	return 8 + len(h.name)
}
func (h headerExtensionExternalDataFilename) toBytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], 0x44415441)
	binary.BigEndian.PutUint32(b[4:8], uint32(len(h.name)))
	b = append(b, []byte(h.name)...)
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h headerExtensionExternalDataFilename) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionExternalDataFilename)
	if !ok {
		return false
	}
	return h == oe
}

type headerExtensionUnknown struct {
	length int
}

func (h headerExtensionUnknown) size() int {
	return 8 + h.length
}
func (h headerExtensionUnknown) toBytes() []byte {
	b := make([]byte, 8+h.length)
	binary.BigEndian.PutUint32(b[0:4], 0x44415441)
	binary.BigEndian.PutUint32(b[4:8], uint32(h.length))
	b = append(b, zeropad(len(b), 8)...)
	return b
}
func (h headerExtensionUnknown) equal(o headerExtension) bool {
	if o == nil {
		return false
	}
	oe, ok := o.(headerExtensionUnknown)
	if !ok {
		return false
	}
	return h == oe
}

func parseHeaderExtension(b []byte) (headerExtension, error) {
	var he headerExtension
	extType := binary.BigEndian.Uint32(b[:4])
	extLength := int(binary.BigEndian.Uint32(b[4:8]))
	if 8+extLength > len(b) {
		return nil, fmt.Errorf("header extension had length of %d but insufficient bytes %d to read", extLength, len(b))
	}
	switch extType {
	case 0x00000000:
		he = headerExtensionEnd{}
	case 0xe2792aca:
		he = headerExtensionBackingFileFormatName{name: string(b[8 : 8+extLength])}
	case 0x6803f857:
		var features []featureName
		// always need exactly 48 bytes
		for i := 8; i < 8+extLength; i += 48 {
			var name []byte
			for j := i + 2; j < i+48; j++ {
				if b[j] == 0 {
					break
				}
				name = append(name, b[j])
			}
			features = append(features, featureName{
				featureType: featureType(b[i]),
				bitNumber:   b[i+1],
				name:        string(name),
			})
		}
		he = headerExtensionFeatureNameTable(features)
	case 0x23852875:
		if len(b) < 8+24 {
			return nil, fmt.Errorf("bitmaps extension header had %d bytes, less than minimum %d", len(b), 8+24)
		}
		he = headerExtensionBitmaps{
			count:     binary.BigEndian.Uint32(b[8:12]),
			dirSize:   binary.BigEndian.Uint64(b[16:24]),
			dirOffset: binary.BigEndian.Uint64(b[24:32]),
		}
	case 0x0537be77:
		he = headerExtensionFullDiskEncryption{}
	case 0x44415441:
		he = headerExtensionExternalDataFilename{name: string(b[8 : 8+extLength])}
	default:
		// unknown header extension, ignore it
		he = headerExtensionUnknown{length: extLength}
	}
	return he, nil
}
