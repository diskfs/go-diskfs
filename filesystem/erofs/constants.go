package erofs

const (
	superMagic            uint32 = 0xE0F5E1E2
	superOffset                  = 1024
	superblockBaseSize           = 128
	superblockExtSlotSize        = 16
	inodeSlotSize                = 32
	inodeCompactSize             = 32
	inodeExtendedSize            = 64
	direntSize                   = 12
	maxNameLength                = 255
)

const (
	featureCompatChecksum uint32 = 1 << 0
	featureCompatMtime    uint32 = 1 << 1
	featureCompatXattr    uint32 = 1 << 2

	featureIncompatLZ4Padding uint32 = 1 << 0
	featureIncompatComprCfgs  uint32 = 1 << 1
	featureIncompatFragments  uint32 = 1 << 5
)

const (
	dataLayoutFlatPlain uint8 = iota
	dataLayoutCompressedFull
	dataLayoutFlatInline
	dataLayoutCompressedCompact
	dataLayoutChunked
)

const inodeFormatReservedMask uint16 = 0xffe0

const (
	zAdviseCompacted2B  uint16 = 1 << 0
	zAdviseBigPcluster1 uint16 = 1 << 1
	zAdviseBigPcluster2 uint16 = 1 << 2
	zAdviseInlineData   uint16 = 1 << 3
	zAdviseInterlaced   uint16 = 1 << 4
	zAdviseFragment     uint16 = 1 << 5
)

const (
	zClusterTypePlain uint8 = iota
	zClusterTypeHead1
	zClusterTypeNonHead
	zClusterTypeHead2
)

const (
	zD0ClusterCount uint16 = 1 << 11
	zPartialRef     uint16 = 1 << 15
)

const (
	compressionLZ4 uint8 = iota
	compressionLZMA
	compressionDeflate
	compressionZstd
)

const (
	compressionShifted    uint8 = 4
	compressionInterlaced uint8 = 5
)
