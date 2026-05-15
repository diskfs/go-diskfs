package iso9660

// StatT is the ISO9660-specific metadata returned by directoryEntry.Sys().
// Rock Ridge fields are zero-valued when no Rock Ridge extensions are present.
type StatT struct {
	// ExtAttrSize is the size of the extended attribute record in bytes.
	ExtAttrSize uint8
	// Location is the extent LBA where the file's data begins.
	Location uint32
	// VolumeSequence is the volume sequence number.
	VolumeSequence uint16
	// IsHidden reports whether the hidden flag is set.
	IsHidden bool
	// IsAssociated reports whether the associated file flag is set.
	IsAssociated bool
	// HasExtendedAttrs reports whether the extended-attributes flag is set.
	HasExtendedAttrs bool
	// HasOwnerGroupPermissions reports whether owner/group permissions are present.
	HasOwnerGroupPermissions bool

	// UID is the owner user ID from a Rock Ridge PX record.
	UID uint32
	// GID is the owner group ID from a Rock Ridge PX record.
	GID uint32
	// NLink is the hard-link count from a Rock Ridge PX record.
	NLink uint32
	// Inode is the inode/serial number from a Rock Ridge PX record.
	Inode uint32
	// LinkTarget is the symbolic-link target from Rock Ridge SL records, only set when not continued.
	LinkTarget string
	// RockRidge is true if any Rock Ridge extension was found on the entry.
	RockRidge bool
}
