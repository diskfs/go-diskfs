package ext4

import (
	"encoding/binary"
	"fmt"
)

// Extended Attributes (xattrs) implementation.
//
// References:
// - Kernel documentation: https://www.kernel.org/doc/html/latest/filesystems/ext4/dynamic.html#extended-attributes
// - Legacy wiki: https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Extended_Attributes
// - Kernel source (structures): https://github.com/torvalds/linux/blob/master/fs/ext4/xattr.h
// - Kernel source (implementation): https://github.com/torvalds/linux/blob/master/fs/ext4/xattr.c
// - e2fsprogs implementation: https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/lib/ext2fs/ext_attr.c

const (
	// xattrMagic is the magic number identifying extended attribute blocks.
	// See ext4_xattr_header in fs/ext4/xattr.h
	xattrMagic = 0xEA020000

	// xattrHeaderSize is the size of ext4_xattr_header (32 bytes).
	xattrHeaderSize = 32

	// xattrEntrySize is the fixed size of ext4_xattr_entry before the variable-length name.
	// See struct ext4_xattr_entry in fs/ext4/xattr.h
	xattrEntrySize = 16

	// Attribute name index values from fs/ext4/xattr.h
	xattrIndexUser            = 1 // EXT4_XATTR_INDEX_USER
	xattrIndexPosixACLAccess  = 2 // EXT4_XATTR_INDEX_POSIX_ACL_ACCESS
	xattrIndexPosixACLDefault = 3 // EXT4_XATTR_INDEX_POSIX_ACL_DEFAULT
	xattrIndexTrusted         = 4 // EXT4_XATTR_INDEX_TRUSTED
	xattrIndexSecurity        = 6 // EXT4_XATTR_INDEX_SECURITY
	xattrIndexSystem          = 7 // EXT4_XATTR_INDEX_SYSTEM
)

// xattrPrefixes maps name index values to their corresponding key prefixes.
// This reduces on-disk space consumption by storing only the index instead of
// the full prefix string.
//
// POSIX ACL entries have no trailing dot because their e_name_len is always 0;
// the full attribute name is exactly the prefix (e.g. "system.posix_acl_access").
//
// See ext4_xattr_prefix_type in fs/ext4/xattr.h
var xattrPrefixes = map[uint8]string{
	0:                         "",
	xattrIndexUser:            "user.",
	xattrIndexPosixACLAccess:  "system.posix_acl_access",
	xattrIndexPosixACLDefault: "system.posix_acl_default",
	xattrIndexTrusted:         "trusted.",
	xattrIndexSecurity:        "security.",
	xattrIndexSystem:          "system.",
}

// parseXattrEntries parses extended attribute entries from a byte slice.
//
// entries contains the ext4_xattr_entry structures; values is the region from which
// e_value_offs is relative. For inline (ibody) xattrs, both point to the same region.
// For block xattrs, entries points to the region after the header, and values points
// to the entire block (including header).
//
// The on-disk format is defined in struct ext4_xattr_entry in fs/ext4/xattr.h:
//
//	struct ext4_xattr_entry {
//	    __u8 e_name_len;      /* length of name */
//	    __u8 e_name_index;    /* attribute name index */
//	    __le16 e_value_offs;  /* offset in disk block of value */
//	    __le32 e_value_inum;  /* inode in which the value is stored */
//	    __le32 e_value_size;  /* size of attribute value */
//	    __le32 e_hash;        /* hash value of name and value */
//	    char e_name[];        /* attribute name */
//	};
func parseXattrEntries(entries, values []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)
	pos := 0
	for pos+xattrEntrySize <= len(entries) {
		nameLen := entries[pos]
		nameIndex := entries[pos+1]
		// The entry list is terminated by a zero-filled entry (e_name_len == 0
		// and e_name_index == 0). See EXT4_IS_LAST_ENTRY in fs/ext4/xattr.h.
		// POSIX ACL entries have e_name_len == 0 but e_name_index != 0.
		if nameLen == 0 && nameIndex == 0 {
			break
		}
		valueOffs := binary.LittleEndian.Uint16(entries[pos+2 : pos+4])
		valueInum := binary.LittleEndian.Uint32(entries[pos+4 : pos+8])
		valueSize := binary.LittleEndian.Uint32(entries[pos+8 : pos+12])

		nameStart := pos + xattrEntrySize
		nameEnd := nameStart + int(nameLen)
		if nameEnd > len(entries) {
			return nil, fmt.Errorf("xattr entry name extends past buffer")
		}

		prefix, ok := xattrPrefixes[nameIndex]
		if !ok {
			prefix = fmt.Sprintf("unknown_%d.", nameIndex)
		}
		fullName := prefix + string(entries[nameStart:nameEnd])

		if valueInum != 0 {
			return nil, fmt.Errorf("xattr %q: ea_inode values not supported", fullName)
		}
		if valueSize > 0 {
			vStart := int(valueOffs)
			vEnd := vStart + int(valueSize)
			if vEnd > len(values) {
				return nil, fmt.Errorf("xattr value for %q extends past buffer", fullName)
			}
			val := make([]byte, valueSize)
			copy(val, values[vStart:vEnd])
			result[fullName] = val
		}

		// Advance to next entry, aligned to 4 bytes.
		pos = (nameEnd + 3) &^ 3
	}
	return result, nil
}
