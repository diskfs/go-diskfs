package erofs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	iofs "io/fs"
)

type directoryEntry struct {
	nid      uint64
	name     string
	fileType uint8
}

func parseDirectory(b []byte) ([]*directoryEntry, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("directory block is empty")
	}
	if len(b)%direntSize != 0 && len(b) < direntSize {
		return nil, fmt.Errorf("directory block is %d bytes, less than entry size %d", len(b), direntSize)
	}
	firstNameOffset := int(binary.LittleEndian.Uint16(b[8:10]))
	if firstNameOffset < direntSize || firstNameOffset >= len(b) || firstNameOffset%direntSize != 0 {
		return nil, fmt.Errorf("invalid first directory name offset %d", firstNameOffset)
	}
	entryCount := firstNameOffset / direntSize
	if entryCount == 0 {
		return nil, fmt.Errorf("directory block has no entries")
	}

	entries := make([]*directoryEntry, 0, entryCount)
	previousName := ""
	for index := 0; index < entryCount; index++ {
		offset := index * direntSize
		entry := b[offset : offset+direntSize]
		nameOffset := int(binary.LittleEndian.Uint16(entry[8:10]))
		nextNameOffset := len(b)
		if index+1 < entryCount {
			nextNameOffset = int(binary.LittleEndian.Uint16(b[offset+direntSize+8 : offset+direntSize+10]))
		}
		if nameOffset < firstNameOffset || nameOffset >= len(b) || nameOffset >= nextNameOffset {
			return nil, fmt.Errorf("directory entry %d has invalid name offset %d", index, nameOffset)
		}
		if entry[11] != 0 {
			return nil, fmt.Errorf("directory entry %d has nonzero reserved byte", index)
		}
		if entry[10] == 0 || entry[10] > fileTypeSymlink {
			return nil, fmt.Errorf("directory entry %d has invalid file type %d", index, entry[10])
		}
		nameBytes := b[nameOffset:nextNameOffset]
		if len(nameBytes) == 0 || len(nameBytes) > maxNameLength || bytes.IndexByte(nameBytes, 0) >= 0 {
			return nil, fmt.Errorf("directory entry %d has invalid name length %d", index, len(nameBytes))
		}
		name := string(nameBytes)
		if index > 0 && string(nameBytes) <= previousName {
			return nil, fmt.Errorf("directory names are not strictly ordered: %q after %q", name, previousName)
		}
		nid := binary.LittleEndian.Uint64(entry[0:8])
		if nid>>63 != 0 {
			return nil, fmt.Errorf("directory entry %d has metabox inode number", index)
		}
		entries = append(entries, &directoryEntry{nid: nid, name: name, fileType: entry[10]})
		previousName = name
	}
	return entries, nil
}

func (f *FileSystem) readDirectory(i *inode) ([]*directoryEntry, error) {
	if !i.isDir() {
		return nil, fmt.Errorf("inode %d is not a directory", i.nid)
	}
	var entries []*directoryEntry
	for offset := int64(0); offset < i.size; offset += f.blockSize() {
		length := f.blockSize()
		if remaining := i.size - offset; remaining < length {
			length = remaining
		}
		b := make([]byte, length)
		if _, err := f.readInodeData(i, b, offset); err != nil {
			return nil, err
		}
		blockEntries, err := parseDirectory(b)
		if err != nil {
			return nil, fmt.Errorf("invalid directory block at inode %d offset %d: %w", i.nid, offset, err)
		}
		entries = append(entries, blockEntries...)
	}
	return entries, nil
}

func (f *FileSystem) lookup(parent *inode, name string) (*inode, error) {
	entries, err := f.readDirectory(parent)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.name == name {
			return f.getInode(entry.nid)
		}
	}
	return nil, iofs.ErrNotExist
}
