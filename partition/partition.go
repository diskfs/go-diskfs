// Package partition provides ability to work with individual partitions.
// All useful implementations are subpackages of this package, e.g. github.com/diskfs/go-diskfs/gpt
package partition

import (
	"fmt"

	"github.com/diskfs/go-diskfs/backend"
	"github.com/diskfs/go-diskfs/partition/gpt"
	"github.com/diskfs/go-diskfs/partition/mbr"
)

// GPTPolicy controls how ReadWithPolicy resolves a disk where both GPT and MBR can be read.
type GPTPolicy int

const (
	// GPTWithMBR prefers a valid conventional MBR. When GPT is valid, it returns
	// GPT if MBR is absent, protective, or hybrid. If GPT is invalid, it falls
	// back to a valid MBR. This is the default.
	GPTWithMBR GPTPolicy = iota
	// GPTIgnoreMBR returns GPT whenever GPT can be read, regardless of MBR
	// contents. If GPT is invalid, it falls back to a valid MBR.
	GPTIgnoreMBR
)

// Read reads a partition table from a disk using the GPTWithMBR policy.
func Read(f backend.File, logicalBlocksize, physicalBlocksize int) (Table, error) {
	return ReadWithPolicy(f, logicalBlocksize, physicalBlocksize, GPTWithMBR)
}

// ReadWithPolicy reads a partition table from a disk using the requested GPT policy.
func ReadWithPolicy(f backend.File, logicalBlocksize, physicalBlocksize int, policy GPTPolicy) (Table, error) {
	if policy != GPTWithMBR && policy != GPTIgnoreMBR {
		return nil, fmt.Errorf("unknown GPT policy %d", policy)
	}

	gptTable, gptErr := gpt.Read(f, logicalBlocksize, physicalBlocksize)
	if gptErr == nil && policy == GPTIgnoreMBR {
		return gptTable, nil
	}

	mbrTable, mbrErr := mbr.Read(f, logicalBlocksize, physicalBlocksize)
	switch {
	case gptErr != nil && mbrErr == nil:
		return mbrTable, nil
	case gptErr != nil:
		return nil, fmt.Errorf("unknown disk partition type")
	case mbrErr != nil:
		return gptTable, nil
	case hasGPTProtectiveEntry(mbrTable):
		return gptTable, nil
	default:
		return mbrTable, nil
	}
}

// hasGPTProtectiveEntry reports whether MBR contains the entry that marks a
// protective or hybrid MBR. Its size is intentionally not checked because a
// disk image can be copied to a larger device without updating that value.
func hasGPTProtectiveEntry(table *mbr.Table) bool {
	for _, partition := range table.Partitions {
		if partition != nil && partition.Type == mbr.GPTProtective && partition.Start == 1 {
			return true
		}
	}
	return false
}
