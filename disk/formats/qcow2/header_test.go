package qcow2

import (
	"bytes"
	"os"
	"testing"
)

const (
	qcowFile = "./testdata/file.qcow2"
)

// GetValidHeader returns a full-length valid v3 header.
// To use for short v3 (first 72 bytes) or v2, wrap this and modify.
func GetValidHeader() *header {
	h := header{
		version:           3,
		backingFileOffset: 0,
		backingFileSize:   0,
		clusterBits:       0x10,
		clusterSize:       65536,
		fileSize:          0xa00000,
		encryptMethod:     encryptionMethodNone,
		l1Size:            0x01,
		l1Offset:          0x030000,
		refCountOffset:    0x010000,
		refcountBits:      16,
		refCountClusters:  0x01,
		refCountOrder:     0x04,
		snapshotsCount:    0x0,
		snapshotsOffset:   0x0,
		headerSize:        0x70,
		compressionType:   compressionZlib,
		// features in bytes 72-95, set via bit flags
		dirty:                  false,
		corrupt:                false,
		externalData:           false,
		nonStandardCompression: false,
		extendedL2:             false,
		lazyRefcounts:          false,
		bitmapsExtension:       false,
		rawExternalData:        false,
		// extensions for v3
		// TODO: we actually *do* have a feature name table here
		extensions: []headerExtension{
			headerExtensionFeatureNameTable([]featureName{
				{featureType: featureIncompatible, bitNumber: 0, name: "dirty bit"},
				{featureType: featureIncompatible, bitNumber: 1, name: "corrupt bit"},
				{featureType: featureIncompatible, bitNumber: 2, name: "external data file"},
				{featureType: featureIncompatible, bitNumber: 3, name: "compression type"},
				{featureType: featureIncompatible, bitNumber: 4, name: "extended L2 entries"},
				{featureType: featureCompatible, bitNumber: 0, name: "lazy refcounts"},
				{featureType: featureAutoclear, bitNumber: 0, name: "bitmaps"},
				{featureType: featureAutoclear, bitNumber: 1, name: "raw external data"},
			}),
		},
	}
	return &h
}

func GetValidHeader2() *header {
	// check out data
	h := GetValidHeader()
	h.version = 2
	h.headerSize = 0x48
	h.extensions = nil
	return h
}

func GetValidHeader3() *header {
	return GetValidHeader()
}

func GetValidHeader3Short() *header {
	h := GetValidHeader()
	h.headerSize = 0x48
	h.extensions = nil
	return h
}

func GetValidHeaderBytes() ([]byte, error) {
	f, err := os.Open(qcowFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// reading 8K is more than enough
	// the header is 0x70
	// the sole extension is 0x180 length
	// each extension starts with 4 bytes of type
	//    and 4 bytes of length
	// and then the end extension of 8 bytes
	// so total size:
	size := 0x70 + (0x4 + 0x4 + 0x180 + 8)
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil {
		return nil, err
	}
	return b, nil
}

func TestParseHeader(t *testing.T) {
	f, err := os.Open(qcowFile)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer f.Close()
	// reading 8K is more than enough
	b := make([]byte, 8192)
	if _, err := f.ReadAt(b, 0); err != nil {
		t.Fatalf("%v", err)
	}
	t.Run("72 bytes", func(t *testing.T) {
		valid := GetValidHeader3Short()
		h, err := parseHeader(b[:72])
		if err != nil {
			t.Fatalf("%v", err)
		}
		if h == nil {
			t.Fatalf("header was unexpectedly nil")
		}
		if !h.equal(valid) {
			t.Errorf("mismatched headers")
			t.Logf("actual: %#v", h)
			t.Logf("valid: %#v", valid)
		}
	})
	t.Run("full header", func(t *testing.T) {
		valid := GetValidHeader3()
		h, err := parseHeader(b)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if h == nil {
			t.Fatalf("header was unexpectedly nil")
		}
		if !h.equal(valid) {
			t.Errorf("mismatched headers")
			t.Logf("actual: %#v", h)
			t.Logf("valid: %#v", valid)
		}
	})
}

func TestHeaderToBytes(t *testing.T) {
	header := GetValidHeader3()
	valid, err := GetValidHeaderBytes()
	if err != nil {
		t.Fatal(err)
	}
	b := header.toBytes()
	if !bytes.Equal(b, valid) {
		t.Error("mismatched bytes")
		t.Logf("actual  : % x", b)
		t.Logf("expected: % x", valid)
	}
}
