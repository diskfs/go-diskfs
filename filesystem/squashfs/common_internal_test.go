package squashfs

// adder compression
type testCompressorAddBytes struct {
	b   []byte
	err error
}

func (c *testCompressorAddBytes) compress(b []byte) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return b[:len(b)-len(c.b)], nil
}
func (c *testCompressorAddBytes) decompress(b []byte) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return append(b, c.b...), nil
}
func (c *testCompressorAddBytes) loadOptions(b []byte) error {
	return nil
}
func (c *testCompressorAddBytes) optionsBytes() []byte {
	return []byte{}
}
func (c *testCompressorAddBytes) flavour() compression {
	return compressionGzip
}

func testEqualUint32Slice(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func CompareEqualMapStringString(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		ov, ok := b[k]
		if !ok {
			return false
		}
		if ov != v {
			return false
		}
	}
	return true
}
