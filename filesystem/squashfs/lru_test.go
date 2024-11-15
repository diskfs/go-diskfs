package squashfs

import (
	"errors"
	"strings"
	"testing"
)

//nolint:gocyclo // we really do not care about the cyclomatic complexity of a test function. Maybe someday we will improve it.
func TestLRU(t *testing.T) {
	const maxBlocks = 10
	l := newLRU(maxBlocks)

	assertEmpty := func(want bool) {
		t.Helper()
		got := l.root.prev == &l.root && l.root.next == &l.root
		if want != got {
			t.Errorf("Wanted empty %v but got %v", want, got)
		}
	}

	assertClear := func(block *lruBlock, want bool) {
		t.Helper()
		got := block.next == nil && block.prev == nil
		if want != got {
			t.Errorf("Wanted block clear %v but block clear %v", want, got)
		}
	}

	assertNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Errorf("Expected no error but got: %v", err)
		}
	}

	assertCacheBlocks := func(want int) {
		if len(l.cache) != want {
			t.Errorf("Expected len(l.cache)=%d but got %d", want, len(l.cache))
		}
	}

	t.Run("Simple", func(t *testing.T) {
		assertEmpty(true)
		block := &lruBlock{
			pos: 1,
		}
		assertClear(block, true)
		l.push(block)
		assertClear(block, false)
		assertEmpty(false)
		block2 := l.pop()
		if block.pos != block2.pos {
			t.Errorf("Wanted block %d but got %d", block.pos, block2.pos)
		}
		assertClear(block, true)
		assertClear(block2, true)
		assertEmpty(true)
	})

	//nolint:revive // t is unused, but it should be kept here for the signature
	t.Run("Unlink", func(t *testing.T) {
		assertEmpty(true)
		block := &lruBlock{
			pos: 1,
		}
		assertClear(block, true)
		l.push(block)
		assertClear(block, false)
		assertEmpty(false)
		l.unlink(block)
		assertEmpty(true)
		assertClear(block, true)
	})

	// Check that we push blocks on and off in FIFO order
	t.Run("FIFO", func(t *testing.T) {
		assertEmpty(true)
		for i := int64(1); i <= 10; i++ {
			block := &lruBlock{
				pos: i,
			}
			l.push(block)
		}
		assertEmpty(false)
		for i := int64(1); i <= 10; i++ {
			block := l.pop()
			if block.pos != i {
				t.Errorf("Wanted block %d but got %d", i, block.pos)
			}
		}
		assertEmpty(true)
	})

	t.Run("Empty", func(t *testing.T) {
		defer func() {
			r, ok := recover().(string)
			if !ok || !strings.Contains(r, "list empty") {
				t.Errorf("Panic string doesn't contain list empty: %q", r)
			}
		}()
		assertEmpty(true)
		l.pop()
		t.Errorf("Expected exception to be thrown")
	})

	t.Run("Add", func(t *testing.T) {
		assertEmpty(true)
		for i := 1; i <= 2*maxBlocks; i++ {
			block := &lruBlock{
				pos: int64(i),
			}
			l.add(block)
			wantItems := i
			if i >= maxBlocks {
				wantItems = maxBlocks
			}
			gotItems := len(l.cache)
			if wantItems != gotItems {
				t.Errorf("Expected %d items but got %d", wantItems, gotItems)
			}
		}
		assertEmpty(false)
		// Check the blocks are correct in the cache
		for i := maxBlocks + 1; i <= 2*maxBlocks; i++ {
			block, found := l.cache[int64(i)]
			if !found {
				t.Errorf("Didn't find block at %d", i)
			} else if block.pos != int64(i) {
				t.Errorf("Expected block.pos=%d but got %d", i, block.pos)
			}
		}
		// Check the blocks are correct in the list
		block := l.root.prev
		for i := maxBlocks + 1; i <= 2*maxBlocks; i++ {
			if block.pos != int64(i) {
				t.Errorf("Expected block.pos=%d but got %d", i, block.pos)
			}
			block = block.prev
		}

		t.Run("Trim", func(t *testing.T) {
			assertCacheBlocks(maxBlocks)
			l.trim(maxBlocks - 1)
			assertCacheBlocks(maxBlocks - 1)
			l.trim(maxBlocks - 1)
			assertCacheBlocks(maxBlocks - 1)

			t.Run("SetMaxBlocks", func(t *testing.T) {
				assertCacheBlocks(maxBlocks - 1)
				l.setMaxBlocks(maxBlocks - 2)
				assertCacheBlocks(maxBlocks - 2)
				if l.maxBlocks != maxBlocks-2 {
					t.Errorf("Expected maxBlocks %d but got %d", maxBlocks-2, l.maxBlocks)
				}
				l.setMaxBlocks(maxBlocks)
				assertCacheBlocks(maxBlocks - 2)
				if l.maxBlocks != maxBlocks {
					t.Errorf("Expected maxBlocks %d but got %d", maxBlocks, l.maxBlocks)
				}
			})
		})
	})

	// Check blocks are as expected in the cache and LRU list
	checkCache := func(expectedPos ...int64) {
		t.Helper()
		// Check the blocks are correct in the cache
		for _, pos := range expectedPos {
			block, found := l.cache[pos]
			if !found {
				t.Errorf("Didn't find block at %d", pos)
			} else if block.pos != pos {
				t.Errorf("Expected block.pos=%d but got %d", pos, block.pos)
			}
		}
		// Check the blocks are correct in the list
		block := l.root.next
		for _, pos := range expectedPos {
			if block.pos != pos {
				t.Errorf("Expected block.pos=%d but got %d", pos, block.pos)
			}
			block = block.next
		}
	}

	l = newLRU(10)
	t.Run("Get", func(t *testing.T) {
		// Fill the cache
		for i := 1; i <= 2*maxBlocks; i++ {
			pos := int64(i)
			_, _, err := l.get(pos, func() (data []byte, size uint16, err error) {
				buf := []byte{byte(pos)}
				return buf, uint16(i), nil
			})
			assertNoError(err)
		}
		checkCache(20, 19, 18, 17, 16, 15, 14, 13, 12, 11)

		// Test cache HIT
		data, size, err := l.get(int64(14), func() (data []byte, size uint16, err error) {
			return nil, 0, errors.New("cached block not found")
		})
		assertNoError(err)
		if data[0] != 14 {
			t.Errorf("Expected magic %d but got %d", 14, data[0])
		}
		if size != 14 {
			t.Errorf("Expected size %d but got %d", 14, size)
		}
		checkCache(14, 20, 19, 18, 17, 16, 15, 13, 12, 11)

		// Test cache MISS
		data, size, err = l.get(int64(1), func() (data []byte, size uint16, err error) {
			buf := []byte{1}
			return buf, uint16(1), nil
		})
		assertNoError(err)
		if data[0] != byte(1) {
			t.Errorf("Expected magic %d but got %d", byte(1), data[0])
		}
		if size != uint16(1) {
			t.Errorf("Expected size %d but got %d", 1, size)
		}
		checkCache(1, 14, 20, 19, 18, 17, 16, 15, 13, 12)

		// Test cache fetch ERROR
		testErr := errors.New("test error")
		_, _, err = l.get(int64(2), func() (data []byte, size uint16, err error) {
			return nil, 0, testErr
		})
		if err != testErr {
			t.Errorf("Want error %q but got %q", testErr, err)
		}
		checkCache(2, 1, 14, 20, 19, 18, 17, 16, 15, 13)
	})
}
