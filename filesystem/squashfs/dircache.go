package squashfs

import (
	"container/list"
	"sync"
)

// defaultDirCacheEntries is the number of directory entries the directory cache holds by default.
const defaultDirCacheEntries = 1 << 16

// dirCache holds the entries of the directories resolved most recently (LRU), as parsed from the directory table and keyed
// on their path from the root ("" for the root itself).
//
// The cached slices are shared: callers must not modify them.
type dirCache struct {
	mu         sync.Mutex
	maxEntries int
	entries    int
	lru        list.List
	items      map[string]*list.Element
}

type dirCacheItem struct {
	path    string
	entries []*directoryEntryRaw
}

func newDirCache(maxEntries int) *dirCache {
	return &dirCache{
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element),
	}
}

// weight is what a directory counts against maxEntries: at least 1, so that empty directories are bounded too.
func weight(entries []*directoryEntryRaw) int {
	return max(1, len(entries))
}

func (c *dirCache) get(p string) ([]*directoryEntryRaw, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[p]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(e)
	item, _ := e.Value.(*dirCacheItem)
	return item.entries, true
}

func (c *dirCache) add(p string, entries []*directoryEntryRaw) {
	if c == nil || weight(entries) > c.maxEntries {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[p]; ok {
		c.lru.MoveToFront(e)
		return
	}
	for c.entries+weight(entries) > c.maxEntries {
		oldest := c.lru.Back()
		item, _ := oldest.Value.(*dirCacheItem)
		c.lru.Remove(oldest)
		delete(c.items, item.path)
		c.entries -= weight(item.entries)
	}
	c.items[p] = c.lru.PushFront(&dirCacheItem{path: p, entries: entries})
	c.entries += weight(entries)
}
