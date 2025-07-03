// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// slabEntry represents a single buffer in the slab pool.
type slabEntry struct {
	buf       []byte
	lastUsed  int64 // Unix timestamp
	inUse     bool
	next      *slabEntry
}

// SlabPool implements a slab allocator for fixed-size buffers.
// It pre-allocates a large chunk of memory and divides it into fixed-size slabs.
type SlabPool struct {
	// Configuration
	slabSize  int
	slabCount int

	// Memory slab
	memory []byte

	// Free list
	freeList   *slabEntry
	freeListMu sync.Mutex

	// All entries for eviction scanning
	allEntries []*slabEntry

	// Statistics
	hits      atomic.Uint64
	misses    atomic.Uint64
	returns   atomic.Uint64
	evictions atomic.Uint64
	inUse     atomic.Int32
}

// NewSlabPool creates a new slab pool with the specified number of slabs.
func NewSlabPool(slabCount, slabSize int) (*SlabPool, error) {
	if slabCount <= 0 || slabSize <= 0 {
		return nil, errors.New("invalid slab count or size")
	}

	// Allocate the entire memory block upfront
	totalSize := slabCount * slabSize
	memory := make([]byte, totalSize)

	pool := &SlabPool{
		slabSize:   slabSize,
		slabCount:  slabCount,
		memory:     memory,
		allEntries: make([]*slabEntry, slabCount),
	}

	// Initialize all slabs and build free list
	var prev *slabEntry
	for i := 0; i < slabCount; i++ {
		// Create a slice with correct capacity by using 3-arg slice
		start := i * slabSize
		end := (i + 1) * slabSize
		entry := &slabEntry{
			buf:      memory[start:end:end], // 3-arg slice to limit capacity
			lastUsed: time.Now().Unix(),
			inUse:    false,
		}

		pool.allEntries[i] = entry

		if prev != nil {
			prev.next = entry
		} else {
			pool.freeList = entry
		}
		prev = entry
	}

	return pool, nil
}

// Get retrieves a buffer from the pool.
func (sp *SlabPool) Get(size int) ([]byte, error) {
	if size > sp.slabSize {
		sp.misses.Add(1)
		return nil, errors.New("requested size exceeds slab size")
	}

	sp.freeListMu.Lock()
	entry := sp.freeList
	if entry != nil {
		sp.freeList = entry.next
		entry.next = nil
		entry.inUse = true
		entry.lastUsed = time.Now().Unix()
		sp.freeListMu.Unlock()

		sp.hits.Add(1)
		sp.inUse.Add(1)
		return entry.buf[:size], nil
	}
	sp.freeListMu.Unlock()

	// No free slabs available
	sp.misses.Add(1)
	return nil, errors.New("no free slabs available")
}

// Put returns a buffer to the pool.
func (sp *SlabPool) Put(buf []byte) {
	if buf == nil || cap(buf) != sp.slabSize {
		return
	}

	// Find the entry for this buffer
	// This is O(n) but could be optimized with a map if needed
	for _, entry := range sp.allEntries {
		if &entry.buf[0] == &buf[:cap(buf)][0] {
			sp.freeListMu.Lock()
			if entry.inUse {
				entry.inUse = false
				entry.lastUsed = time.Now().Unix()
				entry.next = sp.freeList
				sp.freeList = entry
				sp.returns.Add(1)
				sp.inUse.Add(-1)
			}
			sp.freeListMu.Unlock()
			return
		}
	}
}

// EvictOlderThan evicts buffers that haven't been used since the given timestamp.
func (sp *SlabPool) EvictOlderThan(timestamp int64) int {
	// For slab pools, we don't actually evict memory since it's pre-allocated.
	// We just mark entries as evicted for statistics.
	evicted := 0
	
	for _, entry := range sp.allEntries {
		if !entry.inUse && entry.lastUsed < timestamp {
			evicted++
		}
	}
	
	if evicted > 0 {
		sp.evictions.Add(uint64(evicted))
	}
	
	return evicted
}

// Stats returns pool statistics.
func (sp *SlabPool) Stats() PoolStats {
	freeCount := 0
	sp.freeListMu.Lock()
	for entry := sp.freeList; entry != nil; entry = entry.next {
		freeCount++
	}
	sp.freeListMu.Unlock()

	return PoolStats{
		Size:      sp.slabCount - int(sp.inUse.Load()),
		Capacity:  sp.slabCount,
		Hits:      sp.hits.Load(),
		Misses:    sp.misses.Load(),
		Returns:   sp.returns.Load(),
		Evictions: sp.evictions.Load(),
	}
}

// Reset clears all buffers in the pool. This zeros out the memory.
func (sp *SlabPool) Reset() {
	sp.freeListMu.Lock()
	defer sp.freeListMu.Unlock()

	// Zero out all memory
	for i := range sp.memory {
		sp.memory[i] = 0
	}

	// Reset all entries to free
	var prev *slabEntry
	for i, entry := range sp.allEntries {
		entry.inUse = false
		entry.lastUsed = time.Now().Unix()
		entry.next = nil

		if prev != nil {
			prev.next = entry
		} else {
			sp.freeList = entry
		}
		prev = entry

		if i == 0 {
			sp.freeList = entry
		}
	}

	// Reset statistics
	sp.hits.Store(0)
	sp.misses.Store(0)
	sp.returns.Store(0)
	sp.evictions.Store(0)
	sp.inUse.Store(0)
}