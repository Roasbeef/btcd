// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// SimpleArenaPool implements a simplified arena allocator for large buffers.
// It uses a free list of available chunks without complex coalescing.
type SimpleArenaPool struct {
	// Configuration
	maxAllocs int
	chunkSize int // Fixed chunk size
	
	// Pre-allocated chunks
	chunks    [][]byte
	
	// Free list
	freeList   []int // indices of free chunks
	freeListMu sync.Mutex
	
	// Used chunks map for Put operation
	usedChunks map[*byte]int // maps first byte address to chunk index
	usedMu     sync.RWMutex
	
	// Statistics
	hits      atomic.Uint64
	misses    atomic.Uint64
	returns   atomic.Uint64
	evictions atomic.Uint64
	inUse     atomic.Int32
}

// NewSimpleArenaPool creates a new simple arena pool.
func NewSimpleArenaPool(maxAllocs int) (*SimpleArenaPool, error) {
	if maxAllocs <= 0 {
		return nil, errors.New("invalid max allocations")
	}
	
	// Use 8MB chunks (large enough for most blocks)
	chunkSize := 8 * 1024 * 1024
	
	pool := &SimpleArenaPool{
		maxAllocs:  maxAllocs,
		chunkSize:  chunkSize,
		chunks:     make([][]byte, maxAllocs),
		freeList:   make([]int, 0, maxAllocs),
		usedChunks: make(map[*byte]int),
	}
	
	// Pre-allocate all chunks
	for i := 0; i < maxAllocs; i++ {
		pool.chunks[i] = make([]byte, chunkSize)
		pool.freeList = append(pool.freeList, i)
	}
	
	return pool, nil
}

// Get allocates a buffer of the requested size.
func (p *SimpleArenaPool) Get(size int) ([]byte, error) {
	if size <= 0 || size > p.chunkSize {
		p.misses.Add(1)
		return nil, fmt.Errorf("size %d out of range for arena pool (max %d)", size, p.chunkSize)
	}
	
	p.freeListMu.Lock()
	if len(p.freeList) == 0 {
		p.freeListMu.Unlock()
		p.misses.Add(1)
		return nil, errors.New("no free chunks available")
	}
	
	// Get a free chunk
	idx := p.freeList[len(p.freeList)-1]
	p.freeList = p.freeList[:len(p.freeList)-1]
	p.freeListMu.Unlock()
	
	chunk := p.chunks[idx]
	buf := chunk[:size]
	
	// Track the allocation
	p.usedMu.Lock()
	p.usedChunks[&buf[0]] = idx
	p.usedMu.Unlock()
	
	p.hits.Add(1)
	p.inUse.Add(1)
	
	return buf, nil
}

// Put returns a buffer to the pool.
func (p *SimpleArenaPool) Put(buf []byte) {
	if buf == nil || len(buf) == 0 {
		return
	}
	
	// Find the chunk index
	p.usedMu.RLock()
	idx, ok := p.usedChunks[&buf[0]]
	p.usedMu.RUnlock()
	
	if !ok {
		return // Not from this pool
	}
	
	// Clear the buffer
	for i := range buf {
		buf[i] = 0
	}
	
	// Remove from used map
	p.usedMu.Lock()
	delete(p.usedChunks, &buf[0])
	p.usedMu.Unlock()
	
	// Return to free list
	p.freeListMu.Lock()
	p.freeList = append(p.freeList, idx)
	p.freeListMu.Unlock()
	
	p.returns.Add(1)
	p.inUse.Add(-1)
}

// EvictOlderThan is a no-op for simple arena pools.
func (p *SimpleArenaPool) EvictOlderThan(timestamp int64) int {
	// No-op for pre-allocated pools
	return 0
}

// Stats returns pool statistics.
func (p *SimpleArenaPool) Stats() PoolStats {
	p.freeListMu.Lock()
	freeCount := len(p.freeList)
	p.freeListMu.Unlock()
	
	return PoolStats{
		Size:      freeCount,
		Capacity:  p.maxAllocs,
		Hits:      p.hits.Load(),
		Misses:    p.misses.Load(),
		Returns:   p.returns.Load(),
		Evictions: p.evictions.Load(),
	}
}