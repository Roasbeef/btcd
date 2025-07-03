// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Common size classes for medium buffers
var sizeClasses = []int{
	4 * 1024,   // 4KB
	8 * 1024,   // 8KB
	16 * 1024,  // 16KB
	32 * 1024,  // 32KB
	64 * 1024,  // 64KB
}

// sizeClassEntry represents a buffer in a size class pool.
type sizeClassEntry struct {
	buf      []byte
	lastUsed int64
	next     *sizeClassEntry
}

// sizeClassBucket manages buffers of a specific size class.
type sizeClassBucket struct {
	size       int
	maxCount   int
	freeList   *sizeClassEntry
	freeListMu sync.Mutex
	
	// All entries for tracking
	allEntries []*sizeClassEntry
	entriesMu  sync.RWMutex
	
	// Statistics
	hits      atomic.Uint64
	misses    atomic.Uint64
	returns   atomic.Uint64
	evictions atomic.Uint64
	inUse     atomic.Int32
}

// newSizeClassBucket creates a new bucket for a specific size class.
func newSizeClassBucket(size, maxCount int) *sizeClassBucket {
	return &sizeClassBucket{
		size:       size,
		maxCount:   maxCount,
		allEntries: make([]*sizeClassEntry, 0, maxCount),
	}
}

// get retrieves a buffer from the bucket.
func (b *sizeClassBucket) get() ([]byte, error) {
	b.freeListMu.Lock()
	entry := b.freeList
	if entry != nil {
		b.freeList = entry.next
		entry.next = nil
		entry.lastUsed = time.Now().Unix()
		b.freeListMu.Unlock()
		
		b.hits.Add(1)
		b.inUse.Add(1)
		return entry.buf, nil
	}
	b.freeListMu.Unlock()
	
	// Try to allocate new buffer if under limit
	b.entriesMu.Lock()
	if len(b.allEntries) < b.maxCount {
		buf := make([]byte, b.size)
		entry := &sizeClassEntry{
			buf:      buf,
			lastUsed: time.Now().Unix(),
		}
		b.allEntries = append(b.allEntries, entry)
		b.entriesMu.Unlock()
		
		b.hits.Add(1)
		b.inUse.Add(1)
		return buf, nil
	}
	b.entriesMu.Unlock()
	
	b.misses.Add(1)
	return nil, errors.New("no buffers available in size class")
}

// put returns a buffer to the bucket.
func (b *sizeClassBucket) put(buf []byte) {
	if buf == nil || cap(buf) != b.size {
		return
	}
	
	// Find the entry for this buffer
	b.entriesMu.RLock()
	var entry *sizeClassEntry
	for _, e := range b.allEntries {
		if len(e.buf) > 0 && &e.buf[0] == &buf[0] {
			entry = e
			break
		}
	}
	b.entriesMu.RUnlock()
	
	if entry == nil {
		return
	}
	
	// Clear the buffer before returning to pool
	for i := range buf {
		buf[i] = 0
	}
	
	b.freeListMu.Lock()
	entry.lastUsed = time.Now().Unix()
	entry.next = b.freeList
	b.freeList = entry
	b.freeListMu.Unlock()
	
	b.returns.Add(1)
	b.inUse.Add(-1)
}

// evictOlderThan removes buffers older than the given timestamp.
func (b *sizeClassBucket) evictOlderThan(timestamp int64) int {
	b.freeListMu.Lock()
	defer b.freeListMu.Unlock()
	
	evicted := 0
	var newFreeList *sizeClassEntry
	var tail *sizeClassEntry
	
	for entry := b.freeList; entry != nil; {
		next := entry.next
		if entry.lastUsed < timestamp {
			// Evict this entry
			evicted++
			b.evictions.Add(1)
			// Remove from allEntries
			b.entriesMu.Lock()
			for i, e := range b.allEntries {
				if e == entry {
					b.allEntries = append(b.allEntries[:i], b.allEntries[i+1:]...)
					break
				}
			}
			b.entriesMu.Unlock()
		} else {
			// Keep this entry
			entry.next = nil
			if newFreeList == nil {
				newFreeList = entry
				tail = entry
			} else {
				tail.next = entry
				tail = entry
			}
		}
		entry = next
	}
	
	b.freeList = newFreeList
	return evicted
}

// stats returns bucket statistics.
func (b *sizeClassBucket) stats() PoolStats {
	b.freeListMu.Lock()
	freeCount := 0
	for entry := b.freeList; entry != nil; entry = entry.next {
		freeCount++
	}
	b.freeListMu.Unlock()
	
	return PoolStats{
		Size:      freeCount,
		Capacity:  b.maxCount,
		Hits:      b.hits.Load(),
		Misses:    b.misses.Load(),
		Returns:   b.returns.Load(),
		Evictions: b.evictions.Load(),
	}
}

// SizeClassPool implements a pool with multiple size classes for medium buffers.
type SizeClassPool struct {
	// Buckets for each size class
	buckets map[int]*sizeClassBucket
	
	// Total capacity across all buckets
	totalCapacity int
	
	// Statistics
	totalHits      atomic.Uint64
	totalMisses    atomic.Uint64
	totalReturns   atomic.Uint64
	totalEvictions atomic.Uint64
}

// NewSizeClassPool creates a new size class pool with the specified total capacity.
func NewSizeClassPool(totalCapacity int) (*SizeClassPool, error) {
	if totalCapacity <= 0 {
		return nil, errors.New("invalid total capacity")
	}
	
	pool := &SizeClassPool{
		buckets:       make(map[int]*sizeClassBucket),
		totalCapacity: totalCapacity,
	}
	
	// Distribute capacity among size classes
	// Use a weighted distribution - smaller sizes get more slots
	weights := []int{5, 4, 3, 2, 1} // 4KB gets 5x weight, 64KB gets 1x
	totalWeight := 0
	for _, w := range weights {
		totalWeight += w
	}
	
	// Ensure all size classes get initialized even with small capacity
	for _, size := range sizeClasses {
		pool.buckets[size] = newSizeClassBucket(size, 0)
	}
	
	remainingCapacity := totalCapacity
	for i, size := range sizeClasses {
		// Calculate capacity for this size class
		capacity := (totalCapacity * weights[i]) / totalWeight
		// Ensure at least 1 buffer per size class if we have capacity
		if capacity == 0 && remainingCapacity > 0 {
			capacity = 1
		}
		if i == len(sizeClasses)-1 {
			// Give remaining capacity to last bucket
			capacity = remainingCapacity
		}
		
		if capacity > 0 {
			pool.buckets[size].maxCount = capacity
			remainingCapacity -= capacity
		}
	}
	
	return pool, nil
}

// Get retrieves a buffer of at least the requested size.
func (sp *SizeClassPool) Get(size int) ([]byte, error) {
	if size <= 0 || size > sizeClasses[len(sizeClasses)-1] {
		sp.totalMisses.Add(1)
		return nil, fmt.Errorf("size %d out of range for size class pool", size)
	}
	
	// Find the appropriate size class
	for _, classSize := range sizeClasses {
		if size <= classSize {
			bucket := sp.buckets[classSize]
			buf, err := bucket.get()
			if err == nil {
				sp.totalHits.Add(1)
				return buf[:size], nil
			}
			// Try next size class if this one is exhausted
		}
	}
	
	sp.totalMisses.Add(1)
	return nil, errors.New("no buffers available in any size class")
}

// Put returns a buffer to the pool.
func (sp *SizeClassPool) Put(buf []byte) {
	if buf == nil {
		return
	}
	
	capacity := cap(buf)
	bucket, ok := sp.buckets[capacity]
	if !ok {
		// Buffer doesn't belong to any size class
		return
	}
	
	bucket.put(buf[:capacity])
	sp.totalReturns.Add(1)
}

// EvictOlderThan evicts buffers older than the given timestamp.
func (sp *SizeClassPool) EvictOlderThan(timestamp int64) int {
	totalEvicted := 0
	for _, bucket := range sp.buckets {
		evicted := bucket.evictOlderThan(timestamp)
		totalEvicted += evicted
	}
	
	if totalEvicted > 0 {
		sp.totalEvictions.Add(uint64(totalEvicted))
	}
	
	return totalEvicted
}

// Stats returns pool statistics.
func (sp *SizeClassPool) Stats() PoolStats {
	stats := PoolStats{
		Hits:      sp.totalHits.Load(),
		Misses:    sp.totalMisses.Load(),
		Returns:   sp.totalReturns.Load(),
		Evictions: sp.totalEvictions.Load(),
	}
	
	// Aggregate stats from all buckets
	for _, bucket := range sp.buckets {
		bucketStats := bucket.stats()
		stats.Size += bucketStats.Size
		stats.Capacity += bucketStats.Capacity
	}
	
	return stats
}

// DetailedStats returns per-size-class statistics.
func (sp *SizeClassPool) DetailedStats() map[int]PoolStats {
	details := make(map[int]PoolStats)
	for size, bucket := range sp.buckets {
		details[size] = bucket.stats()
	}
	return details
}