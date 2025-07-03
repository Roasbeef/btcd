// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	// DefaultMaxMemory is the default maximum memory allowed for wire protocol buffers.
	DefaultMaxMemory = 512 * 1024 * 1024 // 512MB

	// DefaultGlobalAllocRate is the default global allocation rate limit.
	DefaultGlobalAllocRate = 1000 // allocations per second

	// DefaultPerPeerAllocRate is the default per-peer allocation rate limit.
	DefaultPerPeerAllocRate = 50 // allocations per second per peer

	// SmallBufferSize is the threshold for small buffer allocations.
	SmallBufferSize = 4 * 1024 // 4KB

	// MediumBufferSize is the threshold for medium buffer allocations.
	MediumBufferSize = 64 * 1024 // 64KB

	// DefaultSmallPoolSize is the default number of small buffers.
	DefaultSmallPoolSize = 10000

	// DefaultMediumPoolSize is the default number of medium buffers.
	DefaultMediumPoolSize = 1000

	// DefaultLargePoolSize is the default number of large buffers.
	DefaultLargePoolSize = 100
)

var (
	// ErrMemoryLimitExceeded is returned when allocation would exceed memory limit.
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")

	// ErrAllocationTimeout is returned when allocation times out waiting for memory.
	ErrAllocationTimeout = errors.New("allocation timeout")

	// ErrInvalidSize is returned when requesting an invalid buffer size.
	ErrInvalidSize = errors.New("invalid buffer size")
)

// MemoryConfig contains configuration for the memory controller.
type MemoryConfig struct {
	// MaxMemory is the maximum total memory allowed for wire buffers.
	MaxMemory int64

	// SmallPoolSize is the number of small buffers to maintain.
	SmallPoolSize int

	// MediumPoolSize is the number of medium buffers to maintain.
	MediumPoolSize int

	// LargePoolSize is the number of large buffers to maintain.
	LargePoolSize int

	// GlobalAllocRate is the global allocation rate limit per second.
	GlobalAllocRate int

	// PerPeerAllocRate is the per-peer allocation rate limit per second.
	PerPeerAllocRate int

	// EvictionInterval is how often to check for buffer eviction.
	EvictionInterval time.Duration

	// BufferMaxAge is the maximum age before a buffer is evicted.
	BufferMaxAge time.Duration
}

// DefaultMemoryConfig returns a default memory configuration.
func DefaultMemoryConfig() *MemoryConfig {
	return &MemoryConfig{
		MaxMemory:        DefaultMaxMemory,
		SmallPoolSize:    DefaultSmallPoolSize,
		MediumPoolSize:   DefaultMediumPoolSize,
		LargePoolSize:    DefaultLargePoolSize,
		GlobalAllocRate:  DefaultGlobalAllocRate,
		PerPeerAllocRate: DefaultPerPeerAllocRate,
		EvictionInterval: 30 * time.Second,
		BufferMaxAge:     5 * time.Minute,
	}
}

// MemoryStats tracks memory allocation statistics.
type MemoryStats struct {
	// CurrentUsage is the current memory usage in bytes.
	CurrentUsage int64

	// PeakUsage is the peak memory usage in bytes.
	PeakUsage int64

	// TotalAllocations is the total number of allocations.
	TotalAllocations uint64

	// BlockedAllocs is the number of allocations that blocked.
	BlockedAllocs uint64

	// FailedAllocs is the number of failed allocations.
	FailedAllocs uint64

	// BuffersRecycled is the number of buffers returned to pools.
	BuffersRecycled uint64

	// BuffersEvicted is the number of buffers evicted from pools.
	BuffersEvicted uint64

	// SmallPoolHits is the number of allocations from small pool.
	SmallPoolHits uint64

	// MediumPoolHits is the number of allocations from medium pool.
	MediumPoolHits uint64

	// LargePoolHits is the number of allocations from large pool.
	LargePoolHits uint64
}

// Pool defines the interface for buffer pools.
type Pool interface {
	// Get retrieves a buffer from the pool.
	Get(size int) ([]byte, error)

	// Put returns a buffer to the pool.
	Put(buf []byte)

	// EvictOlderThan evicts buffers older than the given timestamp.
	EvictOlderThan(timestamp int64) int

	// Stats returns pool statistics.
	Stats() PoolStats
}

// PoolStats contains statistics for a buffer pool.
type PoolStats struct {
	// Size is the current number of buffers in the pool.
	Size int

	// Capacity is the maximum capacity of the pool.
	Capacity int

	// Hits is the number of successful gets from the pool.
	Hits uint64

	// Misses is the number of times a new buffer was allocated.
	Misses uint64

	// Returns is the number of buffers returned to the pool.
	Returns uint64

	// Evictions is the number of buffers evicted.
	Evictions uint64
}

// MemoryController manages global memory allocation for wire protocol messages.
type MemoryController struct {
	// Configuration
	config *MemoryConfig

	// Current memory usage
	currentUsage atomic.Int64

	// Peak memory usage
	peakUsage atomic.Int64

	// Rate limiters
	globalLimiter *rate.Limiter
	peerLimiters  sync.Map // map[string]*rate.Limiter

	// Buffer pools
	smallPool  Pool
	mediumPool Pool
	largePool  Pool

	// Statistics
	stats MemoryStats
	mu    sync.RWMutex

	// Waiters for memory availability
	waiters     []chan struct{}
	waitersMu   sync.Mutex
	waitersSize sync.Map // map[chan struct{}]int

	// Shutdown
	quit chan struct{}
	wg   sync.WaitGroup
}

// NewMemoryController creates a new memory controller with the given configuration.
func NewMemoryController(config *MemoryConfig) (*MemoryController, error) {
	if config == nil {
		config = DefaultMemoryConfig()
	}

	if config.MaxMemory <= 0 {
		return nil, fmt.Errorf("invalid max memory: %d", config.MaxMemory)
	}

	mc := &MemoryController{
		config:        config,
		globalLimiter: rate.NewLimiter(rate.Limit(config.GlobalAllocRate), config.GlobalAllocRate),
		quit:          make(chan struct{}),
	}

	// Initialize pools
	smallPool, err := NewSlabPool(config.SmallPoolSize, SmallBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create small pool: %w", err)
	}
	mc.smallPool = smallPool
	
	mediumPool, err := NewSizeClassPool(config.MediumPoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create medium pool: %w", err)
	}
	mc.mediumPool = mediumPool
	
	largePool, err := NewSimpleArenaPool(config.LargePoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create large pool: %w", err)
	}
	mc.largePool = largePool

	// Start background tasks
	mc.wg.Add(1)
	go mc.evictionLoop()

	return mc, nil
}

// Allocate allocates a buffer of the given size from the appropriate pool.
func (mc *MemoryController) Allocate(size int) ([]byte, error) {
	if size <= 0 {
		return nil, ErrInvalidSize
	}

	// Check if allocation would exceed limit
	currentUsage := mc.currentUsage.Load()
	if currentUsage+int64(size) > mc.config.MaxMemory {
		// Try to wait for memory to become available
		if err := mc.waitForMemory(size); err != nil {
			atomic.AddUint64(&mc.stats.FailedAllocs, 1)
			return nil, err
		}
	}

	// Rate limit check
	if err := mc.globalLimiter.Wait(context.Background()); err != nil {
		atomic.AddUint64(&mc.stats.FailedAllocs, 1)
		return nil, fmt.Errorf("rate limit error: %w", err)
	}

	// Select appropriate pool and allocate
	var buf []byte
	var err error

	switch {
	case size <= SmallBufferSize:
		if mc.smallPool != nil {
			buf, err = mc.smallPool.Get(size)
			if err == nil {
				atomic.AddUint64(&mc.stats.SmallPoolHits, 1)
			}
		}
	case size <= MediumBufferSize:
		if mc.mediumPool != nil {
			buf, err = mc.mediumPool.Get(size)
			if err == nil {
				atomic.AddUint64(&mc.stats.MediumPoolHits, 1)
			}
		}
	default:
		if mc.largePool != nil {
			buf, err = mc.largePool.Get(size)
			if err == nil {
				atomic.AddUint64(&mc.stats.LargePoolHits, 1)
			}
		}
	}

	// Fallback to direct allocation if pools are not available or failed
	if buf == nil {
		buf = make([]byte, size)
	}

	// Update usage statistics
	newUsage := mc.currentUsage.Add(int64(cap(buf)))
	mc.updatePeakUsage(newUsage)
	atomic.AddUint64(&mc.stats.TotalAllocations, 1)

	return buf[:size], nil
}

// AllocateBuffer allocates a buffer wrapped in a Buffer object with lifecycle management.
func (mc *MemoryController) AllocateBuffer(size int) (*Buffer, error) {
	data, err := mc.Allocate(size)
	if err != nil {
		return nil, err
	}
	
	// Determine which pool this came from
	var pool Pool
	switch {
	case size <= SmallBufferSize:
		pool = mc.smallPool
	case size <= MediumBufferSize:
		pool = mc.mediumPool
	default:
		pool = mc.largePool
	}
	
	return NewBuffer(data, size, pool, mc), nil
}

// Release returns a buffer to the appropriate pool.
func (mc *MemoryController) Release(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}

	size := cap(buf)

	// Return to appropriate pool
	switch {
	case size <= SmallBufferSize:
		if mc.smallPool != nil {
			mc.smallPool.Put(buf)
		}
	case size <= MediumBufferSize:
		if mc.mediumPool != nil {
			mc.mediumPool.Put(buf)
		}
	default:
		if mc.largePool != nil {
			mc.largePool.Put(buf)
		}
	}

	// Update usage and notify waiters
	mc.currentUsage.Add(-int64(size))
	atomic.AddUint64(&mc.stats.BuffersRecycled, 1)
	mc.notifyWaiters()
}

// GetPeerLimiter returns the rate limiter for a specific peer.
func (mc *MemoryController) GetPeerLimiter(peerID string) *rate.Limiter {
	if limiter, ok := mc.peerLimiters.Load(peerID); ok {
		return limiter.(*rate.Limiter)
	}

	// Create new limiter for peer
	limiter := rate.NewLimiter(rate.Limit(mc.config.PerPeerAllocRate), mc.config.PerPeerAllocRate)
	actual, _ := mc.peerLimiters.LoadOrStore(peerID, limiter)
	return actual.(*rate.Limiter)
}

// RemovePeerLimiter removes the rate limiter for a peer.
func (mc *MemoryController) RemovePeerLimiter(peerID string) {
	mc.peerLimiters.Delete(peerID)
}

// Stats returns current memory statistics.
func (mc *MemoryController) Stats() MemoryStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := mc.stats
	stats.CurrentUsage = mc.currentUsage.Load()
	stats.PeakUsage = mc.peakUsage.Load()
	return stats
}

// Shutdown gracefully shuts down the memory controller.
func (mc *MemoryController) Shutdown() {
	close(mc.quit)
	mc.wg.Wait()
}

// waitForMemory waits for memory to become available for allocation.
func (mc *MemoryController) waitForMemory(size int) error {
	waiter := make(chan struct{})

	mc.waitersMu.Lock()
	mc.waiters = append(mc.waiters, waiter)
	mc.waitersSize.Store(waiter, size)
	mc.waitersMu.Unlock()

	atomic.AddUint64(&mc.stats.BlockedAllocs, 1)

	// Wait with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	select {
	case <-waiter:
		// Memory available, check again
		if mc.currentUsage.Load()+int64(size) <= mc.config.MaxMemory {
			return nil
		}
		return ErrMemoryLimitExceeded
	case <-ctx.Done():
		// Remove from waiters
		mc.waitersMu.Lock()
		for i, w := range mc.waiters {
			if w == waiter {
				mc.waiters = append(mc.waiters[:i], mc.waiters[i+1:]...)
				break
			}
		}
		mc.waitersMu.Unlock()
		mc.waitersSize.Delete(waiter)
		return ErrAllocationTimeout
	}
}

// notifyWaiters notifies waiting allocations that memory may be available.
func (mc *MemoryController) notifyWaiters() {
	mc.waitersMu.Lock()
	defer mc.waitersMu.Unlock()

	currentUsage := mc.currentUsage.Load()
	maxMemory := mc.config.MaxMemory

	// Notify waiters that can now allocate
	remaining := mc.waiters[:0]
	for _, waiter := range mc.waiters {
		if sizeVal, ok := mc.waitersSize.Load(waiter); ok {
			size := sizeVal.(int)
			if currentUsage+int64(size) <= maxMemory {
				close(waiter)
				mc.waitersSize.Delete(waiter)
				currentUsage += int64(size)
			} else {
				remaining = append(remaining, waiter)
			}
		}
	}
	mc.waiters = remaining
}

// updatePeakUsage updates the peak usage if current is higher.
func (mc *MemoryController) updatePeakUsage(current int64) {
	for {
		peak := mc.peakUsage.Load()
		if current <= peak || mc.peakUsage.CompareAndSwap(peak, current) {
			break
		}
	}
}

// evictionLoop runs periodic eviction of old buffers.
func (mc *MemoryController) evictionLoop() {
	defer mc.wg.Done()

	ticker := time.NewTicker(mc.config.EvictionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.evictOldBuffers()
		case <-mc.quit:
			return
		}
	}
}

// evictOldBuffers evicts buffers older than the configured max age.
func (mc *MemoryController) evictOldBuffers() {
	now := time.Now().Unix()
	maxAge := int64(mc.config.BufferMaxAge.Seconds())
	cutoff := now - maxAge

	evicted := 0
	if mc.smallPool != nil {
		evicted += mc.smallPool.EvictOlderThan(cutoff)
	}
	if mc.mediumPool != nil {
		evicted += mc.mediumPool.EvictOlderThan(cutoff)
	}
	if mc.largePool != nil {
		evicted += mc.largePool.EvictOlderThan(cutoff)
	}

	if evicted > 0 {
		atomic.AddUint64(&mc.stats.BuffersEvicted, uint64(evicted))
	}
}

// ReclaimMemory attempts to reclaim memory by triggering GC and eviction.
func (mc *MemoryController) ReclaimMemory() {
	// Force a GC cycle
	runtime.GC()

	// Evict old buffers
	mc.evictOldBuffers()

	// Check if we need to shrink pools
	usage := mc.currentUsage.Load()
	limit := mc.config.MaxMemory
	if float64(usage) > float64(limit)*0.9 {
		// TODO: Implement pool shrinking
	}
}

// GetMemoryUsage returns the current memory usage and system memory stats.
func (mc *MemoryController) GetMemoryUsage() (current int64, system runtime.MemStats) {
	current = mc.currentUsage.Load()
	runtime.ReadMemStats(&system)
	return
}