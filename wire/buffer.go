// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Buffer represents a pooled buffer with reference counting and lifecycle management.
type Buffer struct {
	// data is the underlying byte slice
	data []byte
	
	// pool is the pool this buffer belongs to
	pool Pool
	
	// controller is the memory controller managing this buffer
	controller *MemoryController
	
	// refCount tracks the number of active references
	refCount atomic.Int32
	
	// lastUsed tracks when the buffer was last used
	lastUsed atomic.Int64
	
	// size is the requested size (may be less than cap(data))
	size int
}

// NewBuffer creates a new buffer with the given data and pool.
func NewBuffer(data []byte, size int, pool Pool, controller *MemoryController) *Buffer {
	b := &Buffer{
		data:       data,
		size:       size,
		pool:       pool,
		controller: controller,
	}
	b.refCount.Store(1)
	b.lastUsed.Store(time.Now().Unix())
	return b
}

// Bytes returns the buffer's data slice sized to the requested size.
func (b *Buffer) Bytes() []byte {
	if b == nil || b.data == nil {
		return nil
	}
	return b.data[:b.size]
}

// Cap returns the capacity of the underlying buffer.
func (b *Buffer) Cap() int {
	if b == nil || b.data == nil {
		return 0
	}
	return cap(b.data)
}

// Len returns the size of the buffer (as requested during allocation).
func (b *Buffer) Len() int {
	if b == nil {
		return 0
	}
	return b.size
}

// AddRef increments the reference count.
func (b *Buffer) AddRef() {
	if b == nil {
		return
	}
	b.refCount.Add(1)
	b.lastUsed.Store(time.Now().Unix())
}

// Release decrements the reference count and returns the buffer to the pool
// when the count reaches zero.
func (b *Buffer) Release() {
	if b == nil {
		return
	}
	
	if b.refCount.Add(-1) == 0 {
		// Return to pool
		if b.pool != nil && b.data != nil {
			b.pool.Put(b.data)
		}
		
		// Update controller statistics
		if b.controller != nil {
			b.controller.currentUsage.Add(-int64(cap(b.data)))
			atomic.AddUint64(&b.controller.stats.BuffersRecycled, 1)
			b.controller.notifyWaiters()
		}
		
		// Clear the buffer to prevent use after free
		b.data = nil
		b.pool = nil
		b.controller = nil
		b.size = 0
	}
}

// RefCount returns the current reference count.
func (b *Buffer) RefCount() int32 {
	if b == nil {
		return 0
	}
	return b.refCount.Load()
}

// LastUsed returns the unix timestamp of last use.
func (b *Buffer) LastUsed() int64 {
	if b == nil {
		return 0
	}
	return b.lastUsed.Load()
}

// Touch updates the last used timestamp.
func (b *Buffer) Touch() {
	if b == nil {
		return
	}
	b.lastUsed.Store(time.Now().Unix())
}

// Clone creates a new buffer with a copy of the data.
// The new buffer has its own reference count starting at 1.
func (b *Buffer) Clone() (*Buffer, error) {
	if b == nil || b.data == nil {
		return nil, fmt.Errorf("cannot clone nil or released buffer")
	}
	
	// Allocate new buffer from controller
	if b.controller == nil {
		return nil, fmt.Errorf("no controller for buffer clone")
	}
	
	newData, err := b.controller.Allocate(b.size)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate clone buffer: %w", err)
	}
	
	// Copy data
	copy(newData, b.Bytes())
	
	// Wrap in buffer (controller.Allocate should return a Buffer in the future)
	return &Buffer{
		data:       newData,
		size:       b.size,
		pool:       b.pool,
		controller: b.controller,
		refCount:   atomic.Int32{},
		lastUsed:   atomic.Int64{},
	}, nil
}

// Zero clears the buffer's contents.
func (b *Buffer) Zero() {
	if b == nil || b.data == nil {
		return
	}
	for i := range b.data[:b.size] {
		b.data[i] = 0
	}
}

// BufferHolder is implemented by message types that need to hold a buffer reference.
type BufferHolder interface {
	// SetBuffer sets the buffer reference.
	SetBuffer(*Buffer)
	
	// ReleaseBuffer releases the buffer reference.
	ReleaseBuffer()
}

// BufferManager provides high-level buffer management operations.
type BufferManager struct {
	controller *MemoryController
}

// NewBufferManager creates a new buffer manager.
func NewBufferManager(controller *MemoryController) *BufferManager {
	return &BufferManager{
		controller: controller,
	}
}

// AllocateBuffer allocates a new buffer of the given size.
func (bm *BufferManager) AllocateBuffer(size int) (*Buffer, error) {
	data, err := bm.controller.Allocate(size)
	if err != nil {
		return nil, err
	}
	
	// Determine which pool this came from
	var pool Pool
	switch {
	case size <= SmallBufferSize:
		pool = bm.controller.smallPool
	case size <= MediumBufferSize:
		pool = bm.controller.mediumPool
	default:
		pool = bm.controller.largePool
	}
	
	return NewBuffer(data, size, pool, bm.controller), nil
}

// AllocateBufferForPeer allocates a buffer with peer-specific rate limiting.
func (bm *BufferManager) AllocateBufferForPeer(peerID string, size int) (*Buffer, error) {
	// Check peer rate limit
	limiter := bm.controller.GetPeerLimiter(peerID)
	if !limiter.Allow() {
		return nil, fmt.Errorf("peer %s exceeded rate limit", peerID)
	}
	
	return bm.AllocateBuffer(size)
}

// TransferBuffer transfers ownership of a buffer to a message that implements BufferHolder.
func (bm *BufferManager) TransferBuffer(buf *Buffer, holder BufferHolder) {
	if buf == nil || holder == nil {
		return
	}
	
	// Add reference for the holder
	buf.AddRef()
	
	// Set the buffer on the holder
	holder.SetBuffer(buf)
	
	// Release our reference
	buf.Release()
}