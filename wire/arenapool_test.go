// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestArenaPoolBasic tests basic arena pool functionality.
func TestArenaPoolBasic(t *testing.T) {
	pool, err := NewArenaPool(10)
	if err != nil {
		t.Fatalf("Failed to create arena pool: %v", err)
	}

	// Test allocation
	sizes := []int{1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024} // 1MB, 2MB, 4MB
	buffers := make([][]byte, 0, len(sizes))

	for _, size := range sizes {
		buf, err := pool.Get(size)
		if err != nil {
			t.Errorf("Failed to allocate %d bytes: %v", size, err)
			continue
		}

		if len(buf) != size {
			t.Errorf("Buffer size mismatch: got %d, want %d", len(buf), size)
		}

		buffers = append(buffers, buf)
	}

	// Check stats before return
	stats := pool.GetDetailedStats()
	if stats.Allocations != int32(len(buffers)) {
		t.Errorf("Allocation count mismatch: got %d, want %d", stats.Allocations, len(buffers))
	}

	// Return buffers and verify coalescing
	for _, buf := range buffers {
		pool.Put(buf)
	}

	// After returning all, we should have one large free block due to coalescing
	stats = pool.GetDetailedStats()
	if stats.Allocations != 0 {
		t.Errorf("Expected 0 allocations after returns, got %d", stats.Allocations)
	}

	// The free list should ideally have 1 block due to coalescing
	if stats.Size > len(sizes) {
		t.Logf("Warning: Free list has %d blocks, expected fewer due to coalescing", stats.Size)
	}
}

// TestArenaPoolFragmentation tests fragmentation handling.
func TestArenaPoolFragmentation(t *testing.T) {
	pool, err := NewArenaPool(20)
	if err != nil {
		t.Fatalf("Failed to create arena pool: %v", err)
	}

	// Allocate alternating pattern to create fragmentation
	buffers := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		size := 1024 * 1024 // 1MB each
		buf, err := pool.Get(size)
		if err != nil {
			t.Fatalf("Failed to allocate buffer %d: %v", i, err)
		}
		buffers[i] = buf
	}

	// Free every other buffer to create fragmentation
	for i := 0; i < 10; i += 2 {
		pool.Put(buffers[i])
	}

	stats := pool.GetDetailedStats()
	t.Logf("Fragmentation after alternating frees: %d%%", stats.Fragmentation)

	// Try to allocate a large buffer that won't fit in fragments
	largeBuf, err := pool.Get(3 * 1024 * 1024) // 3MB
	if err == nil {
		t.Logf("Successfully allocated 3MB buffer despite fragmentation")
		pool.Put(largeBuf)
	}

	// Free remaining buffers
	for i := 1; i < 10; i += 2 {
		pool.Put(buffers[i])
	}

	// After all frees, fragmentation should be 0
	stats = pool.GetDetailedStats()
	if stats.Fragmentation != 0 {
		t.Errorf("Expected 0 fragmentation after all frees, got %d%%", stats.Fragmentation)
	}
}

// TestArenaPoolPropertyBased tests the arena pool with property-based testing.
func TestArenaPoolPropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxAllocs := rapid.IntRange(5, 50).Draw(t, "maxAllocs")
		
		pool, err := NewArenaPool(maxAllocs)
		if err != nil {
			t.Fatalf("Failed to create arena pool: %v", err)
		}

		// Generate allocation operations
		type op struct {
			size   int
			holdMs int // How long to hold before releasing
		}
		
		ops := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) op {
			// Generate sizes between 64KB and 8MB
			size := rapid.IntRange(64*1024, 8*1024*1024).Draw(t, "size")
			hold := rapid.IntRange(0, 10).Draw(t, "holdMs")
			return op{size: size, holdMs: hold}
		}), 1, maxAllocs*2).Draw(t, "operations")

		// Track allocations
		type allocation struct {
			buf        []byte
			releaseAt  time.Time
		}
		allocations := make([]allocation, 0)
		
		// Execute operations
		for i, operation := range ops {
			// First, release any buffers that are due
			now := time.Now()
			remaining := allocations[:0]
			for _, alloc := range allocations {
				if now.After(alloc.releaseAt) {
					pool.Put(alloc.buf)
				} else {
					remaining = append(remaining, alloc)
				}
			}
			allocations = remaining

			// Try to allocate
			buf, err := pool.Get(operation.size)
			if err == nil {
				if len(buf) != operation.size {
					t.Fatalf("Op %d: Buffer size mismatch: got %d, want %d", 
						i, len(buf), operation.size)
				}
				
				allocations = append(allocations, allocation{
					buf:       buf,
					releaseAt: now.Add(time.Duration(operation.holdMs) * time.Millisecond),
				})
			}
		}

		// Clean up remaining allocations
		for _, alloc := range allocations {
			pool.Put(alloc.buf)
		}

		// Verify final state
		stats := pool.GetDetailedStats()
		if stats.Allocations != 0 {
			t.Fatalf("Expected 0 allocations at end, got %d", stats.Allocations)
		}
		
		// Verify we processed some operations successfully
		if stats.Hits == 0 && len(ops) > 0 {
			t.Fatalf("No successful allocations despite %d operations", len(ops))
		}
	})
}

// TestArenaPoolConcurrent tests concurrent access to arena pool.
func TestArenaPoolConcurrent(t *testing.T) {
	pool, err := NewArenaPool(100)
	if err != nil {
		t.Fatalf("Failed to create arena pool: %v", err)
	}

	numWorkers := 10
	allocsPerWorker := 20
	
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	
	worker := func(workerID int) {
		defer wg.Done()
		
		for i := 0; i < allocsPerWorker; i++ {
			// Vary sizes: 512KB to 4MB
			size := (512 + (workerID+i)*256) * 1024
			if size > 4*1024*1024 {
				size = 4*1024*1024
			}
			
			buf, err := pool.Get(size)
			if err == nil {
				// Write pattern to detect corruption
				pattern := byte(workerID&0xFF) ^ byte(i&0xFF)
				for j := 0; j < len(buf); j += 4096 {
					buf[j] = pattern
				}
				
				// Simulate some work
				time.Sleep(time.Microsecond * time.Duration(i%10))
				
				// Verify pattern
				for j := 0; j < len(buf); j += 4096 {
					if buf[j] != pattern {
						t.Errorf("Worker %d: Buffer corruption detected", workerID)
						return
					}
				}
				
				pool.Put(buf)
			}
		}
	}
	
	start := time.Now()
	for i := 0; i < numWorkers; i++ {
		go worker(i)
	}
	wg.Wait()
	duration := time.Since(start)
	
	stats := pool.GetDetailedStats()
	t.Logf("Concurrent test completed in %v", duration)
	t.Logf("Stats: Hits=%d, Misses=%d, Returns=%d, Peak Allocations=%d", 
		stats.Hits, stats.Misses, stats.Returns, stats.Allocations)
	t.Logf("Arena: Size=%d MB, Used=%d MB, Fragmentation=%d%%",
		stats.ArenaSize/(1024*1024), stats.UsedBytes/(1024*1024), stats.Fragmentation)
}

// BenchmarkArenaPool benchmarks arena pool performance.
func BenchmarkArenaPool(b *testing.B) {
	pool, err := NewArenaPool(1000)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	
	sizes := []int{
		256 * 1024,      // 256KB
		1024 * 1024,     // 1MB
		4 * 1024 * 1024, // 4MB
	}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf, err := pool.Get(size)
					if err != nil {
						b.Fatalf("Allocation failed: %v", err)
					}
					pool.Put(buf)
				}
			})
		})
	}
}