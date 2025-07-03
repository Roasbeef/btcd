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

// TestSizeClassPoolBasic tests basic size class pool functionality.
func TestSizeClassPoolBasic(t *testing.T) {
	pool, err := NewSizeClassPool(100)
	if err != nil {
		t.Fatalf("Failed to create size class pool: %v", err)
	}

	// Test allocation for each size class
	for _, size := range []int{1024, 4096, 8192, 16384, 32768, 65536} {
		buf, err := pool.Get(size)
		if err != nil {
			t.Errorf("Failed to get buffer of size %d: %v", size, err)
			continue
		}

		if len(buf) != size {
			t.Errorf("Buffer length mismatch: got %d, want %d", len(buf), size)
		}

		// Find which size class it came from
		actualSize := cap(buf)
		found := false
		for _, classSize := range sizeClasses {
			if actualSize == classSize && size <= classSize {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Buffer capacity %d doesn't match any size class", actualSize)
		}

		// Return buffer
		pool.Put(buf)
	}

	// Check stats
	stats := pool.Stats()
	if stats.Returns != 6 {
		t.Errorf("Expected 6 returns, got %d", stats.Returns)
	}
}

// TestSizeClassPoolPropertyBased tests the size class pool with property-based testing.
func TestSizeClassPoolPropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		totalCapacity := rapid.IntRange(50, 500).Draw(t, "totalCapacity")
		
		pool, err := NewSizeClassPool(totalCapacity)
		if err != nil {
			t.Fatalf("Failed to create size class pool: %v", err)
		}

		// Generate allocation operations
		type op struct {
			size int
			hold bool
		}
		
		ops := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) op {
			// Generate sizes that fit in our size classes
			sizeIdx := rapid.IntRange(0, len(sizeClasses)-1).Draw(t, "sizeIdx")
			maxSize := sizeClasses[sizeIdx]
			minSize := 1
			if sizeIdx > 0 {
				minSize = sizeClasses[sizeIdx-1] + 1
			}
			
			return op{
				size: rapid.IntRange(minSize, maxSize).Draw(t, "size"),
				hold: rapid.Bool().Draw(t, "hold"),
			}
		}), 10, 200).Draw(t, "operations")

		// Track allocations
		allocations := make([][]byte, 0)
		
		// Execute operations
		for i, operation := range ops {
			buf, err := pool.Get(operation.size)
			
			if err == nil {
				// Verify buffer properties
				if len(buf) != operation.size {
					t.Fatalf("Op %d: Buffer size mismatch: got %d, want %d", 
						i, len(buf), operation.size)
				}
				
				// Verify it came from correct size class
				capacity := cap(buf)
				validClass := false
				for _, classSize := range sizeClasses {
					if capacity == classSize && operation.size <= classSize {
						validClass = true
						break
					}
				}
				if !validClass {
					t.Fatalf("Op %d: Invalid capacity %d for size %d", 
						i, capacity, operation.size)
				}
				
				if operation.hold {
					allocations = append(allocations, buf)
				} else {
					pool.Put(buf)
				}
			}
		}

		// Return all held buffers
		for _, buf := range allocations {
			pool.Put(buf)
		}

		// Verify stats consistency
		stats := pool.Stats()
		if stats.Hits+stats.Misses == 0 && len(ops) > 0 {
			t.Fatalf("No allocations recorded despite operations")
		}
		
		// Detailed stats check
		detailed := pool.DetailedStats()
		totalCap := 0
		for _, bucketStats := range detailed {
			totalCap += bucketStats.Capacity
		}
		// Allow some rounding difference due to weighted distribution
		if totalCap < totalCapacity-len(sizeClasses) || totalCap > totalCapacity {
			t.Fatalf("Total capacity mismatch: got %d, want ~%d", totalCap, totalCapacity)
		}
	})
}

// TestSizeClassPoolConcurrent tests concurrent access to size class pool.
func TestSizeClassPoolConcurrent(t *testing.T) {
	pool, err := NewSizeClassPool(1000)
	if err != nil {
		t.Fatalf("Failed to create size class pool: %v", err)
	}

	numWorkers := 10
	allocsPerWorker := 100
	
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	
	worker := func(workerID int) {
		defer wg.Done()
		
		for i := 0; i < allocsPerWorker; i++ {
			// Vary sizes across workers
			sizeIdx := (workerID + i) % len(sizeClasses)
			size := sizeClasses[sizeIdx] / 2 // Use half the class size
			
			buf, err := pool.Get(size)
			if err == nil {
				// Simulate some work
				time.Sleep(time.Microsecond)
				
				// Write pattern to detect corruption
				pattern := byte(workerID&0xFF) ^ byte(i&0xFF)
				for j := 0; j < len(buf); j += 1024 {
					buf[j] = pattern
				}
				
				// Verify pattern
				for j := 0; j < len(buf); j += 1024 {
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
	
	stats := pool.Stats()
	t.Logf("Concurrent test completed in %v", duration)
	t.Logf("Stats: Hits=%d, Misses=%d, Returns=%d", 
		stats.Hits, stats.Misses, stats.Returns)
	
	// Most allocations should succeed
	successRate := float64(stats.Hits) / float64(stats.Hits+stats.Misses) * 100
	if successRate < 90 {
		t.Errorf("Low success rate: %.1f%% (hits=%d, misses=%d)", 
			successRate, stats.Hits, stats.Misses)
	}
}

// TestSizeClassPoolEviction tests buffer eviction.
func TestSizeClassPoolEviction(t *testing.T) {
	pool, err := NewSizeClassPool(100)
	if err != nil {
		t.Fatalf("Failed to create size class pool: %v", err)
	}

	// Allocate and return some buffers
	buffers := make([][]byte, 20)
	for i := range buffers {
		size := 8192 // 8KB
		buf, err := pool.Get(size)
		if err != nil {
			t.Fatalf("Failed to allocate buffer %d: %v", i, err)
		}
		buffers[i] = buf
	}
	
	// Return all buffers
	for _, buf := range buffers {
		pool.Put(buf)
	}
	
	// Wait a bit
	time.Sleep(100 * time.Millisecond)
	
	// Evict old buffers
	cutoff := time.Now().Unix() + 1 // Future time, should evict all
	evicted := pool.EvictOlderThan(cutoff)
	
	if evicted == 0 {
		t.Errorf("Expected some buffers to be evicted, got 0")
	}
	
	stats := pool.Stats()
	if stats.Evictions != uint64(evicted) {
		t.Errorf("Eviction count mismatch: stats=%d, returned=%d", 
			stats.Evictions, evicted)
	}
}

// BenchmarkSizeClassPool benchmarks size class pool performance.
func BenchmarkSizeClassPool(b *testing.B) {
	pool, err := NewSizeClassPool(10000)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	
	sizes := []int{4096, 8192, 16384, 32768, 65536}
	
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