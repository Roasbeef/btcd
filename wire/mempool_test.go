// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestMemoryControllerPropertyBased uses property-based testing to verify
// memory controller behavior under various conditions.
func TestMemoryControllerPropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random configuration
		config := &MemoryConfig{
			MaxMemory:        rapid.Int64Range(1024*1024, 100*1024*1024).Draw(t, "maxMemory"),
			SmallPoolSize:    rapid.IntRange(10, 1000).Draw(t, "smallPoolSize"),
			MediumPoolSize:   rapid.IntRange(10, 500).Draw(t, "mediumPoolSize"),
			LargePoolSize:    rapid.IntRange(5, 100).Draw(t, "largePoolSize"),
			GlobalAllocRate:  rapid.IntRange(10, 1000).Draw(t, "globalAllocRate"),
			PerPeerAllocRate: rapid.IntRange(1, 100).Draw(t, "perPeerAllocRate"),
			EvictionInterval: 1 * time.Second,
			BufferMaxAge:     5 * time.Second,
		}

		mc, err := NewMemoryController(config)
		if err != nil {
			t.Fatalf("Failed to create memory controller: %v", err)
		}
		defer mc.Shutdown()

		// Generate allocation operations
		type allocOp struct {
			size  int
			hold  bool
			delay time.Duration
		}
		
		ops := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) allocOp {
			return allocOp{
				size:  rapid.IntRange(1, int(config.MaxMemory/10)).Draw(t, "allocSize"),
				hold:  rapid.Bool().Draw(t, "hold"),
				delay: time.Duration(rapid.Uint64Range(0, 100).Draw(t, "delay")) * time.Microsecond,
			}
		}), 1, 100).Draw(t, "operations")

		// Track allocations
		var allocations [][]byte
		var totalAllocated int64

		// Execute operations
		for i, op := range ops {
			buf, err := mc.Allocate(op.size)
			
			// Verify allocation respects memory limit
			if err == nil {
				if atomic.AddInt64(&totalAllocated, int64(cap(buf))) > config.MaxMemory {
					t.Fatalf("Operation %d: Allocation exceeded memory limit", i)
				}
				allocations = append(allocations, buf)
				
				// Verify buffer size
				if len(buf) != op.size {
					t.Fatalf("Operation %d: Buffer size mismatch: got %d, want %d", i, len(buf), op.size)
				}
				
				// Hold or release based on operation
				if !op.hold && len(allocations) > 0 {
					// Release oldest allocation
					old := allocations[0]
					allocations = allocations[1:]
					mc.Release(old)
					atomic.AddInt64(&totalAllocated, -int64(cap(old)))
				}
			}
			
			// Small delay between operations
			if op.delay > 0 {
				time.Sleep(op.delay)
			}
		}

		// Clean up remaining allocations
		for _, buf := range allocations {
			mc.Release(buf)
		}

		// Verify statistics consistency
		stats := mc.Stats()
		if stats.CurrentUsage < 0 {
			t.Fatalf("Negative current usage: %d", stats.CurrentUsage)
		}
		if stats.PeakUsage < stats.CurrentUsage {
			t.Fatalf("Peak usage less than current usage: peak=%d, current=%d", 
				stats.PeakUsage, stats.CurrentUsage)
		}
		if stats.TotalAllocations == 0 && len(ops) > 0 {
			t.Fatalf("No allocations recorded despite operations")
		}
	})
}

// TestMemoryControllerConcurrentPropertyBased tests concurrent allocation behavior.
func TestMemoryControllerConcurrentPropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Smaller limits for concurrent test
		config := &MemoryConfig{
			MaxMemory:        rapid.Int64Range(10*1024*1024, 50*1024*1024).Draw(t, "maxMemory"),
			SmallPoolSize:    100,
			GlobalAllocRate:  1000,
			PerPeerAllocRate: 50,
			EvictionInterval: 1 * time.Second,
			BufferMaxAge:     5 * time.Second,
		}

		mc, err := NewMemoryController(config)
		if err != nil {
			t.Fatalf("Failed to create memory controller: %v", err)
		}
		defer mc.Shutdown()

		// Number of concurrent workers
		numWorkers := rapid.IntRange(2, 10).Draw(t, "numWorkers")
		
		// Allocations per worker
		allocsPerWorker := rapid.IntRange(10, 50).Draw(t, "allocsPerWorker")

		// Synchronization
		var wg sync.WaitGroup
		var totalAllocated atomic.Int64
		var successfulAllocs atomic.Uint64
		var failedAllocs atomic.Uint64

		// Worker function
		worker := func(workerID int) {
			defer wg.Done()
			
			var localAllocs [][]byte
			
			for i := 0; i < allocsPerWorker; i++ {
				// Random size based on message type distribution
				var size int
				switch rapid.IntRange(0, 2).Example() {
				case 0: // Small (most common)
					size = rapid.IntRange(1, SmallBufferSize).Example()
				case 1: // Medium
					size = rapid.IntRange(SmallBufferSize+1, MediumBufferSize).Example()
				case 2: // Large (rare)
					size = rapid.IntRange(MediumBufferSize+1, 4*1024*1024).Example()
				}

				buf, err := mc.Allocate(size)
				if err == nil {
					successfulAllocs.Add(1)
					totalAllocated.Add(int64(cap(buf)))
					localAllocs = append(localAllocs, buf)
					
					// Hold briefly then maybe release
					time.Sleep(time.Microsecond * time.Duration(rapid.IntRange(1, 100).Example()))
					
					if rapid.Bool().Example() && len(localAllocs) > 0 {
						// Release random buffer
						idx := rapid.IntRange(0, len(localAllocs)-1).Example()
						mc.Release(localAllocs[idx])
						totalAllocated.Add(-int64(cap(localAllocs[idx])))
						localAllocs = append(localAllocs[:idx], localAllocs[idx+1:]...)
					}
				} else {
					failedAllocs.Add(1)
				}
			}
			
			// Clean up remaining allocations
			for _, buf := range localAllocs {
				mc.Release(buf)
				totalAllocated.Add(-int64(cap(buf)))
			}
		}

		// Start workers
		wg.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			go worker(i)
		}
		
		// Wait for completion
		wg.Wait()

		// Verify invariants
		stats := mc.Stats()
		
		// Memory usage should be close to zero after all releases
		if stats.CurrentUsage > int64(config.SmallPoolSize*SmallBufferSize) {
			t.Logf("Warning: High residual memory usage: %d", stats.CurrentUsage)
		}
		
		// Peak usage should never exceed limit
		if stats.PeakUsage > config.MaxMemory {
			t.Fatalf("Peak usage exceeded limit: %d > %d", stats.PeakUsage, config.MaxMemory)
		}
		
		// Total allocations should match successful allocations
		if stats.TotalAllocations != successfulAllocs.Load() {
			t.Fatalf("Allocation count mismatch: stats=%d, tracked=%d", 
				stats.TotalAllocations, successfulAllocs.Load())
		}

		t.Logf("Concurrent test completed: %d workers, %d successful, %d failed, peak=%d MB",
			numWorkers, successfulAllocs.Load(), failedAllocs.Load(), stats.PeakUsage/(1024*1024))
	})
}

// TestSlabPoolPropertyBased tests the slab pool implementation.
func TestSlabPoolPropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		slabCount := rapid.IntRange(10, 1000).Draw(t, "slabCount")
		slabSize := rapid.IntRange(1024, 8192).Draw(t, "slabSize")

		pool, err := NewSlabPool(slabCount, slabSize)
		if err != nil {
			t.Fatalf("Failed to create slab pool: %v", err)
		}

		// Track allocations
		allocations := make(map[*byte][]byte)
		
		// Allocation pattern
		numOps := rapid.IntRange(slabCount, slabCount*3).Draw(t, "numOps")
		
		for i := 0; i < numOps; i++ {
			if rapid.Bool().Draw(t, fmt.Sprintf("allocate_%d", i)) {
				// Allocate
				size := rapid.IntRange(1, slabSize).Draw(t, fmt.Sprintf("size_%d", i))
				buf, err := pool.Get(size)
				
				if len(allocations) < slabCount {
					// Should succeed
					if err != nil {
						t.Fatalf("Operation %d: Allocation failed when pool not full: %v", i, err)
					}
					if len(buf) != size {
						t.Fatalf("Operation %d: Size mismatch: got %d, want %d", i, len(buf), size)
					}
					if cap(buf) != slabSize {
						t.Fatalf("Operation %d: Capacity mismatch: got %d, want %d", i, cap(buf), slabSize)
					}
					allocations[&buf[0]] = buf
				} else {
					// Should fail
					if err == nil {
						t.Fatalf("Operation %d: Allocation succeeded when pool full", i)
					}
				}
			} else if len(allocations) > 0 {
				// Return random allocation
				for ptr, buf := range allocations {
					pool.Put(buf)
					delete(allocations, ptr)
					break
				}
			}
		}

		// Verify statistics
		stats := pool.Stats()
		if stats.Capacity != slabCount {
			t.Fatalf("Capacity mismatch: got %d, want %d", stats.Capacity, slabCount)
		}
		if stats.Size+len(allocations) != slabCount {
			t.Fatalf("Size accounting error: free=%d, allocated=%d, total=%d", 
				stats.Size, len(allocations), slabCount)
		}
	})
}

// TestMemoryPressurePropertyBased tests behavior under memory pressure.
func TestMemoryPressurePropertyBased(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Small memory limit to induce pressure
		config := &MemoryConfig{
			MaxMemory:        rapid.Int64Range(1*1024*1024, 10*1024*1024).Draw(t, "maxMemory"),
			SmallPoolSize:    10,
			GlobalAllocRate:  100,
			PerPeerAllocRate: 10,
			EvictionInterval: 100 * time.Millisecond,
			BufferMaxAge:     500 * time.Millisecond,
		}

		mc, err := NewMemoryController(config)
		if err != nil {
			t.Fatalf("Failed to create memory controller: %v", err)
		}
		defer mc.Shutdown()

		// Allocate until we hit the limit
		var allocations [][]byte
		var totalSize int64
		
		for totalSize < config.MaxMemory {
			size := rapid.IntRange(1024, int(config.MaxMemory/10)).Draw(t, "allocSize")
			
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			buf, err := mc.AllocateWithContext(ctx, size)
			cancel()
			
			if err == nil {
				allocations = append(allocations, buf)
				totalSize += int64(cap(buf))
			} else {
				// Should only fail near the limit
				if totalSize < config.MaxMemory/2 {
					t.Fatalf("Allocation failed too early: used=%d, limit=%d", totalSize, config.MaxMemory)
				}
				break
			}
		}

		// Verify we're near the limit
		stats := mc.Stats()
		utilizationPct := float64(stats.CurrentUsage) / float64(config.MaxMemory) * 100
		if utilizationPct < 80 {
			t.Fatalf("Poor memory utilization: %.1f%%", utilizationPct)
		}

		// Test that new allocations block
		blocked := make(chan bool, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := mc.AllocateWithContext(ctx, 1024*1024)
			blocked <- (err != nil)
		}()

		// Should timeout since we're at limit
		select {
		case wasBlocked := <-blocked:
			if !wasBlocked {
				t.Fatalf("Allocation should have been blocked at memory limit")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Allocation blocking test timed out")
		}

		// Release some memory
		if len(allocations) > 0 {
			for i := 0; i < len(allocations)/2; i++ {
				mc.Release(allocations[i])
			}
		}

		// Now allocation should succeed
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err = mc.AllocateWithContext(ctx, 1024)
		cancel()
		if err != nil {
			t.Fatalf("Allocation failed after releasing memory: %v", err)
		}
	})
}

// AllocateWithContext is a helper that adds context support to Allocate.
func (mc *MemoryController) AllocateWithContext(ctx context.Context, size int) ([]byte, error) {
	done := make(chan struct{})
	var buf []byte
	var err error
	
	go func() {
		buf, err = mc.Allocate(size)
		close(done)
	}()
	
	select {
	case <-done:
		return buf, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// BenchmarkMemoryController benchmarks allocation performance.
func BenchmarkMemoryController(b *testing.B) {
	config := DefaultMemoryConfig()
	mc, err := NewMemoryController(config)
	if err != nil {
		b.Fatalf("Failed to create memory controller: %v", err)
	}
	defer mc.Shutdown()

	sizes := []int{
		256,              // Very small
		1024,             // Small
		4 * 1024,         // Small (max)
		16 * 1024,        // Medium
		64 * 1024,        // Medium (max)
		1024 * 1024,      // Large (1MB)
		4 * 1024 * 1024,  // Large (4MB - block size)
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ResetTimer()
			
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf, err := mc.Allocate(size)
					if err != nil {
						b.Fatalf("Allocation failed: %v", err)
					}
					mc.Release(buf)
				}
			})
		})
	}
}

// TestMemoryStatisticsTracking verifies memory statistics using runtime.ReadMemStats.
func TestMemoryStatisticsTracking(t *testing.T) {
	// Force GC to get clean baseline
	runtime.GC()
	runtime.GC()
	
	var baseStats runtime.MemStats
	runtime.ReadMemStats(&baseStats)

	config := &MemoryConfig{
		MaxMemory:     100 * 1024 * 1024, // 100MB
		SmallPoolSize: 1000,
		GlobalAllocRate: 1000,
	}

	mc, err := NewMemoryController(config)
	if err != nil {
		t.Fatalf("Failed to create memory controller: %v", err)
	}
	defer mc.Shutdown()

	// Let pools initialize
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	
	var afterInitStats runtime.MemStats
	runtime.ReadMemStats(&afterInitStats)

	// Pool initialization should have allocated memory
	poolMemory := int64(config.SmallPoolSize * SmallBufferSize)
	actualIncrease := int64(afterInitStats.HeapAlloc) - int64(baseStats.HeapAlloc)
	
	// Allow some overhead (metadata, etc)
	if actualIncrease < poolMemory || actualIncrease > poolMemory*2 {
		t.Logf("Warning: Unexpected memory increase after pool init: expected ~%d, got %d", 
			poolMemory, actualIncrease)
	}

	// Test allocation tracking
	allocSizes := []int{1024, 2048, 4096}
	var allocations [][]byte
	var expectedUsage int64

	for _, size := range allocSizes {
		buf, err := mc.Allocate(size)
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		allocations = append(allocations, buf)
		expectedUsage += int64(cap(buf))
	}

	// Verify controller's view matches expected
	stats := mc.Stats()
	if stats.CurrentUsage != expectedUsage {
		t.Errorf("Current usage mismatch: got %d, want %d", stats.CurrentUsage, expectedUsage)
	}

	// Release all and verify
	for _, buf := range allocations {
		mc.Release(buf)
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	stats = mc.Stats()
	if stats.CurrentUsage != 0 {
		t.Errorf("Current usage after release: got %d, want 0", stats.CurrentUsage)
	}

	// Verify system memory stats
	current, sysStats := mc.GetMemoryUsage()
	if current != 0 {
		t.Errorf("GetMemoryUsage current mismatch: got %d, want 0", current)
	}
	
	t.Logf("System memory stats: HeapAlloc=%d MB, HeapInuse=%d MB, HeapReleased=%d MB",
		sysStats.HeapAlloc/(1024*1024), 
		sysStats.HeapInuse/(1024*1024),
		sysStats.HeapReleased/(1024*1024))
}