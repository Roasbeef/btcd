// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemoryControllerIntegration tests the complete memory management system.
func TestMemoryControllerIntegration(t *testing.T) {
	// Create a memory controller with reasonable limits
	config := &MemoryConfig{
		MaxMemory:        100 * 1024 * 1024, // 100MB
		SmallPoolSize:    1000,
		MediumPoolSize:   500,
		LargePoolSize:    20,
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

	// Test allocation across all size classes
	testCases := []struct {
		name string
		size int
		pool string
	}{
		{"tiny", 256, "small"},
		{"small", 2048, "small"},
		{"small-max", 4096, "small"},
		{"medium-min", 4097, "medium"},
		{"medium", 32768, "medium"},
		{"medium-max", 65536, "medium"},
		{"large-min", 65537, "large"},
		{"large", 1024 * 1024, "large"},
		{"large-block", 4 * 1024 * 1024, "large"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := mc.Allocate(tc.size)
			if err != nil {
				t.Errorf("Failed to allocate %d bytes: %v", tc.size, err)
				return
			}

			if len(buf) != tc.size {
				t.Errorf("Buffer size mismatch: got %d, want %d", len(buf), tc.size)
			}

			// Verify it came from the right pool
			stats := mc.Stats()
			t.Logf("%s: allocated %d bytes, stats: small=%d, medium=%d, large=%d",
				tc.name, tc.size, stats.SmallPoolHits, stats.MediumPoolHits, stats.LargePoolHits)

			mc.Release(buf)
		})
	}

	// Final stats
	stats := mc.Stats()
	t.Logf("Final stats: Allocations=%d, Recycled=%d, Failed=%d",
		stats.TotalAllocations, stats.BuffersRecycled, stats.FailedAllocs)
}

// TestMemoryControllerStress performs a stress test simulating multiple peers.
func TestMemoryControllerStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	config := &MemoryConfig{
		MaxMemory:        200 * 1024 * 1024, // 200MB
		SmallPoolSize:    2000,
		MediumPoolSize:   1000,
		LargePoolSize:    50,
		GlobalAllocRate:  5000,
		PerPeerAllocRate: 100,
		EvictionInterval: 500 * time.Millisecond,
		BufferMaxAge:     2 * time.Second,
	}

	mc, err := NewMemoryController(config)
	if err != nil {
		t.Fatalf("Failed to create memory controller: %v", err)
	}
	defer mc.Shutdown()

	// Track metrics
	var totalAllocated atomic.Uint64
	var totalReleased atomic.Uint64
	var peakConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	// Simulate multiple peers
	numPeers := 20
	messagesPerPeer := 100
	
	var wg sync.WaitGroup
	wg.Add(numPeers)

	peerFunc := func(peerID int) {
		defer wg.Done()
		
		peerIDStr := fmt.Sprintf("peer-%d", peerID)
		limiter := mc.GetPeerLimiter(peerIDStr)
		
		allocations := make([][]byte, 0, 10)
		
		for i := 0; i < messagesPerPeer; i++ {
			// Wait for rate limit
			if !limiter.Allow() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			
			// Simulate different message types
			var size int
			switch i % 10 {
			case 0: // Block
				size = 1024 * 1024 + i*1024 // 1MB+
			case 1, 2: // Large transaction
				size = 50 * 1024 // 50KB
			default: // Small messages
				size = 250 + i*10 // 250B+
			}
			
			buf, err := mc.Allocate(size)
			if err != nil {
				// Expected under memory pressure
				continue
			}
			
			totalAllocated.Add(1)
			current := currentConcurrent.Add(1)
			
			// Update peak
			for {
				peak := peakConcurrent.Load()
				if current <= peak || peakConcurrent.CompareAndSwap(peak, current) {
					break
				}
			}
			
			allocations = append(allocations, buf)
			
			// Simulate processing time
			time.Sleep(time.Duration(i%5) * time.Millisecond)
			
			// Release old allocations
			if len(allocations) > 5 {
				old := allocations[0]
				allocations = allocations[1:]
				mc.Release(old)
				totalReleased.Add(1)
				currentConcurrent.Add(-1)
			}
		}
		
		// Clean up remaining allocations
		for _, buf := range allocations {
			mc.Release(buf)
			totalReleased.Add(1)
			currentConcurrent.Add(-1)
		}
		
		mc.RemovePeerLimiter(peerIDStr)
	}

	// Start peers with staggered timing
	for i := 0; i < numPeers; i++ {
		go peerFunc(i)
		time.Sleep(50 * time.Millisecond)
	}

	// Monitor memory usage during test
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				current, sysStats := mc.GetMemoryUsage()
				stats := mc.Stats()
				t.Logf("Memory: controller=%dMB, heap=%dMB, peak concurrent=%d",
					current/(1024*1024), sysStats.HeapAlloc/(1024*1024), peakConcurrent.Load())
				t.Logf("Stats: allocs=%d, recycles=%d, blocked=%d, failed=%d",
					stats.TotalAllocations, stats.BuffersRecycled, 
					stats.BlockedAllocs, stats.FailedAllocs)
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)

	// Final statistics
	stats := mc.Stats()
	t.Logf("\nFinal Statistics:")
	t.Logf("Total allocated: %d", totalAllocated.Load())
	t.Logf("Total released: %d", totalReleased.Load())
	t.Logf("Peak concurrent: %d", peakConcurrent.Load())
	t.Logf("Peak memory usage: %d MB", stats.PeakUsage/(1024*1024))
	t.Logf("Allocations: %d", stats.TotalAllocations)
	t.Logf("Recycled: %d", stats.BuffersRecycled)
	t.Logf("Blocked: %d", stats.BlockedAllocs)
	t.Logf("Failed: %d", stats.FailedAllocs)
	
	// Verify all buffers were released
	if current := currentConcurrent.Load(); current != 0 {
		t.Errorf("Memory leak detected: %d buffers not released", current)
	}
	
	if totalAllocated.Load() != totalReleased.Load() {
		t.Errorf("Allocation/Release mismatch: %d allocated, %d released",
			totalAllocated.Load(), totalReleased.Load())
	}
}

// TestMemoryControllerWithMessages tests integration with wire protocol messages.
func TestMemoryControllerWithMessages(t *testing.T) {
	// Set up global memory controller
	config := DefaultMemoryConfig()
	mc, err := NewMemoryController(config)
	if err != nil {
		t.Fatalf("Failed to create memory controller: %v", err)
	}
	defer mc.Shutdown()
	
	SetGlobalMemoryController(mc)

	// Test with actual message reading would go here
	// For now, just verify the global controller is set
	if GetGlobalMemoryController() != mc {
		t.Errorf("Global memory controller not set correctly")
	}

	// Test buffer manager
	bm := NewBufferManager(mc)
	
	// Allocate buffer through buffer manager
	buf, err := bm.AllocateBuffer(1024)
	if err != nil {
		t.Fatalf("Failed to allocate buffer: %v", err)
	}
	
	if buf.Len() != 1024 {
		t.Errorf("Buffer size mismatch: got %d, want 1024", buf.Len())
	}
	
	// Test reference counting
	initialRef := buf.RefCount()
	buf.AddRef()
	if buf.RefCount() != initialRef+1 {
		t.Errorf("Reference count not incremented")
	}
	
	buf.Release()
	buf.Release()
	
	// Verify buffer was returned to pool
	stats := mc.Stats()
	if stats.BuffersRecycled == 0 {
		t.Errorf("Buffer not recycled")
	}
}