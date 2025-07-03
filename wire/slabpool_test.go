// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"testing"
)

// TestSlabPoolBasic tests basic slab pool functionality.
func TestSlabPoolBasic(t *testing.T) {
	slabCount := 10
	slabSize := 1024

	pool, err := NewSlabPool(slabCount, slabSize)
	if err != nil {
		t.Fatalf("Failed to create slab pool: %v", err)
	}

	// Test allocation
	buf, err := pool.Get(512)
	if err != nil {
		t.Fatalf("Failed to get buffer: %v", err)
	}

	t.Logf("Buffer len=%d, cap=%d, expected slab size=%d", len(buf), cap(buf), slabSize)

	if len(buf) != 512 {
		t.Errorf("Buffer length mismatch: got %d, want 512", len(buf))
	}

	if cap(buf) != slabSize {
		t.Errorf("Buffer capacity mismatch: got %d, want %d", cap(buf), slabSize)
	}

	// Check pool stats
	stats := pool.Stats()
	t.Logf("Pool stats: Capacity=%d, Size=%d, InUse=%d", stats.Capacity, stats.Size, pool.inUse.Load())

	if stats.Capacity != slabCount {
		t.Errorf("Pool capacity mismatch: got %d, want %d", stats.Capacity, slabCount)
	}
}