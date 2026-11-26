// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package progressivecf

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// MaxRangeBlocks bounds the inclusive interval in an experimental range
	// filter.
	MaxRangeBlocks = 16

	rangeVersion          = 1
	rangeHeaderSize       = 81
	rangeOverhead         = rangeHeaderSize + sha256.Size
	rangeCommitmentDomain = "fermion/range-filter/commitment/v1\x00"
)

// RangeDescriptor identifies the inclusive block interval of a range filter.
type RangeDescriptor struct {
	FirstHeight uint32
	LastHeight  uint32
	FirstHash   chainhash.Hash
	LastHash    chainhash.Hash
}

// DecodeRangeDescriptor validates a precomputed range filter's network,
// encoding, and commitment. Verifying its contents against chain scripts
// requires rebuilding the union independently.
func DecodeRangeDescriptor(object []byte, net wire.BitcoinNet) (RangeDescriptor, error) {
	if len(object) <= rangeOverhead || len(object) > wire.MaxCFRangeDataSize {
		return RangeDescriptor{}, fmt.Errorf("invalid range filter size: %d", len(object))
	}
	if object[0] != rangeVersion {
		return RangeDescriptor{}, fmt.Errorf("unsupported range filter version: %d", object[0])
	}
	objectNet := wire.BitcoinNet(binary.LittleEndian.Uint32(object[1:5]))
	if objectNet != net {
		return RangeDescriptor{}, fmt.Errorf("range filter uses network %v, expected %v", objectNet, net)
	}

	first := binary.LittleEndian.Uint32(object[5:9])
	last := binary.LittleEndian.Uint32(object[9:13])
	if first > last || last-first >= MaxRangeBlocks {
		return RangeDescriptor{}, fmt.Errorf("invalid range filter heights: %d through %d", first, last)
	}
	if dataLen := binary.LittleEndian.Uint32(object[77:rangeHeaderSize]); dataLen != uint32(len(object)-rangeOverhead) {
		return RangeDescriptor{}, errors.New("incorrect range filter GCS length")
	}

	end := len(object) - sha256.Size
	h := sha256.New()
	h.Write([]byte(rangeCommitmentDomain))
	h.Write(object[:end])
	if !bytes.Equal(h.Sum(nil), object[end:]) {
		return RangeDescriptor{}, errors.New("range filter commitment mismatch")
	}

	// Reuse the progressive producer's canonical BIP158 decoder at startup.
	if _, _, err := StageFromBasicFilter(object[rangeHeaderSize:end], 0); err != nil {
		return RangeDescriptor{}, fmt.Errorf("invalid range filter GCS: %w", err)
	}
	return RangeDescriptor{
		FirstHeight: first,
		LastHeight:  last,
		FirstHash:   chainhash.Hash(object[13:45]),
		LastHash:    chainhash.Hash(object[45:77]),
	}, nil
}
