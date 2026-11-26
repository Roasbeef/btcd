// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/internal/progressivecf"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
)

const maxExperimentalCFRanges = 16

type indexedCFRange struct {
	firstHeight uint32
	lastHeight  uint32
	data        []byte
}

func loadCFRanges(paths []string, net wire.BitcoinNet) (
	map[wire.MsgGetCFRange]indexedCFRange, error) {

	if len(paths) == 0 {
		return nil, nil
	}
	if len(paths) > maxExperimentalCFRanges {
		return nil, fmt.Errorf("too many range filter files: %d", len(paths))
	}
	ranges := make(map[wire.MsgGetCFRange]indexedCFRange, len(paths))
	for _, path := range paths {
		data, err := readCFRangeFile(path)
		if err != nil {
			return nil, fmt.Errorf("range filter %q: %w", path, err)
		}
		descriptor, err := progressivecf.DecodeRangeDescriptor(data, net)
		if err != nil {
			return nil, fmt.Errorf("range filter %q: %w", path, err)
		}
		key := wire.MsgGetCFRange{
			FirstHash: descriptor.FirstHash,
			LastHash:  descriptor.LastHash,
		}
		if _, exists := ranges[key]; exists {
			return nil, fmt.Errorf("range filter %q repeats configured endpoints", path)
		}
		ranges[key] = indexedCFRange{
			firstHeight: descriptor.FirstHeight,
			lastHeight:  descriptor.LastHeight,
			data:        data,
		}
	}
	return ranges, nil
}

func readCFRangeFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > wire.MaxCFRangeDataSize {
		return nil, fmt.Errorf("expected a regular file of at most %d bytes",
			wire.MaxCFRangeDataSize)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, wire.MaxCFRangeDataSize+1))
}

// OnGetCFRange serves a configured range while its endpoints are on the active
// chain at their recorded heights.
func (sp *serverPeer) OnGetCFRange(p *peer.Peer, msg *wire.MsgGetCFRange) {
	s := sp.server
	entry, ok := s.cfRanges[*msg]
	if !ok {
		return
	}
	select {
	case s.experimentalCFRequests <- struct{}{}:
		defer func() { <-s.experimentalCFRequests }()
	default:
		return
	}

	lastHeight, err := s.chain.BlockHeightByHash(&msg.LastHash)
	if err != nil || uint32(lastHeight) != entry.lastHeight {
		return
	}
	hashes, err := s.chain.HeightToHashRange(
		int32(entry.firstHeight), &msg.LastHash, progressivecf.MaxRangeBlocks,
	)
	if err != nil || len(hashes) == 0 || hashes[0] != msg.FirstHash ||
		!s.chain.MainChainHasBlock(&msg.LastHash) {

		return
	}
	s.sendExperimentalCF(p, &wire.MsgCFRange{Data: entry.data})
}
