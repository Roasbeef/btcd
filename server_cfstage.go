// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/internal/progressivecf"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	maxActiveCFStageRequests = 4
	cfStageWriteTimeout      = 10 * time.Second
)

// OnGetCFStage serves one stage from the committed filter index.
func (sp *serverPeer) OnGetCFStage(p *peer.Peer, msg *wire.MsgGetCFStage) {
	sp.serveCFStages(p, []chainhash.Hash{msg.BlockHash}, msg.Stage)
}

// OnGetCFStages serves the same stage for each requested block in order.
func (sp *serverPeer) OnGetCFStages(p *peer.Peer, msg *wire.MsgGetCFStages) {
	sp.serveCFStages(p, msg.BlockHashes, msg.Stage)
}

func (sp *serverPeer) serveCFStages(p *peer.Peer, hashes []chainhash.Hash, stage uint8) {
	s := sp.server
	select {
	case s.cfStageRequests <- struct{}{}:
		defer func() { <-s.cfStageRequests }()
	default:
		return
	}

	// Fetch the connected blocks' filters in a single index snapshot,
	// so a reorg cannot cause the lookups to see different chain views.
	hashPtrs := make([]*chainhash.Hash, len(hashes))
	for i := range hashes {
		hashPtrs[i] = &hashes[i]
	}
	filters, err := s.cfIndex.FiltersByBlockHashes(hashPtrs, wire.GCSFilterRegular)
	if err != nil {
		peerLog.Errorf("Error fetching indexed filters for cfstage: %v", err)
		return
	}

	// Build every response before sending the first one, so an unavailable
	// entry does not produce a partial group.
	responses := make([]wire.MsgCFStage, len(filters))
	for i, filter := range filters {
		if len(filter) == 0 {
			return
		}
		n, data, err := progressivecf.StageFromBasicFilter(filter, stage)
		if err != nil {
			peerLog.Debugf("Cannot serve stage %d for %v: %v", stage,
				hashes[i], err)
			return
		}
		responses[i] = wire.MsgCFStage{
			BlockHash: hashes[i],
			Stage:     stage,
			N:         n,
			Data:      data,
		}
	}

	// Wait for each write, bounding the output queue for peers that send
	// requests faster than they read responses. Give the entire group one
	// deadline and disconnect if the write cannot finish in time.
	timer := time.NewTimer(cfStageWriteTimeout)
	defer timer.Stop()
	done := make(chan struct{}, 1)
	for i := range responses {
		p.QueueMessage(&responses[i], done)
		select {
		case <-done:
		case <-p.Done():
			return
		case <-s.quit:
			return
		case <-timer.C:
			p.Disconnect()
			return
		}
	}
}
