// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build rpctest

package integration

import (
	"net"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/internal/progressivecf"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

func TestProgressiveCFStages(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil,
		[]string{"--experimentalprogressivecfilters"}, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.TearDown()) })
	require.NoError(t, node.SetUp(false, 0))
	hashes, err := node.Client.Generate(2)
	require.NoError(t, err)

	ready := make(chan struct{}, 1)
	messages := make(chan wire.Message, 4)
	p, err := peer.NewOutboundPeer(&peer.Config{
		ChainParams:      &chaincfg.SimNetParams,
		UserAgentName:    "progressive-test",
		UserAgentVersion: "1.0",
		Listeners: peer.MessageListeners{
			OnVerAck: func(_ *peer.Peer, _ *wire.MsgVerAck) {
				ready <- struct{}{}
			},
			OnCFStage: func(_ *peer.Peer, msg *wire.MsgCFStage) {
				messages <- msg
			},
			OnCFilter: func(_ *peer.Peer, msg *wire.MsgCFilter) {
				messages <- msg
			},
			OnPong: func(_ *peer.Peer, msg *wire.MsgPong) {
				messages <- msg
			},
		},
	}, node.P2PAddress())
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", node.P2PAddress(), 5*time.Second)
	require.NoError(t, err)
	p.AssociateConnection(conn)
	t.Cleanup(p.Disconnect)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the peer handshake")
	}
	receive := func() wire.Message {
		t.Helper()
		select {
		case msg := <-messages:
			return msg
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a peer response")
			return nil
		}
	}

	// Retrieve both baseline BIP157 filters through the same peer.
	p.QueueMessage(wire.NewMsgGetCFilters(wire.GCSFilterRegular, 1, hashes[1]), nil)
	baseline := make(map[chainhash.Hash][]byte, len(hashes))
	for _, hash := range hashes {
		msg, ok := receive().(*wire.MsgCFilter)
		require.True(t, ok)
		require.Equal(t, *hash, msg.BlockHash)
		baseline[*hash] = msg.Data
	}

	// Group deliberately queries in reverse block-height order.
	reversed := []chainhash.Hash{*hashes[1], *hashes[0]}
	for stage := uint8(0); stage <= wire.MaxCFStage; stage++ {
		p.QueueMessage(wire.NewMsgGetCFStages(reversed, stage), nil)
		for _, hash := range reversed {
			n, data, err := progressivecf.StageFromBasicFilter(baseline[hash], stage)
			require.NoError(t, err)
			msg, ok := receive().(*wire.MsgCFStage)
			require.True(t, ok)
			require.Equal(t, hash, msg.BlockHash)
			require.Equal(t, stage, msg.Stage)
			require.Equal(t, n, msg.N)
			require.Equal(t, data, msg.Data)
		}
	}

	// A mixed group with a missing hash yields no partial response. The
	// pong confirms that the peer processed the group and stayed connected.
	p.QueueMessage(wire.NewMsgGetCFStages(
		[]chainhash.Hash{*hashes[0], {}}, 0,
	), nil)
	p.QueueMessage(wire.NewMsgPing(31337), nil)
	pong, ok := receive().(*wire.MsgPong)
	require.True(t, ok)
	require.EqualValues(t, 31337, pong.Nonce)

	p.QueueMessage(wire.NewMsgGetCFStage(hashes[0], 1), nil)
	single, ok := receive().(*wire.MsgCFStage)
	require.True(t, ok)
	n, data, err := progressivecf.StageFromBasicFilter(baseline[*hashes[0]], 1)
	require.NoError(t, err)
	require.Equal(t, &wire.MsgCFStage{
		BlockHash: *hashes[0], Stage: 1, N: n, Data: data,
	}, single)
}
