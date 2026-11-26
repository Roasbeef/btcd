// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build rpctest

package integration

import (
	"crypto/sha256"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2/gcs"
	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

func TestPrecomputedCFRange(t *testing.T) {
	params := &chaincfg.SimNetParams
	genesis := params.GenesisHash

	// Simnet genesis has no spent outputs. Collect its basic-filter
	// output scripts to construct the one-block union independently.
	var scripts [][]byte
	seen := make(map[string]bool)
	for _, tx := range params.GenesisBlock.Transactions {
		for _, output := range tx.TxOut {
			script := output.PkScript
			if len(script) == 0 || script[0] == txscript.OP_RETURN ||
				seen[string(script)] {

				continue
			}
			seen[string(script)] = true
			scripts = append(scripts, script)
		}
	}
	require.NotEmpty(t, scripts)

	object := makeSingleBlockCFRange(t, params.Net, 0, genesis, scripts)
	dir := t.TempDir()
	path := filepath.Join(dir, "genesis.frc")
	require.NoError(t, os.WriteFile(path, object, 0600))

	// Also configure a well-formed object whose endpoint is off-chain.
	offChain := chainhash.Hash{0x19}
	offChainObject := makeSingleBlockCFRange(t, params.Net, 0, &offChain, scripts)
	offChainPath := filepath.Join(dir, "off-chain.frc")
	require.NoError(t, os.WriteFile(offChainPath, offChainObject, 0600))

	node, err := rpctest.New(params, nil, []string{
		"--experimentalprogressivecfilters",
		"--experimentalrangecfilter=" + path,
		"--experimentalrangecfilter=" + offChainPath,
	}, "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.TearDown()) })
	require.NoError(t, node.SetUp(false, 0))

	// Replacing the file after startup must not alter served bytes.
	require.NoError(t, os.WriteFile(path, nil, 0600))
	ready := make(chan struct{}, 1)
	messages := make(chan wire.Message, 2)
	p, err := peer.NewOutboundPeer(&peer.Config{
		ChainParams:      params,
		UserAgentName:    "range-test",
		UserAgentVersion: "1.0",
		Listeners: peer.MessageListeners{
			OnVerAck: func(_ *peer.Peer, _ *wire.MsgVerAck) {
				ready <- struct{}{}
			},
			OnCFRange: func(_ *peer.Peer, msg *wire.MsgCFRange) {
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

	// The pong follows the ignored off-chain request on the same connection.
	p.QueueMessage(wire.NewMsgGetCFRange(&offChain, &offChain), nil)
	p.QueueMessage(wire.NewMsgPing(2718), nil)
	pong, ok := receive().(*wire.MsgPong)
	require.True(t, ok)
	require.EqualValues(t, 2718, pong.Nonce)

	p.QueueMessage(wire.NewMsgGetCFRange(genesis, genesis), nil)
	response, ok := receive().(*wire.MsgCFRange)
	require.True(t, ok)
	require.Equal(t, object, response.Data)
}

func makeSingleBlockCFRange(t *testing.T, network wire.BitcoinNet, height uint32,
	hash *chainhash.Hash, scripts [][]byte) []byte {

	t.Helper()
	header := make([]byte, 81)
	header[0] = 1
	binary.LittleEndian.PutUint32(header[1:5], uint32(network))
	binary.LittleEndian.PutUint32(header[5:9], height)
	binary.LittleEndian.PutUint32(header[9:13], height)
	copy(header[13:45], hash[:])
	copy(header[45:77], hash[:])
	keyHash := sha256.Sum256(append([]byte("fermion/range-filter/key/v1\x00"), header[:77]...))
	key := [gcs.KeySize]byte(keyHash[:gcs.KeySize])
	filter, err := gcs.BuildGCSFilter(builder.DefaultP, builder.DefaultM, key, scripts)
	require.NoError(t, err)
	filterData, err := filter.NBytes()
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(header[77:81], uint32(len(filterData)))
	data := append(header, filterData...)
	commitment := sha256.Sum256(append([]byte("fermion/range-filter/commitment/v1\x00"), data...))
	return append(data, commitment[:]...)
}
