// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire_test

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

func TestExperimentalCFRangeWire(t *testing.T) {
	first := chainhash.Hash{0x12}
	last := chainhash.Hash{31: 0x34}
	request := wire.NewMsgGetCFRange(&first, &last)
	response := &wire.MsgCFRange{Data: []byte{0xab, 0xcd}}
	tests := []struct {
		msg     wire.Message
		command string
		payload []byte
	}{
		{request, "getcfrange", append(bytes.Clone(first[:]), last[:]...)},
		{response, "cfrange", []byte{2, 0xab, 0xcd}},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			var framed bytes.Buffer
			err := wire.WriteMessage(&framed, test.msg, wire.ProtocolVersion, wire.TestNet)
			require.NoError(t, err)
			command := make([]byte, wire.CommandSize)
			copy(command, test.command)
			require.Equal(t, command, framed.Bytes()[4:4+wire.CommandSize])
			_, msg, payload, err := wire.ReadMessageN(&framed, wire.ProtocolVersion, wire.TestNet)
			require.NoError(t, err)
			require.Equal(t, test.msg, msg)
			require.Equal(t, test.payload, payload)
		})
	}
}

func TestExperimentalCFRangeWireBounds(t *testing.T) {
	msg := &wire.MsgCFRange{Data: make([]byte, wire.MaxCFRangeDataSize)}
	var encoded bytes.Buffer
	require.NoError(t, msg.BtcEncode(&encoded, wire.ProtocolVersion, wire.BaseEncoding))
	require.EqualValues(t, msg.MaxPayloadLength(wire.ProtocolVersion), encoded.Len())
	var decoded wire.MsgCFRange
	require.NoError(t, decoded.BtcDecode(&encoded, wire.ProtocolVersion, wire.BaseEncoding))
	require.Equal(t, msg, &decoded)

	msg.Data = append(msg.Data, 0)
	require.Error(t, msg.BtcEncode(&encoded, wire.ProtocolVersion, wire.BaseEncoding))
	require.NoError(t, wire.WriteVarInt(&encoded, wire.ProtocolVersion, wire.MaxCFRangeDataSize+1))
	require.Error(t, decoded.BtcDecode(&encoded, wire.ProtocolVersion, wire.BaseEncoding))

	noncanonical := bytes.NewReader([]byte{0xfd, 1, 0, 0})
	require.Error(t, decoded.BtcDecode(noncanonical, wire.ProtocolVersion, wire.BaseEncoding))
}
