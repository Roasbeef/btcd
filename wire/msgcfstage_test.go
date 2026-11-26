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

func TestProgressiveCFilterFraming(t *testing.T) {
	first := chainhash.Hash{0xab, 0xc1}
	second := chainhash.Hash{31: 0xd2}

	tests := []struct {
		command string
		msg     wire.Message
		payload []byte
	}{
		{
			command: "getcfstage",
			msg:     wire.NewMsgGetCFStage(&first, 3),
			payload: append(bytes.Clone(first[:]), 3),
		},
		{
			command: "getcfstages",
			msg:     wire.NewMsgGetCFStages([]chainhash.Hash{second, first}, 1),
			payload: append(append([]byte{1, 2}, second[:]...), first[:]...),
		},
		{
			command: "cfstage",
			msg: &wire.MsgCFStage{BlockHash: first, Stage: 0,
				N: 253, Data: []byte{0xff, 0x20}},
			payload: append(bytes.Clone(first[:]), 0, 0xfd, 0xfd, 0, 2, 0xff, 0x20),
		},
		{
			command: "cfstage",
			msg: &wire.MsgCFStage{BlockHash: first, Stage: 3,
				N: 0, Data: []byte{}},
			payload: append(bytes.Clone(first[:]), 3, 0, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			var framed bytes.Buffer
			require.NoError(t, wire.WriteMessage(&framed, test.msg,
				wire.ProtocolVersion, wire.TestNet))
			wantCommand := make([]byte, wire.CommandSize)
			copy(wantCommand, test.command)
			require.Equal(t, wantCommand, framed.Bytes()[4:4+wire.CommandSize])
			require.Equal(t, test.payload, framed.Bytes()[wire.MessageHeaderSize:])

			_, msg, payload, err := wire.ReadMessageN(&framed, wire.ProtocolVersion, wire.TestNet)
			require.NoError(t, err)
			require.Equal(t, test.payload, payload)
			require.Equal(t, test.msg, msg)

			_, err = wire.WriteV2MessageN(&framed, test.msg, wire.ProtocolVersion, wire.BaseEncoding)
			require.NoError(t, err)
			require.Equal(t, append([]byte{0}, wantCommand...),
				framed.Bytes()[:1+wire.CommandSize])
			msg, payload, err = wire.ReadV2MessageN(framed.Bytes(), wire.ProtocolVersion, wire.BaseEncoding)
			require.NoError(t, err)
			require.Equal(t, test.payload, payload)
			require.Equal(t, test.msg, msg)
		})
	}
}

func TestProgressiveCFilterWireBounds(t *testing.T) {
	hashes := make([]chainhash.Hash, wire.MaxGetCFStagesHashes+1)
	for i := range hashes {
		hashes[i][0] = byte(i)
	}
	response := &wire.MsgCFStage{
		N:    wire.MaxCFStageElements,
		Data: make([]byte, wire.MaxCFStageDataSize),
	}
	group := wire.NewMsgGetCFStages(hashes[:wire.MaxGetCFStagesHashes], 3)
	tests := []struct {
		valid   wire.Message
		decoded wire.Message
	}{
		{response, &wire.MsgCFStage{}},
		{group, &wire.MsgGetCFStages{}},
	}
	for _, test := range tests {
		var payload bytes.Buffer
		require.NoError(t, test.valid.BtcEncode(&payload, wire.ProtocolVersion, wire.BaseEncoding))
		require.EqualValues(t, test.valid.MaxPayloadLength(wire.ProtocolVersion), payload.Len())
		require.NoError(t, test.decoded.BtcDecode(&payload, wire.ProtocolVersion, wire.BaseEncoding))
		require.Equal(t, test.valid, test.decoded)
	}
	invalid := []wire.Message{
		&wire.MsgGetCFStage{Stage: 4},
		wire.NewMsgGetCFStages(hashes[:1], 4),
		wire.NewMsgGetCFStages(nil, 0),
		wire.NewMsgGetCFStages(hashes, 0),
		wire.NewMsgGetCFStages([]chainhash.Hash{hashes[1], hashes[1]}, 0),
		&wire.MsgCFStage{Stage: 4},
		&wire.MsgCFStage{N: wire.MaxCFStageElements + 1},
		&wire.MsgCFStage{Data: make([]byte, wire.MaxCFStageDataSize+1)},
	}
	for _, msg := range invalid {
		var payload bytes.Buffer
		require.Error(t, msg.BtcEncode(&payload, wire.ProtocolVersion, wire.BaseEncoding))
		require.Empty(t, payload.Bytes())
	}
}

func TestMalformedProgressiveCFilterPayloads(t *testing.T) {
	hash := make([]byte, chainhash.HashSize)
	stageHeader := func(rest ...byte) []byte {
		return append(append(bytes.Clone(hash), 0), rest...)
	}
	tests := []struct {
		name    string
		msg     wire.Message
		payload []byte
	}{
		{"single truncated", &wire.MsgGetCFStage{}, hash},
		{"single invalid stage", &wire.MsgGetCFStage{}, append(bytes.Clone(hash), 4)},
		{"group invalid stage", &wire.MsgGetCFStages{}, append([]byte{4, 1}, hash...)},
		{"group zero count", &wire.MsgGetCFStages{}, []byte{0, 0}},
		{"group large count", &wire.MsgGetCFStages{}, []byte{0, 17}},
		{"group noncanonical count", &wire.MsgGetCFStages{},
			append([]byte{0, 0xfd, 1, 0}, hash...)},
		{"group duplicate", &wire.MsgGetCFStages{}, append([]byte{0, 2}, make([]byte, 64)...)},
		{"group truncated", &wire.MsgGetCFStages{}, append([]byte{0, 2}, hash...)},
		{"response invalid stage", &wire.MsgCFStage{}, append(bytes.Clone(hash), 4, 0, 0)},
		{"response large count", &wire.MsgCFStage{}, stageHeader(0xfe, 0xa1, 0x86, 1, 0, 0)},
		{"response noncanonical count", &wire.MsgCFStage{}, stageHeader(0xfd, 1, 0, 0)},
		{"response large data", &wire.MsgCFStage{}, stageHeader(1, 0xfe, 1, 0, 4, 0)},
		{"response noncanonical data length", &wire.MsgCFStage{}, stageHeader(1, 0xfd, 1, 0, 1)},
		{"response truncated", &wire.MsgCFStage{}, stageHeader(1, 2, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.msg.BtcDecode(bytes.NewReader(test.payload), wire.ProtocolVersion, wire.BaseEncoding)
			require.Error(t, err)
		})
	}
}
