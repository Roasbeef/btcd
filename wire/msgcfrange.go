// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chainhash/v2"
)

// MaxCFRangeDataSize bounds an experimental range filter object, including its
// 81-byte header, up to 256 KiB of filter data, and 32-byte commitment.
const MaxCFRangeDataSize = 81 + 256*1024 + 32

// MsgGetCFRange requests a precomputed experimental filter for the inclusive
// range identified by its first and last block hashes.
type MsgGetCFRange struct {
	FirstHash chainhash.Hash
	LastHash  chainhash.Hash
}

// NewMsgGetCFRange returns a request for the given range endpoints.
func NewMsgGetCFRange(first, last *chainhash.Hash) *MsgGetCFRange {
	return &MsgGetCFRange{FirstHash: *first, LastHash: *last}
}

// BtcDecode decodes an experimental range filter request.
func (msg *MsgGetCFRange) BtcDecode(r io.Reader, _ uint32, _ MessageEncoding) error {
	return readElements(r, &msg.FirstHash, &msg.LastHash)
}

// BtcEncode encodes an experimental range filter request.
func (msg *MsgGetCFRange) BtcEncode(w io.Writer, _ uint32, _ MessageEncoding) error {
	return writeElements(w, &msg.FirstHash, &msg.LastHash)
}

// Command returns the experimental range request command.
func (*MsgGetCFRange) Command() string { return CmdGetCFRange }

// MaxPayloadLength returns the maximum encoded request size.
func (*MsgGetCFRange) MaxPayloadLength(uint32) uint32 { return 2 * chainhash.HashSize }

// MsgCFRange carries a serialized experimental range filter object.
type MsgCFRange struct {
	Data []byte
}

// BtcDecode decodes an experimental range filter response and bounds its size.
func (msg *MsgCFRange) BtcDecode(r io.Reader, pver uint32, _ MessageEncoding) error {
	var err error
	msg.Data, err = ReadVarBytes(r, pver, MaxCFRangeDataSize, "cfrange data")
	return err
}

// BtcEncode encodes an experimental range filter response.
func (msg *MsgCFRange) BtcEncode(w io.Writer, pver uint32, _ MessageEncoding) error {
	if len(msg.Data) > MaxCFRangeDataSize {
		return messageError("MsgCFRange.BtcEncode",
			fmt.Sprintf("too many data bytes: %d", len(msg.Data)))
	}
	return WriteVarBytes(w, pver, msg.Data)
}

// Command returns the experimental range response command.
func (*MsgCFRange) Command() string { return CmdCFRange }

// MaxPayloadLength returns the maximum encoded response size.
func (*MsgCFRange) MaxPayloadLength(uint32) uint32 {
	return uint32(VarIntSerializeSize(MaxCFRangeDataSize)) + MaxCFRangeDataSize
}
