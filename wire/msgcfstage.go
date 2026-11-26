// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chainhash/v2"
)

const (
	// MaxCFStage is the highest experimental progressive filter stage.
	MaxCFStage = 3

	// MaxCFStageElements bounds the number of elements in one block's stage.
	MaxCFStageElements = 100_000

	// MaxCFStageDataSize is the maximum length of one stage's data.
	MaxCFStageDataSize = 256 * 1024

	// MaxGetCFStagesHashes is the maximum number of hashes in a grouped
	// experimental progressive filter request.
	MaxGetCFStagesHashes = 16
)

// MsgGetCFStage requests one experimental progressive filter stage for a block.
type MsgGetCFStage struct {
	BlockHash chainhash.Hash
	Stage     uint8
}

// NewMsgGetCFStage returns a request for a single block and stage.
func NewMsgGetCFStage(hash *chainhash.Hash, stage uint8) *MsgGetCFStage {
	return &MsgGetCFStage{BlockHash: *hash, Stage: stage}
}

// BtcDecode decodes a single-block request.
func (msg *MsgGetCFStage) BtcDecode(r io.Reader, _ uint32, _ MessageEncoding) error {
	if err := readElements(r, &msg.BlockHash, &msg.Stage); err != nil {
		return err
	}
	return checkCFStage("MsgGetCFStage.BtcDecode", msg.Stage)
}

// BtcEncode encodes a single-block request.
func (msg *MsgGetCFStage) BtcEncode(w io.Writer, _ uint32, _ MessageEncoding) error {
	if err := checkCFStage("MsgGetCFStage.BtcEncode", msg.Stage); err != nil {
		return err
	}
	return writeElements(w, &msg.BlockHash, msg.Stage)
}

// Command returns the experimental command for a single-block request.
func (*MsgGetCFStage) Command() string { return CmdGetCFStage }

// MaxPayloadLength returns the maximum encoded request size.
func (*MsgGetCFStage) MaxPayloadLength(uint32) uint32 { return chainhash.HashSize + 1 }

// MsgGetCFStages requests one stage for each block in an ordered group. An
// experimental server replies with individual MsgCFStage messages in order.
type MsgGetCFStages struct {
	Stage       uint8
	BlockHashes []chainhash.Hash
}

// NewMsgGetCFStages returns a request for the same stage of multiple blocks.
func NewMsgGetCFStages(hashes []chainhash.Hash, stage uint8) *MsgGetCFStages {
	return &MsgGetCFStages{
		Stage:       stage,
		BlockHashes: append([]chainhash.Hash(nil), hashes...),
	}
}

// BtcDecode decodes a grouped request with a bounded number of unique hashes.
func (msg *MsgGetCFStages) BtcDecode(r io.Reader, pver uint32, _ MessageEncoding) error {
	const fn = "MsgGetCFStages.BtcDecode"
	if err := readElement(r, &msg.Stage); err != nil {
		return err
	}
	if err := checkCFStage(fn, msg.Stage); err != nil {
		return err
	}
	count, err := ReadVarInt(r, pver)
	if err != nil {
		return err
	}
	if err := checkCFStageGroupSize(fn, count); err != nil {
		return err
	}
	msg.BlockHashes = make([]chainhash.Hash, count)
	for i := range msg.BlockHashes {
		if err := readElement(r, &msg.BlockHashes[i]); err != nil {
			return err
		}
	}
	return checkCFStageGroupHashes(fn, msg.BlockHashes)
}

// BtcEncode encodes a grouped request with a bounded number of unique hashes.
func (msg *MsgGetCFStages) BtcEncode(w io.Writer, pver uint32, _ MessageEncoding) error {
	const fn = "MsgGetCFStages.BtcEncode"
	if err := checkCFStage(fn, msg.Stage); err != nil {
		return err
	}
	count := uint64(len(msg.BlockHashes))
	if err := checkCFStageGroupSize(fn, count); err != nil {
		return err
	}
	if err := checkCFStageGroupHashes(fn, msg.BlockHashes); err != nil {
		return err
	}
	if err := writeElement(w, msg.Stage); err != nil {
		return err
	}
	if err := WriteVarInt(w, pver, count); err != nil {
		return err
	}
	for i := range msg.BlockHashes {
		if err := writeElement(w, &msg.BlockHashes[i]); err != nil {
			return err
		}
	}
	return nil
}

// Command returns the experimental command for a grouped request.
func (*MsgGetCFStages) Command() string { return CmdGetCFStages }

// MaxPayloadLength returns the maximum encoded grouped request size.
func (*MsgGetCFStages) MaxPayloadLength(uint32) uint32 {
	return 1 + uint32(VarIntSerializeSize(MaxGetCFStagesHashes)) +
		chainhash.HashSize*MaxGetCFStagesHashes
}

// MsgCFStage carries one experimental progressive filter stage. The element
// count describes the complete block filter at every stage. Data contains the
// encoded stage, whose interpretation belongs to the progressive filter codec.
type MsgCFStage struct {
	BlockHash chainhash.Hash
	Stage     uint8
	N         uint32
	Data      []byte
}

// BtcDecode decodes a stage response and enforces the envelope bounds.
func (msg *MsgCFStage) BtcDecode(r io.Reader, pver uint32, _ MessageEncoding) error {
	const fn = "MsgCFStage.BtcDecode"
	if err := readElements(r, &msg.BlockHash, &msg.Stage); err != nil {
		return err
	}
	if err := checkCFStage(fn, msg.Stage); err != nil {
		return err
	}
	count, err := ReadVarInt(r, pver)
	if err != nil {
		return err
	}
	if count > MaxCFStageElements {
		return messageError(fn, fmt.Sprintf("too many elements: %d", count))
	}
	msg.N = uint32(count)
	msg.Data, err = ReadVarBytes(r, pver, MaxCFStageDataSize, "cfstage data")
	return err
}

// BtcEncode encodes a stage response and enforces the envelope bounds.
func (msg *MsgCFStage) BtcEncode(w io.Writer, pver uint32, _ MessageEncoding) error {
	const fn = "MsgCFStage.BtcEncode"
	if err := checkCFStage(fn, msg.Stage); err != nil {
		return err
	}
	if msg.N > MaxCFStageElements {
		return messageError(fn, fmt.Sprintf("too many elements: %d", msg.N))
	}
	if len(msg.Data) > MaxCFStageDataSize {
		return messageError(fn, fmt.Sprintf("too many data bytes: %d", len(msg.Data)))
	}
	if err := writeElements(w, &msg.BlockHash, msg.Stage); err != nil {
		return err
	}
	if err := WriteVarInt(w, pver, uint64(msg.N)); err != nil {
		return err
	}
	return WriteVarBytes(w, pver, msg.Data)
}

// Command returns the experimental command for a stage response.
func (*MsgCFStage) Command() string { return CmdCFStage }

// MaxPayloadLength returns the maximum encoded response size.
func (*MsgCFStage) MaxPayloadLength(uint32) uint32 {
	return chainhash.HashSize + 1 + uint32(VarIntSerializeSize(MaxCFStageElements)+
		VarIntSerializeSize(MaxCFStageDataSize)) + MaxCFStageDataSize
}

func checkCFStage(fn string, stage uint8) error {
	if stage > MaxCFStage {
		return messageError(fn, fmt.Sprintf("invalid stage: %d", stage))
	}
	return nil
}

func checkCFStageGroupSize(fn string, count uint64) error {
	if count == 0 || count > MaxGetCFStagesHashes {
		return messageError(fn, fmt.Sprintf("invalid group size: %d", count))
	}
	return nil
}

func checkCFStageGroupHashes(fn string, hashes []chainhash.Hash) error {
	seen := make(map[chainhash.Hash]struct{}, len(hashes))
	for _, hash := range hashes {
		if _, ok := seen[hash]; ok {
			return messageError(fn, fmt.Sprintf("duplicate block hash: %s", hash))
		}
		seen[hash] = struct{}{}
	}
	return nil
}
