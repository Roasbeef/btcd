// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

// This file demonstrates how to extend MsgBlock to implement BufferHolder
// for efficient memory management.

// BlockWithBuffer wraps MsgBlock with buffer lifecycle management.
type BlockWithBuffer struct {
	*MsgBlock
	buffer *Buffer
}

// NewBlockWithBuffer creates a new BlockWithBuffer.
func NewBlockWithBuffer(block *MsgBlock) *BlockWithBuffer {
	return &BlockWithBuffer{
		MsgBlock: block,
	}
}

// SetBuffer implements the BufferHolder interface.
func (b *BlockWithBuffer) SetBuffer(buf *Buffer) {
	if b.buffer != nil {
		b.buffer.Release()
	}
	b.buffer = buf
	if buf != nil {
		buf.AddRef()
	}
}

// ReleaseBuffer implements the BufferHolder interface.
func (b *BlockWithBuffer) ReleaseBuffer() {
	if b.buffer != nil {
		b.buffer.Release()
		b.buffer = nil
	}
}

// GetBuffer returns the underlying buffer.
func (b *BlockWithBuffer) GetBuffer() *Buffer {
	return b.buffer
}

// Clone creates a deep copy of the block with its own buffer.
func (b *BlockWithBuffer) Clone() (*BlockWithBuffer, error) {
	// Clone the MsgBlock
	newBlock := &MsgBlock{
		Header:       b.Header,
		Transactions: make([]*MsgTx, len(b.Transactions)),
	}
	for i, tx := range b.Transactions {
		newBlock.Transactions[i] = tx.Copy()
	}

	// Clone the buffer if present
	newBwb := &BlockWithBuffer{MsgBlock: newBlock}
	if b.buffer != nil {
		clonedBuf, err := b.buffer.Clone()
		if err != nil {
			return nil, err
		}
		newBwb.SetBuffer(clonedBuf)
		clonedBuf.Release() // SetBuffer added a ref, so release our ref
	}

	return newBwb, nil
}

// Example of how to use BlockWithBuffer in practice:
//
// func handleBlock(r io.Reader, pver uint32, btcnet BitcoinNet) error {
//     msg, buf, err := ReadMessagePool(r, pver, btcnet)
//     if err != nil {
//         return err
//     }
//     defer buf.Release()
//
//     switch msg := msg.(type) {
//     case *MsgBlock:
//         // Wrap in BlockWithBuffer for lifecycle management
//         block := NewBlockWithBuffer(msg)
//         block.SetBuffer(buf)
//         defer block.ReleaseBuffer()
//         
//         // Process block...
//         return processBlock(block)
//     default:
//         return fmt.Errorf("unexpected message type: %T", msg)
//     }
// }