// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"bytes"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

// globalMemController is the global memory controller instance.
// It should be initialized by the application before use.
var globalMemController *MemoryController

// SetGlobalMemoryController sets the global memory controller.
// This should be called during application initialization.
func SetGlobalMemoryController(mc *MemoryController) {
	globalMemController = mc
}

// GetGlobalMemoryController returns the global memory controller.
// If not set, it creates a default one.
func GetGlobalMemoryController() *MemoryController {
	if globalMemController == nil {
		// Create default controller if not set
		mc, err := NewMemoryController(DefaultMemoryConfig())
		if err != nil {
			// This should not happen with default config
			panic(fmt.Sprintf("failed to create default memory controller: %v", err))
		}
		globalMemController = mc
	}
	return globalMemController
}

// ReadMessageWithEncodingNPool reads, validates, and parses the next bitcoin
// Message from r using buffer pools for the provided protocol version and
// bitcoin network. It returns the number of bytes read in addition to the
// parsed Message and a Buffer object which comprises the message.
// The caller is responsible for calling Release() on the returned Buffer.
func ReadMessageWithEncodingNPool(r io.Reader, pver uint32, btcnet BitcoinNet,
	enc MessageEncoding) (int, Message, *Buffer, error) {

	totalBytes := 0
	n, hdr, err := readMessageHeader(r)
	totalBytes += n
	if err != nil {
		return totalBytes, nil, nil, err
	}

	// Enforce maximum message payload.
	if hdr.length > MaxMessagePayload {
		str := fmt.Sprintf("message payload is too large - header "+
			"indicates %d bytes, but max message payload is %d "+
			"bytes.", hdr.length, MaxMessagePayload)
		return totalBytes, nil, nil, messageError("ReadMessage", str)
	}

	// Check for messages from the wrong bitcoin network.
	if hdr.magic != btcnet {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("message from other network [%v]", hdr.magic)
		return totalBytes, nil, nil, messageError("ReadMessage", str)
	}

	// Check for malformed commands.
	command := hdr.command
	if !utf8.ValidString(command) {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("invalid command %v", []byte(command))
		return totalBytes, nil, nil, messageError("ReadMessage", str)
	}

	// Create a contiguous slice of the message for the convenience of the
	// caller since there is no way to recover a message based on the bytes
	// of a message.
	msg, err := makeEmptyMessage(command)
	if err != nil {
		discardInput(r, hdr.length)
		return totalBytes, nil, nil, messageError("ReadMessage",
			err.Error())
	}

	// Check for maximum length based on the message type as a malicious client
	// could otherwise create a message which intentionally consumes a lot of
	// memory.
	mpl := msg.MaxPayloadLength(pver)
	if hdr.length > mpl {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("payload exceeds max length - header "+
			"indicates %v bytes, but max payload size for "+
			"messages of type [%v] is %v.", hdr.length, command, mpl)
		return totalBytes, nil, nil, messageError("ReadMessage", str)
	}

	// Allocate buffer from pool
	mc := GetGlobalMemoryController()
	buf, err := mc.AllocateBuffer(int(hdr.length))
	if err != nil {
		return totalBytes, nil, nil, fmt.Errorf("failed to allocate message buffer: %w", err)
	}

	// Read payload into pooled buffer
	payload := buf.Bytes()
	n, err = io.ReadFull(r, payload)
	totalBytes += n
	if err != nil {
		buf.Release()
		return totalBytes, nil, nil, err
	}

	// Test checksum.
	checksum := chainhash.DoubleHashB(payload)[0:4]
	if !bytes.Equal(checksum, hdr.checksum[:]) {
		buf.Release()
		str := fmt.Sprintf("payload checksum failed - header "+
			"indicates %v, but actual checksum is %v.",
			hdr.checksum, checksum)
		return totalBytes, nil, nil, messageError("ReadMessage", str)
	}

	// Unmarshal message.  NOTE: This must be a *bytes.Buffer since the
	// MsgVersion BtcDecode function requires it.
	pr := bytes.NewBuffer(payload)
	err = msg.BtcDecode(pr, pver, enc)
	if err != nil {
		buf.Release()
		return totalBytes, nil, nil, err
	}

	// Transfer buffer ownership to message if it implements BufferHolder
	if holder, ok := msg.(BufferHolder); ok {
		holder.SetBuffer(buf)
	}

	return totalBytes, msg, buf, nil
}

// ReadMessageNPool reads, validates, and parses the next bitcoin Message from r
// using buffer pools for the provided protocol version and bitcoin network.
// It returns the number of bytes read in addition to the parsed Message and
// Buffer object which comprise the message.
func ReadMessageNPool(r io.Reader, pver uint32, btcnet BitcoinNet) (int, Message, *Buffer, error) {
	return ReadMessageWithEncodingNPool(r, pver, btcnet, BaseEncoding)
}

// ReadMessagePool reads, validates, and parses the next bitcoin Message from r
// using buffer pools for the provided protocol version and bitcoin network.
// It returns the parsed Message and Buffer object which comprise the message.
func ReadMessagePool(r io.Reader, pver uint32, btcnet BitcoinNet) (Message, *Buffer, error) {
	_, msg, buf, err := ReadMessageNPool(r, pver, btcnet)
	return msg, buf, err
}

// ReadMessagePoolForPeer reads a message using peer-specific rate limiting.
func ReadMessagePoolForPeer(r io.Reader, pver uint32, btcnet BitcoinNet, peerID string) (Message, *Buffer, error) {
	totalBytes := 0
	n, hdr, err := readMessageHeader(r)
	totalBytes += n
	if err != nil {
		return nil, nil, err
	}

	// Enforce maximum message payload.
	if hdr.length > MaxMessagePayload {
		str := fmt.Sprintf("message payload is too large - header "+
			"indicates %d bytes, but max message payload is %d "+
			"bytes.", hdr.length, MaxMessagePayload)
		return nil, nil, messageError("ReadMessage", str)
	}

	// Check for messages from the wrong bitcoin network.
	if hdr.magic != btcnet {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("message from other network [%v]", hdr.magic)
		return nil, nil, messageError("ReadMessage", str)
	}

	// Check for malformed commands.
	command := hdr.command
	if !utf8.ValidString(command) {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("invalid command %v", []byte(command))
		return nil, nil, messageError("ReadMessage", str)
	}

	// Create a contiguous slice of the message for the convenience of the
	// caller since there is no way to recover a message based on the bytes
	// of a message.
	msg, err := makeEmptyMessage(command)
	if err != nil {
		discardInput(r, hdr.length)
		return nil, nil, messageError("ReadMessage", err.Error())
	}

	// Check for maximum length based on the message type as a malicious client
	// could otherwise create a message which intentionally consumes a lot of
	// memory.
	mpl := msg.MaxPayloadLength(pver)
	if hdr.length > mpl {
		discardInput(r, hdr.length)
		str := fmt.Sprintf("payload exceeds max length - header "+
			"indicates %v bytes, but max payload size for "+
			"messages of type [%v] is %v.", hdr.length, command, mpl)
		return nil, nil, messageError("ReadMessage", str)
	}

	// Allocate buffer with peer rate limiting
	mc := GetGlobalMemoryController()
	limiter := mc.GetPeerLimiter(peerID)
	if !limiter.Allow() {
		return nil, nil, fmt.Errorf("peer %s exceeded rate limit", peerID)
	}

	buf, err := mc.AllocateBuffer(int(hdr.length))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate message buffer: %w", err)
	}

	// Read payload into pooled buffer
	payload := buf.Bytes()
	n, err = io.ReadFull(r, payload)
	if err != nil {
		buf.Release()
		return nil, nil, err
	}

	// Test checksum.
	checksum := chainhash.DoubleHashB(payload)[0:4]
	if !bytes.Equal(checksum, hdr.checksum[:]) {
		buf.Release()
		str := fmt.Sprintf("payload checksum failed - header "+
			"indicates %v, but actual checksum is %v.",
			hdr.checksum, checksum)
		return nil, nil, messageError("ReadMessage", str)
	}

	// Unmarshal message.  NOTE: This must be a *bytes.Buffer since the
	// MsgVersion BtcDecode function requires it.
	pr := bytes.NewBuffer(payload)
	err = msg.BtcDecode(pr, pver, BaseEncoding)
	if err != nil {
		buf.Release()
		return nil, nil, err
	}

	// Transfer buffer ownership to message if it implements BufferHolder
	if holder, ok := msg.(BufferHolder); ok {
		holder.SetBuffer(buf)
	}

	return msg, buf, nil
}