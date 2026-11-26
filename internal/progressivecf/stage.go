// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package progressivecf produces the experimental progressive representation of
// the basic compact filter stored in btcd's committed filter index.
package progressivecf

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/btcutil/v2/gcs/builder"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	nibbleBits = 4
	suffixBits = wire.MaxCFStage * nibbleBits
	prefixP    = builder.DefaultP - suffixBits
)

// StageFromBasicFilter reads a serialized BIP158 basic filter and returns its
// element count and the encoded data for one stage. It preserves the sorted,
// per-block mapped values, including repetitions. Stage 0 re-encodes their
// high bits as a Golomb-Rice stream. Stages 1 through 3 carry successive low
// nibbles in the same order.
func StageFromBasicFilter(serialized []byte, stage uint8) (uint32, []byte, error) {
	if stage > wire.MaxCFStage {
		return 0, nil, fmt.Errorf("invalid stage: %d", stage)
	}
	source := bytes.NewBuffer(serialized)
	count, err := wire.ReadVarInt(source, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("read basic filter count: %w", err)
	}
	if count > wire.MaxCFStageElements {
		return 0, nil, fmt.Errorf("basic filter has too many elements: %d", count)
	}

	// Each value uses a terminator and the P remainder bits, plus a
	// quotient bounded by the range of all values across the whole filter.
	minBits := count * (1 + builder.DefaultP)
	modulus := count * builder.DefaultM
	maxBits := minBits
	if count != 0 {
		maxBits += (modulus - 1) >> builder.DefaultP
	}
	if size := uint64(source.Len()); size < (minBits+7)/8 ||
		size > (maxBits+7)/8 {

		return 0, nil, errors.New("invalid basic filter length")
	}

	reader := bitReader{data: source.Bytes()}
	var writer bitWriter
	var nibbles []byte
	if stage == 0 {
		maxPrefixBits := count*(1+prefixP) + maxBits - minBits
		writer.data = make([]byte, 0, (maxPrefixBits+7)/8)
	} else {
		nibbles = make([]byte, (count+1)/2)
	}

	var value, lastPrefix uint64
	for i := uint64(0); i < count; i++ {
		delta, err := reader.readRice(builder.DefaultP, modulus-1-value)
		if err != nil {
			return 0, nil, fmt.Errorf("read basic filter element %d: %w", i, err)
		}
		value += delta
		if stage == 0 {
			prefix := value >> suffixBits
			writer.writeRice(prefix-lastPrefix, prefixP)
			lastPrefix = prefix
		} else {
			shift := suffixBits - int(stage)*nibbleBits
			nibble := byte((value >> shift) & 0xf)
			if i%2 == 0 {
				nibble <<= nibbleBits
			}
			nibbles[i/2] |= nibble
		}
	}
	if !reader.zeroPadding() {
		return 0, nil, errors.New("non-canonical basic filter padding")
	}
	if stage == 0 {
		return uint32(count), writer.data, nil
	}
	return uint32(count), nibbles, nil
}

type bitReader struct {
	data []byte
	pos  int
}

func (r *bitReader) readBits(count int) (uint64, error) {
	if r.pos+count > len(r.data)*8 {
		return 0, io.ErrUnexpectedEOF
	}
	var value uint64
	for range count {
		bit := (r.data[r.pos/8] >> (7 - r.pos%8)) & 1
		value = value<<1 | uint64(bit)
		r.pos++
	}
	return value, nil
}

func (r *bitReader) readRice(p int, maxDelta uint64) (uint64, error) {
	var quotient uint64
	for {
		bit, err := r.readBits(1)
		if err != nil {
			return 0, err
		}
		if bit == 0 {
			break
		}
		quotient++
		if quotient > maxDelta>>p {
			return 0, errors.New("Golomb quotient exceeds basic filter range")
		}
	}
	remainder, err := r.readBits(p)
	if err != nil {
		return 0, err
	}
	delta := quotient<<p | remainder
	if delta > maxDelta {
		return 0, errors.New("Golomb delta exceeds basic filter range")
	}
	return delta, nil
}

func (r *bitReader) zeroPadding() bool {
	padding := len(r.data)*8 - r.pos
	if padding >= 8 {
		return false
	}
	bits, _ := r.readBits(padding)
	return bits == 0
}

type bitWriter struct {
	data []byte
	pos  int
}

func (w *bitWriter) writeBit(bit byte) {
	if w.pos%8 == 0 {
		w.data = append(w.data, 0)
	}
	w.data[w.pos/8] |= bit << (7 - w.pos%8)
	w.pos++
}

func (w *bitWriter) writeRice(delta uint64, p int) {
	for range delta >> p {
		w.writeBit(1)
	}
	w.writeBit(0)
	for shift := p - 1; shift >= 0; shift-- {
		w.writeBit(byte(delta>>shift) & 1)
	}
}
