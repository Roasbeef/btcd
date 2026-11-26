// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package progressivecf

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

func TestDecodeRangeDescriptor(t *testing.T) {
	// This fixture is also used by Fermion's rangefilter package.
	const fixture = "010b1109072a0000002b000000" +
		"1100000000000000000000000000000000000000000000000000000000000000" +
		"2200000000000000000000000000000000000000000000000000000000000000" +
		"0c0000000452fcad4e0309a5ec1a12c0" +
		"88524efb2eb37730ac888925e82fccaecf35de8276b55f2b24d43bb4bae82a56"
	object, err := hex.DecodeString(fixture)
	require.NoError(t, err)
	const net = wire.BitcoinNet(0x0709110b)
	descriptor, err := DecodeRangeDescriptor(object, net)
	require.NoError(t, err)
	require.Equal(t, RangeDescriptor{
		FirstHeight: 42,
		LastHeight:  43,
		FirstHash:   chainhash.Hash{0x11},
		LastHash:    chainhash.Hash{0x22},
	}, descriptor)

	_, err = DecodeRangeDescriptor(object, wire.SimNet)
	require.Error(t, err)

	tests := []struct {
		name   string
		offset int
		value  byte
	}{
		{"version", 0, 2},
		{"span", 9, 58},
		{"reversed heights", 9, 41},
		{"wrong filter size", 77, 11},
		{"wrong commitment", len(object) - 1, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bad := bytes.Clone(object)
			bad[test.offset] = test.value
			_, err := DecodeRangeDescriptor(bad, net)
			require.Error(t, err)
		})
	}

	// A valid object commitment cannot make a noncanonical GCS acceptable.
	body := bytes.Clone(object[:len(object)-sha256.Size])
	body[rangeHeaderSize] = 0
	hash := sha256.Sum256(append([]byte(rangeCommitmentDomain), body...))
	bad := append(body, hash[:]...)
	_, err = DecodeRangeDescriptor(bad, net)
	require.Error(t, err)
}
