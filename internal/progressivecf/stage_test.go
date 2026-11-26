// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package progressivecf

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStageFromBasicFilter(t *testing.T) {
	// A canonical BIP158 encoding of the following sorted mapped values,
	// including both a repeat and changes in the high prefix:
	//
	//   123, 123, 456, abc, 1234, 2fed, 82341, 101234, 24000f
	//
	// Stage 0 prefixes are 0, 0, 0, 0, 1, 2, 82, 101, 240 (hex).
	source, err := hex.DecodeString("09001230000000333006660077801db97f3547eef3cfb76c")
	require.NoError(t, err)
	expected := []string{
		"000000000101803fe7e0",
		"114a2f3200",
		"225b3e4300",
		"336c4d14f0",
	}
	for stage, want := range expected {
		n, data, err := StageFromBasicFilter(source, uint8(stage))
		require.NoError(t, err, "stage %d", stage)
		require.EqualValues(t, 9, n)
		require.Equal(t, want, hex.EncodeToString(data), "stage %d", stage)

		n, data, err = StageFromBasicFilter([]byte{0}, uint8(stage))
		require.NoError(t, err, "stage %d", stage)
		require.Zero(t, n)
		require.Empty(t, data)
	}
}

func TestStageFromInvalidBasicFilter(t *testing.T) {
	tests := map[string]string{
		"absent":                   "",
		"noncanonical count":       "fd0100000000",
		"count exceeds limit":      "fea1860100",
		"truncated value":          "010000",
		"extra zero byte":          "0100000000",
		"nonzero padding":          "01000001",
		"out of range quotient":    "01c00000",
		"out of range delta":       "019ffff0",
		"empty with trailing byte": "0000",
	}
	for name, serial := range tests {
		t.Run(name, func(t *testing.T) {
			source, err := hex.DecodeString(serial)
			require.NoError(t, err)
			_, _, err = StageFromBasicFilter(source, 0)
			require.Error(t, err)
		})
	}
	_, _, err := StageFromBasicFilter([]byte{0}, 4)
	require.Error(t, err)
}
