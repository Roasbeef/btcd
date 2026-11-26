# Experimental progressive compact filters

Build this branch with `go build .` and start btcd with
`--regtest --experimentalprogressivecfilters` or
`--simnet --experimentalprogressivecfilters`. The flag requires the default
committed filter index. Configure the client with the peer's address and
enable the experiment explicitly.

The server reads serialized BIP158 basic filters from the existing committed
filter index. Each filter uses its own block hash as key material and its own
element count for range reduction. A grouped request saves request frames and
receives independent per-block responses in the same order as the request.

## Wire format

| Command | Payload |
| --- | --- |
| `getcfstage` | 32-byte block hash, 1-byte stage |
| `getcfstages` | 1-byte stage, CompactSize hash count, that many 32-byte block hashes |
| `cfstage` | 32-byte block hash, 1-byte stage, CompactSize element count N, CompactSize data length, stage data |

Block hashes use internal byte order, as in `getcfilters`. Stages range from
0 through 3. A group contains 1 to 16 distinct hashes. Every response reports
the complete filter's element count N, capped at 100,000. The stage data length
is capped at 256 KiB. Counts and lengths use canonical CompactSize encoding.

These experimental commands use ordinary Bitcoin v1 message framing or the
BIP324 long command form. The experiment has no service bit or protocol version
allocation. Standard `getcfilters`, `getcfheaders`, and `getcfcheckpt` remain
available.

The server prepares a group's stages from a single index snapshot before
sending them. If any requested filter is unavailable or outside the experimental
limits, the server ignores the request. The server accepts up to four stage
requests in flight across all peers and disconnects after ten seconds if a
response cannot be written. Clients should set their own request timeout.

## Stage encoding

Let v be a sorted, range-reduced BIP158 basic filter value, with
`M = 784931` and `P = 19`. Retain repetitions in the sorted sequence.

| Stage | Encoding |
| --- | --- |
| 0 | Delta-code `v >> 12` from zero with Golomb-Rice `P = 7`. |
| 1 | Pack bits 11 through 8 of each v in sorted order. |
| 2 | Pack bits 7 through 4 of each v in sorted order. |
| 3 | Pack bits 3 through 0 of each v in sorted order. |

In stage 0, a quotient q is q one bits followed by a zero and the seven
remainder bits. Both the Golomb-Rice stream and the packed nibbles use
most-significant-bit-first bit order. The final byte is padded with zero bits.
An empty filter has empty stage data at all four stages.

Stage payloads carry no independent commitments. The four stages reconstruct
the same value sequence as the indexed BIP158 filter, which a client can
validate against the BIP157 filter hash. A no-match decision based on an
unverified prefix relies on the explicitly configured peer.
