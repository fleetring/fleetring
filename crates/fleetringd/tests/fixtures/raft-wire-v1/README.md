# Raft wire and storage vectors, version 1

Golden JSON for FleetRing's encodings of openraft's values (ADR-0040 point 3): one file per
message, entry kind, vote, log ID, membership (one config and joint), and snapshot metadata.
`control/raft/codec.rs`'s `every_golden_vector_round_trips` decodes each file into openraft's
type, encodes it back, and requires the same bytes.

The files are append-only, like placement's golden vectors (ADR-0008): an existing file never
changes. A new vector gets a new file, written by the ignored test `write_new_golden_vectors`,
which never rewrites a file that exists.
