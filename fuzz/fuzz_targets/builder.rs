//! Arbitrary memberships either build or fail with an error, never a panic, and every ring that
//! builds keeps the replica-selection invariants for arbitrary keys (ADR-0006).

#![no_main]

mod common;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Arbitrary)]
struct Input {
    ring: common::RingInput,
    keys: Vec<Vec<u8>>,
}

fuzz_target!(|input: Input| {
    let Some(ring) = input.ring.build() else {
        return;
    };
    for key in input.keys.iter().take(16) {
        common::check_key(&ring, key);
    }
});
