//! Untrusted ring specs (from the network or the control db) never panic `from_spec`, and every
//! spec that loads round-trips to the same fingerprint (ADR-0008).

#![no_main]

use fleetring::{Ring, RingSpec, Xxh3};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(spec) = serde_json::from_slice::<RingSpec>(data) else {
        return;
    };
    let Ok(ring) = Ring::from_spec(&spec, Xxh3) else {
        return;
    };
    let again = Ring::from_spec(&ring.to_spec(), Xxh3).expect("a loaded ring's own spec loads");
    assert_eq!(again.fingerprint(), ring.fingerprint(), "round trip keeps the fingerprint");
    assert_eq!(again.fingerprint(), spec.fingerprint, "the stored fingerprint was verified");
});
