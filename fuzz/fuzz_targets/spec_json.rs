//! Untrusted ring and cells specs (from the network or the control db) never panic `from_spec`,
//! and every spec that loads round-trips to the same fingerprint (ADR-0008, ADR-0012). The same
//! bytes are tried as a ring spec and as a cells spec, whose layer loads as one layer of rings
//! or two.

#![no_main]

use fleetring::{
    CellId, Cells, CellsSpec, FromCellsSpecError, FromSpecError, Placement, PlacementSpec, Ring,
    RingSpec, Xxh3,
};
use libfuzzer_sys::fuzz_target;

/// How many stored fingerprints a cells spec may have repaired before the target gives up on it.
const REPAIRS: usize = 16;

fuzz_target!(|data: &[u8]| {
    if let Ok(spec) = serde_json::from_slice::<RingSpec>(data) {
        check_ring(&spec);
    }
    if let Ok(spec) = serde_json::from_slice::<CellsSpec>(data) {
        // Whether it loads or not, a spec that parses writes JSON that parses back to it, so no
        // directory rule changes on the way.
        let json = serde_json::to_vec(&spec).expect("a parsed spec serializes");
        let again: CellsSpec = serde_json::from_slice(&json).expect("a written spec parses");
        assert_eq!(again, spec, "JSON round trip keeps the spec");
        check_cells::<Ring>(spec.clone());
        check_cells::<Cells<Ring>>(spec);
    }
});

fn check_ring(spec: &RingSpec) {
    let Ok(ring) = Ring::from_spec(spec, Xxh3) else {
        return;
    };
    let again = Ring::from_spec(&ring.to_spec(), Xxh3).expect("a loaded ring's own spec loads");
    assert_eq!(again.fingerprint(), ring.fingerprint(), "round trip keeps the fingerprint");
    assert_eq!(again.fingerprint(), spec.fingerprint, "the stored fingerprint was verified");
}

/// Loads `spec` as a `Cells<P>` and checks that the layer round-trips through its own spec and
/// that spec's JSON.
///
/// Almost every mutation of a valid spec breaks a stored fingerprint, so a spec that fails only
/// on fingerprints has them set to the computed ones, as `to_spec` would write them, and is
/// tried again. That lets mutated layers reach the round-trip checks.
fn check_cells<P: Placement<Hasher = Xxh3>>(mut spec: CellsSpec) {
    let mut loaded = Cells::<P>::from_spec(&spec, Xxh3);
    for _ in 0..REPAIRS {
        let Err(error) = &loaded else {
            break;
        };
        if repair(&mut spec, error).is_none() {
            return;
        }
        loaded = Cells::<P>::from_spec(&spec, Xxh3);
    }
    let Ok(layer) = loaded else {
        return;
    };
    let own = layer.to_spec();
    let again = Cells::<P>::from_spec(&own, Xxh3).expect("a loaded layer's own spec loads");
    assert_eq!(again.fingerprint(), layer.fingerprint(), "round trip keeps the fingerprint");
    assert_eq!(again.fingerprint(), spec.fingerprint, "the stored fingerprint was verified");
    assert_eq!(again.to_spec(), own, "a layer rebuilt from its spec writes the same spec");
    let json = serde_json::to_vec(&own).expect("a spec serializes");
    let parsed: CellsSpec = serde_json::from_slice(&json).expect("a written spec parses");
    assert_eq!(parsed, own, "JSON round trip keeps the layer's spec");
}

/// Sets the stored fingerprint that `error` reports as wrong to the computed one. Returns `None`
/// for every other error, and when the error's path doesn't lead to a spec.
fn repair(spec: &mut CellsSpec, error: &FromCellsSpecError) -> Option<()> {
    match error {
        FromCellsSpecError::FingerprintMismatch { path, computed, .. } => {
            layer_at(spec, path)?.fingerprint = *computed;
        }
        FromCellsSpecError::InvalidSelector {
            path,
            source: FromSpecError::FingerprintMismatch { computed, .. },
        } => layer_at(spec, path)?.ring_selector.as_mut()?.fingerprint = *computed,
        FromCellsSpecError::InvalidRing {
            path,
            source: FromSpecError::FingerprintMismatch { computed, .. },
        } => ring_at(spec, path)?.fingerprint = *computed,
        _ => return None,
    }
    Some(())
}

/// Returns the spec of the layer that the cell at `path` holds: the first cell with each ID,
/// which `from_spec` also checks first.
fn layer_at<'a>(spec: &'a mut CellsSpec, path: &[CellId]) -> Option<&'a mut CellsSpec> {
    let Some((id, rest)) = path.split_first() else {
        return Some(spec);
    };
    match &mut spec.cells.iter_mut().find(|cell| cell.id == *id)?.placement {
        PlacementSpec::Cells(layer) => layer_at(layer, rest),
        _ => None,
    }
}

/// Returns the spec of the ring that the cell at `path` holds.
fn ring_at<'a>(spec: &'a mut CellsSpec, path: &[CellId]) -> Option<&'a mut RingSpec> {
    let (id, parent) = path.split_last()?;
    let layer = layer_at(spec, parent)?;
    match &mut layer.cells.iter_mut().find(|cell| cell.id == *id)?.placement {
        PlacementSpec::Ring(ring) => Some(ring),
        _ => None,
    }
}
