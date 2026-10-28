//! Ships a ring from a control plane to a router as JSON, and checks that both place keys alike.
//!
//! A multiplayer game pins each match to one game server. The matchmaker owns membership: it
//! builds the ring and publishes its spec as JSON. Each router reads the spec, rebuilds the ring,
//! and sends every player of a match to the same server.
//!
//! The spec carries the ring's fingerprint, a digest of everything that affects placement.
//! `Ring::from_spec` recomputes it and rejects a spec that doesn't match, such as one edited after
//! it was written. Peers then compare `(epoch, fingerprint)` pairs: a lower epoch means a stale
//! ring, and the same epoch with a different fingerprint means a misconfigured peer.
//!
//! This example needs the `serde` feature:
//!
//! ```sh
//! cargo run -p fleetring --example spec_roundtrip --features serde
//! ```

use std::error::Error;

use fleetring::{Epoch, FromSpecError, Node, Ring, RingSpec, Xxh3};

fn main() -> Result<(), Box<dyn Error>> {
    // The matchmaker builds the ring for epoch 42 and publishes its spec.
    let ring = Ring::builder()
        .epoch(Epoch::new(42))
        .node(server("game-fra-1", "eu-central")?)
        .node(server("game-fra-2", "eu-central")?)
        .node(server("game-iad-1", "us-east")?)
        .node(server("game-sfo-1", "us-west")?)
        .build()?;
    let json = serde_json::to_string_pretty(&ring.to_spec())?;
    println!("published spec:\n{json}\n");

    // A router reads the spec. `from_spec` validates every field and checks the fingerprint.
    let spec: RingSpec = serde_json::from_str(&json)?;
    let router = Ring::from_spec(&spec, Xxh3)?;
    println!(
        "router loaded epoch {} with fingerprint {}",
        router.epoch(),
        router.fingerprint()
    );
    let in_sync = (router.epoch(), router.fingerprint()) == (ring.epoch(), ring.fingerprint());
    println!("router and matchmaker in sync: {in_sync}\n");

    for game in ["match:7f3a", "match:91bc", "match:c044"] {
        println!(
            "{game}: matchmaker {}, router {}",
            ring.primary(game).id(),
            router.primary(game).id(),
        );
    }

    // A spec edited after it was written no longer matches its fingerprint, so routers refuse
    // it rather than silently sending players elsewhere.
    let mut edited = spec;
    edited.seed = 7;
    match Ring::from_spec(&edited, Xxh3) {
        Err(error @ FromSpecError::FingerprintMismatch { .. }) => {
            println!("\nedited spec rejected: {error}");
        }
        Err(error) => return Err(error.into()),
        Ok(_) => return Err("the edited spec was accepted".into()),
    }
    Ok(())
}

/// Returns a game server in `zone`.
fn server(id: &str, zone: &str) -> Result<Node, Box<dyn Error>> {
    Ok(Node::new(id.parse()?).with_zone(zone.parse()?))
}
