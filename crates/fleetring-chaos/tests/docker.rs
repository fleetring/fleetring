//! A whole fault-injection run on real containers (FR-065). Needs Linux and Docker, so it is
//! ignored by default: `cargo test -p fleetring-chaos --features harness -- --ignored`.

#![cfg(feature = "harness")]

use std::time::Duration;

use fleetring_chaos::node::DEFAULT_IMAGE;
use fleetring_chaos::run::{RunOptions, run};
use fleetring_chaos::schedule::Scenario;
use fleetring_chaos::{Build, Mode, Teardown};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Linux and Docker; run with --ignored"]
async fn a_two_minute_mixed_run_is_valid() {
    let out = tempfile::tempdir().expect("temporary directory");
    let options = RunOptions {
        scenario: Scenario::Mixed,
        seed: 1,
        time_limit: Duration::from_secs(120),
        mode: Mode::Normal,
        image: DEFAULT_IMAGE.to_owned(),
        build: Build::Image,
        out: out.path().to_owned(),
        teardown: Teardown::Remove,
    };
    assert_eq!(run(options).await, 0);
}
