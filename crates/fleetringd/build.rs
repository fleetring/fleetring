//! Records the target triple the daemon is built for, which `fleetringd upgrade` downloads, and
//! declares the `tokio_unstable` cfg, which simulation runs set to replay seeds exactly
//! (ADR-0020), so the compiler accepts `cfg!(tokio_unstable)`.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
    println!("cargo::rerun-if-changed=build.rs");
    // `fleetringd upgrade` picks the release archive built for the same target.
    let target = std::env::var("TARGET").unwrap_or_default();
    println!("cargo::rustc-env=FLEETRINGD_TARGET={target}");
}
