//! Declares the `tokio_unstable` cfg, which simulation runs set to replay seeds exactly
//! (ADR-0020), so the compiler accepts `cfg!(tokio_unstable)`.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
    println!("cargo::rerun-if-changed=build.rs");
}
