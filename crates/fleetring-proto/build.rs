//! Compiles the `.proto` files with protox, a pure-Rust protobuf compiler, so builds don't need a
//! system `protoc` (ADR-0019).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/fleetring/v1/fleetring.proto";
    println!("cargo:rerun-if-changed={proto}");
    let descriptors = protox::compile([proto], ["proto"])?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
