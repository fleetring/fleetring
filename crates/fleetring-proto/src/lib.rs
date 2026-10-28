//! gRPC services and messages between FleetRing nodes (ADR-0019).
//!
//! Package `fleetring.v1` defines the `Control`, `Replication`, and `Database` services. Log
//! positions and batch payloads are opaque bytes that only engine adapters interpret (ADR-0016).
