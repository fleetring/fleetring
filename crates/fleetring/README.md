# fleetring

Consistent-hashing placement for fleets of SQLite, libSQL, and Turso databases. You get one database per agent, tenant, or device, spread across nodes you own.

> **Status:** early development (`0.0.x`). APIs and placement output will change until 0.1.0 freezes placement spec v1.

`fleetring` is the placement core of [FleetRing](https://fleetring.dev). You give it a ring (nodes with weights, zones, and hosts) and a database name. It returns an ordered replica set whose first node is the preferred primary. The library is pure and deterministic: the same inputs produce the same placement on every platform, Rust release, and language port.

The design decisions are recorded in the [architecture decision records](https://github.com/fleetring/fleetring/tree/main/docs/adr).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
