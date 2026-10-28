# FleetRing

**Consistent-hashing placement and routing for fleets of SQLite, libSQL, and Turso databases.**

One database per agent, tenant, or device, spread across nodes you own. FleetRing decides where each database lives, routes traffic to it, and moves it when nodes join or leave.

> **Status:** early development (`0.0.x`). APIs will change. Not production-ready.

## Why

SQLite is now a server-side building block, and a database per agent or tenant is cheap. The hard part is **where each one lives, how clients find it, and how it moves** without losing writes. FleetRing handles that, vendor-neutral and self-hostable.

## Planned features

- Consistent hashing with bounded loads: weighted rendezvous (default) or a virtual-node ring
- Placement API: `key → database → node`, with zone-aware replicas
- Router that works with existing libSQL and Turso clients
- Live migration when nodes join or leave, with zero lost writes
- CLI: `fleetring up`, `node add/remove`, `db create/locate/move`, `status`

## Use cases

- **AI agent memory:** a database per agent, placed next to its compute
- **Multi-tenant SaaS:** a database per customer, for isolation and easy deletes
- **Edge and IoT:** local databases per site or device that sync to regional nodes

## Roadmap

- [ ] `fleetring` ring and placement crate
- [ ] Router sidecar (libSQL/Hrana protocol)
- [ ] Live migration
- [ ] Demo: 10k agents with live rebalancing
- [ ] Backups and point-in-time restore

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

---

[fleetring.dev](https://fleetring.dev) · [GitHub](https://github.com/fleetring/fleetring)
