---
id: FR-061
title: A `fleetring` Ruby gem and an ActiveRecord adapter that place each tenant's database
status: todo
priority: P2
size: L
milestone: M8 Access and integration
depends_on: [FR-047]
adrs: [ADR-0025]
---

# FR-061: A `fleetring` Ruby gem and an ActiveRecord adapter that place each tenant's database

## Context

Rails 8 made SQLite a production database, and Basecamp's [`activerecord-tenanted`](https://github.com/basecamp/activerecord-tenanted) gives each tenant its own SQLite file on one machine. That is FleetRing's shape: one database per tenant, many of them. What the gem can't do is spread those databases over several machines, replicate them, or fail them over. FleetRing can, and a Rails app shouldn't have to know how.

**This ticket depends on the router, which has no ticket yet.** The router (MVP step 4, roadmap E6) is the libSQL/Hrana front door that lets existing libSQL clients reach a database's primary without knowing placement. The adapter talks to the router, not to `fleetringd`'s gRPC `Database` service, which is v0.1-only and goes away when the router lands. Until the router's ticket exists and is done, this ticket stays `todo`; add the router's ID to `depends_on` when it is written. FR-047 (principals, scopes, and tokens) is listed because the gem authenticates as a principal with a token scoped to a tenant prefix.

The RubyGems name `fleetring` is reserved for this project.

## Goal

A Rails app adds `gem "fleetring"`, points `config/database.yml` at a fleet's router, and gets one SQLite database per tenant, placed, replicated, and failed over by FleetRing, with the same tenant-switching API `activerecord-tenanted` offers.

## Scope

- **The `fleetring` gem:** a small Ruby client for the router: connection settings (router URL, token or token command, fleet name), database naming (`tenant:<name>/<db>`, the lexicon's tenant convention), and errors mapped to Ruby exceptions (refused scope, unknown database, unavailable). It creates a tenant's database through the router's admin call when the app asks for one.
- **An ActiveRecord adapter** (`adapter: fleetring` in `database.yml`):
  - Speaks libSQL/Hrana to the router through an existing Ruby libSQL client if one is maintained, and otherwise through a minimal Hrana-over-HTTP implementation in the gem.
  - Maps each tenant to one FleetRing database by name; the router finds its primary.
  - Works with `activerecord-tenanted`'s tenant switching (`ApplicationRecord.with_tenant(...)`), either by plugging into it as a connection backend or by providing the same API; decide which after reading its extension points, and record the choice here.
  - Migrations run per tenant database, as `activerecord-tenanted` runs them.
- **Credentials:** one bearer token per app, scoped to `write:tenant:*` (or narrower), from an environment variable or Rails credentials; never a private key in the app.
- **A sample app** under `examples/rails/` with two tenants, run in CI against a three-node fleet started by the test harness.
- **Not in scope:** the router itself; replica reads; a Rails-side cache of placement (the router has it).

## Acceptance criteria

- [ ] `gem install fleetring` installs the gem from RubyGems, published by the release workflow from a `fleetring-rb-v*` tag with a trusted-publishing token.
- [ ] A Rails 8 app with `adapter: fleetring` creates a database per tenant on a three-node fleet, writes and reads through ActiveRecord, and keeps working when the primary of a tenant's database is stopped (writes resume after failover, with ActiveRecord's retry).
- [ ] `activerecord-tenanted`'s tenant switching works unchanged with the adapter, or the gem's equivalent API is documented with a migration note.
- [ ] A token scoped to `write:tenant:acme/*` can't read another tenant's database, and the adapter raises a clear error naming the database.
- [ ] Migrations run on every tenant's database with one command.
- [ ] The README has a Rails section, and the sample app runs in CI.

## Progress

- Not started. Blocked on the router's ticket.
