# Security policy

## Supported versions

FleetRing is in early development. Security fixes go into the latest release of the `fleetring` crate.

## Authentication and authorization

Every `fleetringd` connection is mutual TLS with certificates from the cluster's authority, and every request is authorized against the caller's scopes before it touches any state (ADR-0025). What each RPC needs, and how principals get scopes from `[[principal]]` entries or bearer tokens, is in the daemon README's [scope table](crates/fleetringd/README.md#principals-and-scopes).

A node's certificate acts on that node only: it makes the node-to-node calls and leaves, drains, and hands over the leases of its own node, but it can't read, write, or administer a database, drain or remove another member, or revoke a token (ADR-0026). A stolen node key therefore affects that node and the databases placed on it. The node-to-node calls (replication, fencing, promotion, leases) are not yet bound to placement, so such a key can still make them for other databases within the epoch and quorum rules; FR-050 tracks closing that.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Please don't open a public issue.

Include what you found, how to reproduce it, and the affected versions. We aim to acknowledge reports within a week.

Input that reaches `fleetring` from outside, such as a ring spec decoded from JSON, must never cause a panic. A crash on such input counts as a security bug.
