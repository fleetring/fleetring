//! What the harness writes for each node: its daemon configuration and its clock offset.

use crate::node::{Node, PORT, PRINCIPAL};

/// Returns the TOML configuration of `node`, as the container sees its files: data in `/data`,
/// certificates in `/tls`.
///
/// Every setting it doesn't name keeps the daemon's default (leases, failover margin, heartbeat
/// interval and timeout), so a run tests the defaults operators get. Members stay members across
/// restarts (`on_shutdown = "stay"`), so a kill tests failover rather than a leave.
///
/// The control node names its own address as `[control] address` too: without it, its data
/// role would dial its own listener on loopback, which its certificate, issued for its bridge
/// address only, doesn't name.
#[must_use]
pub fn render_config(node: Node) -> String {
    let name = node.name();
    let role = if node == Node::CONTROL {
        "both"
    } else {
        "data"
    };
    let control = Node::CONTROL.address();
    let advertise = node.address();
    format!(
        "data_dir = \"/data\"
listen = \"0.0.0.0:{PORT}\"
role = \"{role}\"

[node]
id = \"{name}\"
advertise = \"{advertise}\"

[control]
address = \"{control}\"

[membership]
on_shutdown = \"stay\"

[tls]
ca = \"/tls/ca.crt\"
cert = \"/tls/node.{name}.crt\"
key = \"/tls/node.{name}.key\"

[[principal]]
name = \"{PRINCIPAL}\"
scopes = [\"admin:{PRINCIPAL}/*\", \"fleet\"]
"
    )
}

/// Returns what the harness writes into a node's clock file for an offset of `seconds`, in
/// `libfaketime`'s relative form: `+0s`, `+42s`, `-42s`.
#[must_use]
pub fn faketime_offset(seconds: i64) -> String {
    if seconds < 0 {
        format!("-{}s", seconds.unsigned_abs())
    } else {
        format!("+{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use fleetringd::{Config, OnShutdown};

    use super::*;

    #[test]
    fn rendered_configs_are_valid_daemon_configs() {
        for node in Node::ALL {
            let text = render_config(node);
            let config =
                Config::from_toml(&text).unwrap_or_else(|error| panic!("{node}: {error}\n{text}"));
            assert_eq!(config.role, node.role(), "{node}");
            assert_eq!(config.node.id.as_str(), node.name());
            assert_eq!(
                config.node.advertise.map(|address| address.to_string()),
                Some(node.address())
            );
            assert_eq!(config.membership.on_shutdown, OnShutdown::Stay);
            assert_eq!(config.principals.len(), 1, "{node}");
            let principal = &config.principals[0];
            assert_eq!(principal.name.to_string(), "chaos");
            let scopes: Vec<String> = principal
                .scopes
                .as_slice()
                .iter()
                .map(ToString::to_string)
                .collect();
            assert_eq!(scopes, ["admin:chaos/*", "fleet"], "{node}");
        }
    }

    #[test]
    fn only_the_control_node_runs_control() {
        let control = render_config(Node::N1);
        assert!(control.contains("role = \"both\""), "{control}");
        for node in &Node::ALL[1..] {
            let text = render_config(*node);
            assert!(text.contains("role = \"data\""), "{text}");
        }
    }

    #[test]
    fn every_node_dials_the_control_node_at_its_bridge_address() {
        // Including the control node itself, whose certificate names only that address.
        for node in Node::ALL {
            let text = render_config(node);
            assert!(
                text.contains("[control]\naddress = \"10.77.0.11:7400\""),
                "{text}"
            );
            let config = Config::from_toml(&text).expect("config");
            assert_eq!(
                config.control.address.map(|address| address.to_string()),
                Some(Node::CONTROL.address())
            );
        }
    }

    #[test]
    fn the_second_node_renders_exactly() {
        let expected = "data_dir = \"/data\"
listen = \"0.0.0.0:7400\"
role = \"data\"

[node]
id = \"n2\"
advertise = \"10.77.0.12:7400\"

[control]
address = \"10.77.0.11:7400\"

[membership]
on_shutdown = \"stay\"

[tls]
ca = \"/tls/ca.crt\"
cert = \"/tls/node.n2.crt\"
key = \"/tls/node.n2.key\"

[[principal]]
name = \"chaos\"
scopes = [\"admin:chaos/*\", \"fleet\"]
";
        assert_eq!(render_config(Node::N2), expected);
    }

    #[test]
    fn offsets_are_signed_seconds() {
        assert_eq!(faketime_offset(0), "+0s");
        assert_eq!(faketime_offset(42), "+42s");
        assert_eq!(faketime_offset(-42), "-42s");
    }
}
