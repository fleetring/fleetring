//! FR-034: the example configuration the container image starts with stays valid.

use std::error::Error;

use fleetringd::{AdminListener, AdminTls, Config, LogFormat, NodeRole};

const EXAMPLE: &str = include_str!("../../../deploy/fleetringd.toml");

#[test]
fn the_example_config_runs_a_one_node_fleet_from_the_data_volume() -> Result<(), Box<dyn Error>> {
    let config = Config::from_toml(EXAMPLE)?;
    assert_eq!(config.data_dir.to_str(), Some("/data"));
    assert_eq!(config.listen.to_string(), "0.0.0.0:7400");
    assert_eq!(config.role, NodeRole::Both);
    assert_eq!(config.node.id.as_str(), "node-1");
    assert_eq!(
        config.control_target().map(|a| a.to_string()).as_deref(),
        Some("127.0.0.1:7400")
    );
    // The admin listener runs on loopback in plaintext, and logs are text, unless configured.
    assert_eq!(config.admin.enabled, AdminListener::Enabled);
    assert_eq!(config.admin.listen.to_string(), "127.0.0.1:7401");
    assert_eq!(config.admin.tls, AdminTls::Off);
    assert_eq!(config.log.format, LogFormat::Text);
    Ok(())
}
