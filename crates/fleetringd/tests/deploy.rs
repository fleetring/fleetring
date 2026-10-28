//! FR-034: the example configuration the container image starts with stays valid.

use std::error::Error;

use fleetringd::{Config, Role};

const EXAMPLE: &str = include_str!("../../../deploy/fleetringd.toml");

#[test]
fn the_example_config_runs_a_one_node_cluster_from_the_data_volume() -> Result<(), Box<dyn Error>> {
    let config = Config::from_toml(EXAMPLE)?;
    assert_eq!(config.data_dir.to_str(), Some("/data"));
    assert_eq!(config.listen.to_string(), "0.0.0.0:7400");
    assert_eq!(config.role, Role::Both);
    assert_eq!(config.node.id.as_str(), "node-1");
    assert_eq!(
        config.control_target().map(|a| a.to_string()).as_deref(),
        Some("127.0.0.1:7400")
    );
    Ok(())
}
