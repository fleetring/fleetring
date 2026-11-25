//! The `docker` command lines that build the image, start the nodes, fault them, and stop them,
//! and the executor that runs them (ADR-0036, point 2).
//!
//! Every builder is pure and returns the arguments after `docker`. Paths are joined with `/`:
//! the harness runs only on Linux, and the builders' tests pass on every OS.

use std::fmt;
use std::future::Future;
use std::net::Ipv4Addr;
use std::path::Path;
use std::pin::Pin;
use std::process::{Command, Output};

use crate::node::{FLEET, GATEWAY, NETWORK, Node, PRINCIPAL, SUBNET};

/// Where libfaketime is in the node image.
pub const LIBFAKETIME: &str = "/usr/local/lib/libfaketime.so.1";
/// Where a node's clock file is mounted in its container.
pub const CLOCK_DIR: &str = "/var/lib/faketime";
/// The file under [`CLOCK_DIR`] libfaketime reads the offset from.
pub const CLOCK_FILE: &str = "offset";
/// How long `docker stop` lets a daemon shut down before it kills it, in seconds.
pub const STOP_TIMEOUT_S: u32 = 10;

/// A signal the harness sends a node's process with `docker kill`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Signal {
    /// `SIGKILL`: the process dies at once.
    Kill,
    /// `SIGSTOP`: the process stops, keeping its sockets and memory.
    Stop,
    /// `SIGCONT`: a stopped process continues.
    Cont,
}

impl Signal {
    /// Returns the signal's name as `docker kill --signal` takes it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Kill => "KILL",
            Self::Stop => "STOP",
            Self::Cont => "CONT",
        }
    }
}

fn args<const N: usize>(words: [&str; N]) -> Vec<String> {
    words.into_iter().map(str::to_owned).collect()
}

/// `network create`: the nodes' bridge network.
#[must_use]
pub fn network_create_args() -> Vec<String> {
    args([
        "network",
        "create",
        "--driver",
        "bridge",
        "--subnet",
        SUBNET,
        "--gateway",
        GATEWAY,
        NETWORK,
    ])
}

/// `network rm`: removes the nodes' network.
#[must_use]
pub fn network_rm_args() -> Vec<String> {
    args(["network", "rm", NETWORK])
}

/// `build`: builds the node image as `image` from the workspace root.
#[must_use]
pub fn build_args(image: &str) -> Vec<String> {
    args(["build", "-f", "deploy/chaos/Dockerfile", "-t", image, "."])
}

/// `run`: starts `node`'s container from `image` as `user` (`uid:gid`), with its data
/// directory, configuration, and clock file mounted from the run directory `run_dir`, and the
/// fleet's certificates from `<run_dir>/tls`.
#[must_use]
pub fn run_args(node: Node, run_dir: &str, image: &str, user: &str) -> Vec<String> {
    let name = node.name();
    let node_dir = format!("{run_dir}/nodes/{name}");
    vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--name".to_owned(),
        node.container(),
        "--hostname".to_owned(),
        name.to_owned(),
        "--network".to_owned(),
        NETWORK.to_owned(),
        "--ip".to_owned(),
        node.ip().to_string(),
        "--cap-add".to_owned(),
        "NET_ADMIN".to_owned(),
        "--user".to_owned(),
        user.to_owned(),
        "--env".to_owned(),
        format!("LD_PRELOAD={LIBFAKETIME}"),
        "--env".to_owned(),
        format!("FAKETIME_TIMESTAMP_FILE={CLOCK_DIR}/{CLOCK_FILE}"),
        "--env".to_owned(),
        "FAKETIME_NO_CACHE=1".to_owned(),
        "--env".to_owned(),
        "FAKETIME_DONT_FAKE_MONOTONIC=1".to_owned(),
        "--env".to_owned(),
        "RUST_LOG=info".to_owned(),
        "--volume".to_owned(),
        format!("{node_dir}/data:/data"),
        "--volume".to_owned(),
        format!("{run_dir}/tls:/tls:ro"),
        "--volume".to_owned(),
        format!("{node_dir}/fleetringd.toml:/etc/fleetring/fleetringd.toml:ro"),
        "--volume".to_owned(),
        format!("{node_dir}/faketime:{CLOCK_DIR}"),
        image.to_owned(),
    ]
}

/// The `exec` prefix that runs a command as root in `container`'s network namespace.
fn exec_as_root(container: &str) -> Vec<String> {
    args(["exec", "--user", "0:0", container])
}

/// The two `exec ... iptables` commands that drop `container`'s traffic with `peer`, both ways.
#[must_use]
pub fn partition_args(container: &str, peer: Ipv4Addr) -> [Vec<String>; 2] {
    let peer = peer.to_string();
    let rule = |chain: &str, direction: &str| {
        let mut command = exec_as_root(container);
        command.extend(args([
            "iptables", "-A", chain, direction, &peer, "-j", "DROP",
        ]));
        command
    };
    [rule("INPUT", "-s"), rule("OUTPUT", "-d")]
}

/// The two `exec ... iptables -F` commands that flush `container`'s rules.
#[must_use]
pub fn heal_args(container: &str) -> [Vec<String>; 2] {
    let flush = |chain: &str| {
        let mut command = exec_as_root(container);
        command.extend(args(["iptables", "-F", chain]));
        command
    };
    [flush("INPUT"), flush("OUTPUT")]
}

/// `kill --signal`: sends `signal` to `container`'s process.
#[must_use]
pub fn signal_args(container: &str, signal: Signal) -> Vec<String> {
    args(["kill", "--signal", signal.name(), container])
}

/// `start`: starts a stopped container again.
#[must_use]
pub fn start_args(container: &str) -> Vec<String> {
    args(["start", container])
}

/// `stop`: stops a container with `SIGTERM`, killing it after [`STOP_TIMEOUT_S`] seconds.
#[must_use]
pub fn stop_args(container: &str) -> Vec<String> {
    vec![
        "stop".to_owned(),
        "--time".to_owned(),
        STOP_TIMEOUT_S.to_string(),
        container.to_owned(),
    ]
}

/// `logs`: a container's output.
#[must_use]
pub fn logs_args(container: &str) -> Vec<String> {
    args(["logs", container])
}

/// `rm --force`: removes `containers`, running or not.
#[must_use]
pub fn rm_args(containers: &[String]) -> Vec<String> {
    let mut command = args(["rm", "--force"]);
    command.extend(containers.iter().cloned());
    command
}

/// The `run --rm` prefix that runs the image's own `fleetring` command as `user`, with the TLS
/// directory `tls_dir` mounted at `/tls`.
fn cert_command(tls_dir: &str, image: &str, user: &str) -> Vec<String> {
    vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--user".to_owned(),
        user.to_owned(),
        "--volume".to_owned(),
        format!("{tls_dir}:/tls"),
        image.to_owned(),
        "cert".to_owned(),
    ]
}

/// `run ... cert init`: creates the fleet's authority in `tls_dir`.
#[must_use]
pub fn cert_init_args(tls_dir: &str, image: &str, user: &str) -> Vec<String> {
    let mut command = cert_command(tls_dir, image, user);
    command.extend(args(["init", "--fleet", FLEET, "--dir", "/tls"]));
    command
}

/// `run ... cert node`: issues `node`'s certificate for its address.
#[must_use]
pub fn cert_node_args(node: Node, tls_dir: &str, image: &str, user: &str) -> Vec<String> {
    let mut command = cert_command(tls_dir, image, user);
    let address = node.ip().to_string();
    command.extend(args([
        "node",
        node.name(),
        "--addresses",
        &address,
        "--dir",
        "/tls",
    ]));
    command
}

/// `run ... cert client`: issues the certificate the harness's clients present.
#[must_use]
pub fn cert_client_args(tls_dir: &str, image: &str, user: &str) -> Vec<String> {
    let mut command = cert_command(tls_dir, image, user);
    command.extend(args(["client", PRINCIPAL, "--dir", "/tls"]));
    command
}

/// Runs `docker` with arguments, and fails with its stderr when it exits non-zero.
#[derive(Debug, Clone, Copy, Default)]
pub struct Docker;

impl Docker {
    /// Runs `docker` with `args` and returns its stdout, trimmed.
    ///
    /// # Errors
    ///
    /// [`DockerError::Spawn`] if `docker` can't run, and [`DockerError::Failed`] with its stderr
    /// if it exits non-zero.
    pub fn run(&self, args: &[String]) -> Result<String, DockerError> {
        let output = output(Command::new("docker").args(args), args)?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Runs `docker` with `args` in the directory `dir`, as [`run`](Self::run) does: for a
    /// build, whose context is the directory.
    ///
    /// # Errors
    ///
    /// As [`run`](Self::run).
    pub fn run_in(&self, dir: &Path, args: &[String]) -> Result<String, DockerError> {
        let output = output(Command::new("docker").current_dir(dir).args(args), args)?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Runs `docker` with `args` and returns what it wrote to stdout, then what it wrote to
    /// stderr: for `docker logs`, which replays a container's two streams on its own two.
    ///
    /// # Errors
    ///
    /// As [`run`](Self::run).
    pub fn run_combined(&self, args: &[String]) -> Result<String, DockerError> {
        let output = output(Command::new("docker").args(args), args)?;
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        Ok(text)
    }

    /// Runs `docker` with `args` on Tokio's blocking pool, and returns its stdout, trimmed.
    ///
    /// # Errors
    ///
    /// As [`run`](Self::run), and [`DockerError::Join`] if the blocking task fails.
    pub async fn run_async(&self, args: Vec<String>) -> Result<String, DockerError> {
        let docker = *self;
        tokio::task::spawn_blocking(move || docker.run(&args))
            .await
            .map_err(|error| DockerError::Join {
                reason: error.to_string(),
            })?
    }

    /// Runs `docker` with `args` and ignores how it ends: for cleanup, where a container or a
    /// network may already be gone.
    pub fn run_ignoring_errors(&self, args: &[String]) {
        let _ = self.run(args);
    }
}

/// Runs `command`, whose arguments after `docker` are `args`, and returns its output if it
/// exits zero.
fn output(command: &mut Command, args: &[String]) -> Result<Output, DockerError> {
    let output = command
        .output()
        .map_err(|source| DockerError::Spawn { source })?;
    if !output.status.success() {
        return Err(DockerError::Failed {
            command: args.join(" "),
            status: output.status.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(output)
}

/// Runs docker commands for the fault injector: [`Docker`] in a run, a recorder in tests.
pub trait Runner: fmt::Debug + Send + Sync + 'static {
    /// Runs `docker` with `args` and returns its stdout.
    fn run(
        &self,
        args: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<String, DockerError>> + Send + '_>>;
}

impl Runner for Docker {
    fn run(
        &self,
        args: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<String, DockerError>> + Send + '_>> {
        Box::pin(self.run_async(args))
    }
}

/// Why a `docker` command failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DockerError {
    /// `docker` couldn't be started.
    #[error("can't run docker: {source}")]
    Spawn {
        /// The error from starting it.
        source: std::io::Error,
    },
    /// `docker` exited non-zero.
    #[error("docker {command} failed with {status}: {stderr}")]
    Failed {
        /// The arguments, joined with spaces.
        command: String,
        /// How it exited.
        status: String,
        /// What it wrote to stderr, trimmed.
        stderr: String,
    },
    /// The blocking task that ran `docker` panicked or was cancelled.
    #[error("docker blocking task failed: {reason}")]
    Join {
        /// What the task's error said.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<String> {
        text.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn run_args_mount_the_node_files_and_set_libfaketime() {
        let expected = words(
            "run --detach --name fleetring-chaos-n2 --hostname n2 \
             --network fleetring-chaos --ip 10.77.0.12 --cap-add NET_ADMIN --user 1000:1000 \
             --env LD_PRELOAD=/usr/local/lib/libfaketime.so.1 \
             --env FAKETIME_TIMESTAMP_FILE=/var/lib/faketime/offset \
             --env FAKETIME_NO_CACHE=1 \
             --env FAKETIME_DONT_FAKE_MONOTONIC=1 \
             --env RUST_LOG=info \
             --volume /r/nodes/n2/data:/data \
             --volume /r/tls:/tls:ro \
             --volume /r/nodes/n2/fleetringd.toml:/etc/fleetring/fleetringd.toml:ro \
             --volume /r/nodes/n2/faketime:/var/lib/faketime \
             img",
        );
        assert_eq!(run_args(Node::N2, "/r", "img", "1000:1000"), expected);
        let n1 = run_args(Node::N1, "/r", "img", "1000:1000");
        assert!(n1.contains(&"10.77.0.11".to_owned()), "{n1:?}");
        assert!(n1.contains(&"fleetring-chaos-n1".to_owned()), "{n1:?}");
        assert!(n1.contains(&"/r/nodes/n1/data:/data".to_owned()), "{n1:?}");
        assert!(
            n1.contains(&"/r/nodes/n1/faketime:/var/lib/faketime".to_owned()),
            "{n1:?}"
        );
    }

    #[test]
    fn partitions_drop_both_directions() {
        let [input, output] = partition_args("fleetring-chaos-n2", Ipv4Addr::new(10, 77, 0, 13));
        assert_eq!(
            input,
            words("exec --user 0:0 fleetring-chaos-n2 iptables -A INPUT -s 10.77.0.13 -j DROP")
        );
        assert_eq!(
            output,
            words("exec --user 0:0 fleetring-chaos-n2 iptables -A OUTPUT -d 10.77.0.13 -j DROP")
        );
    }

    #[test]
    fn a_heal_flushes_both_chains() {
        let [input, output] = heal_args("c");
        assert_eq!(input, words("exec --user 0:0 c iptables -F INPUT"));
        assert_eq!(output, words("exec --user 0:0 c iptables -F OUTPUT"));
    }

    #[test]
    fn signals_use_docker_kill() {
        assert_eq!(
            signal_args("c", Signal::Stop),
            words("kill --signal STOP c")
        );
        assert_eq!(
            signal_args("c", Signal::Kill),
            words("kill --signal KILL c")
        );
        assert_eq!(
            signal_args("c", Signal::Cont),
            words("kill --signal CONT c")
        );
    }

    #[test]
    fn certificates_come_from_the_image_under_test() {
        assert_eq!(
            cert_node_args(Node::N4, "/r/tls", "img", "1:2"),
            words(
                "run --rm --user 1:2 --volume /r/tls:/tls img cert node n4 --addresses 10.77.0.14 --dir /tls"
            )
        );
        assert_eq!(
            cert_init_args("/r/tls", "img", "1:2"),
            words(
                "run --rm --user 1:2 --volume /r/tls:/tls img cert init --fleet chaos --dir /tls"
            )
        );
        assert_eq!(
            cert_client_args("/r/tls", "img", "1:2"),
            words("run --rm --user 1:2 --volume /r/tls:/tls img cert client chaos --dir /tls")
        );
    }

    #[test]
    fn the_fleet_has_its_own_network_and_containers_stop_gracefully() {
        assert_eq!(
            network_create_args(),
            words(
                "network create --driver bridge --subnet 10.77.0.0/24 --gateway 10.77.0.1 fleetring-chaos"
            )
        );
        assert_eq!(network_rm_args(), words("network rm fleetring-chaos"));
        assert_eq!(stop_args("c"), words("stop --time 10 c"));
        assert_eq!(
            rm_args(&["a".to_owned(), "b".to_owned()]),
            words("rm --force a b")
        );
        assert_eq!(
            build_args("img"),
            words("build -f deploy/chaos/Dockerfile -t img .")
        );
    }
}
