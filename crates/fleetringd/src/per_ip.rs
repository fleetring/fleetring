//! A count of what each client address holds, such as connections or handshakes in progress, so
//! a listener can refuse one address more than its share.
//!
//! An IPv6 client counts by its /64, the block one host is usually given, so that a host can't
//! take a share per address it picks from its block. An IPv4-mapped IPv6 address counts as its
//! IPv4 address.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};

/// How many places each client address holds, up to `max` each.
#[derive(Debug)]
pub(crate) struct PerIp {
    max: usize,
    held: Mutex<BTreeMap<IpAddr, usize>>,
}

impl PerIp {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            held: Mutex::default(),
        }
    }

    /// A place for one more from `peer`, unless its address holds `max` already.
    pub(crate) fn hold(this: &Arc<Self>, peer: SocketAddr) -> Option<Held> {
        let ip = counted_as(peer.ip());
        let mut held = this.held.lock().expect("per-address count lock poisoned");
        let count = held.entry(ip).or_default();
        if *count >= this.max {
            return None;
        }
        *count += 1;
        Some(Held {
            per_ip: Arc::clone(this),
            ip,
        })
    }
}

/// The address `ip` counts under: an IPv6 address's /64, or the IPv4 address it maps.
fn counted_as(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let [a, b, c, d, ..] = v6.segments();
                IpAddr::V6(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0))
            }
        },
    }
}

/// One place in its address's count, given back as it drops.
#[derive(Debug)]
pub(crate) struct Held {
    per_ip: Arc<PerIp>,
    ip: IpAddr,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut held = self
            .per_ip
            .held
            .lock()
            .expect("per-address count lock poisoned");
        if let Some(count) = held.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                held.remove(&self.ip);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An address gets `max` places, and one more once a place is given back; another address
    /// counts apart.
    #[test]
    fn an_address_holds_at_most_max_places() {
        let per_ip = Arc::new(PerIp::new(2));
        let one: SocketAddr = "192.0.2.1:1000".parse().expect("an address");
        let other: SocketAddr = "192.0.2.2:1000".parse().expect("an address");
        let first = PerIp::hold(&per_ip, one).expect("a first place");
        let _second = PerIp::hold(&per_ip, one).expect("a second place");
        assert!(PerIp::hold(&per_ip, one).is_none(), "a third place");
        assert!(PerIp::hold(&per_ip, other).is_some(), "another address");
        drop(first);
        assert!(PerIp::hold(&per_ip, one).is_some(), "a place given back");
    }

    /// Addresses in one IPv6 /64 share a count, and an IPv4-mapped address counts as its IPv4
    /// address.
    #[test]
    fn an_ipv6_64_and_a_mapped_address_count_together() {
        let per_ip = Arc::new(PerIp::new(1));
        let in_block: SocketAddr = "[2001:db8:1:2::1]:1000".parse().expect("an address");
        let same_block: SocketAddr = "[2001:db8:1:2:ffff::9]:1000".parse().expect("an address");
        let next_block: SocketAddr = "[2001:db8:1:3::1]:1000".parse().expect("an address");
        let _held = PerIp::hold(&per_ip, in_block).expect("a place");
        assert!(PerIp::hold(&per_ip, same_block).is_none(), "the same /64");
        assert!(PerIp::hold(&per_ip, next_block).is_some(), "another /64");

        let v4: SocketAddr = "192.0.2.1:1000".parse().expect("an address");
        let mapped: SocketAddr = "[::ffff:192.0.2.1]:1000".parse().expect("an address");
        let _held = PerIp::hold(&per_ip, v4).expect("a place");
        assert!(PerIp::hold(&per_ip, mapped).is_none(), "the mapped address");
    }
}
