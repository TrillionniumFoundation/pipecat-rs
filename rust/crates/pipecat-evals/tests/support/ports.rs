// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Checked four-port blocks for concurrent native-child integration tests.
use std::sync::atomic::{AtomicU64, Ordering};

pub fn port() -> u16 {
    // Build the complete test-server pool before returning its first port. No
    // temporary reservation FD can then overlap this test process's child
    // spawns. Hosted runs still execute concurrently, with no retries.
    static POOL: std::sync::OnceLock<Vec<u16>> = std::sync::OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let pool = POOL.get_or_init(|| {
        let ephemeral = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
            .ok()
            .and_then(|raw| {
                let mut ports = raw.split_whitespace().map(str::parse::<u16>);
                Some((ports.next()?.ok()?, ports.next()?.ok()?))
            });
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as u64
            + u64::from(std::process::id()) * 997;
        let mut pool = Vec::new();
        for index in 0..5000 {
            let base = 10000 + ((seed + index) % 5000) as u16 * 4;
            if (base..base + 4)
                .any(|p| ephemeral.is_some_and(|(start, end)| (start..=end).contains(&p)))
            {
                continue;
            }
            let reservations = (base..base + 4)
                .map(|p| std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, p)))
                .collect::<std::io::Result<Vec<_>>>();
            if let Ok(reservations) = reservations {
                drop(reservations);
                pool.push(base);
                if pool.len() == 256 {
                    break;
                }
            }
        }
        assert_eq!(
            pool.len(),
            256,
            "insufficient free test-server blocks outside the ephemeral range"
        );
        pool
    });
    *pool
        .get(NEXT.fetch_add(1, Ordering::Relaxed) as usize)
        .expect("test-server pool exhausted")
}
