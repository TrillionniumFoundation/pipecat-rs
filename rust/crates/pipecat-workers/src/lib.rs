#![doc = include_str!("../README.md")]
//! Bounded native workers, owned trees, shared-ID job RPC, and pipeline bridges.
//!
//! This crate has its own typed Rust/serde protocol. It does not implement the
//! Python fully-qualified `__type__` wire format, Redis, or PGMQ transports.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod bridge;
mod bus;
mod jobs;
mod pipeline;
mod registry;
mod worker;

pub use bridge::*;
pub use bus::*;
pub use jobs::*;
pub use pipeline::*;
pub use registry::*;
pub use worker::*;

#[cfg(test)]
mod original_tests;
