use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpError, ServerConfig};
use std::time::{Duration, Instant};

#[test]
fn individually_finite_deadlines_reject_an_unrepresentable_combined_guard() {
    let now = Instant::now();
    // Find this platform's representable domain once, without assuming its
    // Instant representation. Both halves fit; their exact sum must not.
    let mut low = 0_u64;
    let mut high = u64::MAX;
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        if now.checked_add(Duration::from_secs(middle)).is_some() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let individual = Duration::from_secs(low / 2 + 1);
    assert!(now.checked_add(individual).is_some());
    let combined = individual.checked_add(individual);
    assert!(combined.is_none_or(|timeout| now.checked_add(timeout).is_none()));
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(
        "http://127.0.0.1:1/mcp",
    )));
    config.limits.request_timeout = individual;
    config.limits.close_timeout = individual;
    assert_eq!(
        McpClient::new(config).unwrap_err(),
        McpError::Config("combined tool timeout must be finite")
    );

    // Two stages fit together, but adding the connection budget must still be
    // checked before any callback can construct its outer timer.
    let third = Duration::from_secs(low / 3 + 1);
    let pair = third.checked_add(third).unwrap();
    assert!(now.checked_add(pair).is_some());
    assert!(pair
        .checked_add(third)
        .is_none_or(|timeout| now.checked_add(timeout).is_none()));
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(
        "http://127.0.0.1:1/mcp",
    )));
    config.limits.connect_timeout = third;
    config.limits.request_timeout = third;
    config.limits.close_timeout = third;
    assert_eq!(
        McpClient::new(config).unwrap_err(),
        McpError::Config("combined tool timeout must be finite")
    );
}
