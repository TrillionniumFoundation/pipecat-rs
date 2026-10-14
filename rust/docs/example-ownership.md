# Example ownership and failure cleanup

The `context_summary` and `llm_switching` examples retain all created owners
until their cleanup has completed. Their request/observation work returns an
outcome before terminal admission and joins run. Observation, terminal and join
errors are collected rather than allowing an early `?` to abandon other owners.
End admission and borrowed joins have deadlines; a failed or timed-out End
falls back to cancellation. After a join deadline, the worker is cancelled and
actually joined using its finite cooperative shutdown configuration.

`context_summary` still requires `--run` plus an explicitly configured model.
Usage-only execution sends no request. Both dedicated assistant inference and
separate summary-responder routing use the same cleanup function. JSON output
is printed only after the worker has joined. Start and summary observation are
bounded, premature output closure is an error, and worker reports are checked.

`llm_switching` still runs only its fixed credential-free loopback providers.
It never automatically replays the primary's failed request. A provider created
before a later construction/observation error is still stopped and joined, and
one provider's join failure cannot skip the other's cleanup. Fixture HTTP
headers/body are bounded to 64 KiB and reads have a deadline. Pending listener
accepts are explicitly stoppable; an in-flight client cancellation can be
observed as peer EOF before fixture shutdown.

## Local failure checks

Run just the two example test harnesses without contacting external services:

```sh
cargo test -p pipecat-turns --example context_summary --locked --offline
cargo test -p pipecat-pipeline --example llm_switching --locked --offline
```

Summary checks use actual loopback HTTP/SSE clients for success, HTTP failure,
and an unfinished response reaching the observation deadline, in both inference
routing modes. A cleanup witness verifies exactly one processor cleanup; the
unfinished response also requires peer EOF and a joined fixture server.
Switching checks cover successful failover, failure before creating the second
provider, observation failure after worker creation, and a streaming backup
hitting its deadline. They check all expected joins, explicit shutdown of idle
listeners, and peer EOF for the streaming response. Expected failures must not
hide additional terminal or join errors.

These are native example ownership checks. They do not change runtime/provider
behavior or qualify real models, devices, deployments, noncooperative code, or
all other examples in the repository.
