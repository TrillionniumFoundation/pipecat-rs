
## Optional native text-eval starter

`src/bin/eval-bot.rs` hosts an independent loopback-only text application.
Edit its prompt, tool and fresh-session factory in `src/eval_app.rs` and assertions
in `evals/starter_text.yaml`. This starter uses real native LLM streaming, context
aggregation and checked-integer `add` tool execution. Its prompt and tool are
separate from the voice runner; a passing scenario does not establish the voice
application's exact prompt/tools, speech path, playback or live-provider behavior.
Normal `cargo run` still selects the voice application through `default-run`.

1. Run your explicitly chosen local OpenAI-compatible chat-completions service
   separately. The generator and eval bot do not install, download, discover or
   launch a model server
2. Edit `evals/bot.json`: keep `native_text_bot_version` at `1`, set `llm_url` to
   the full HTTP chat-completions URL on a literal loopback IP (IPv4 or IPv6), and
   set `model` to that server's model identifier. The empty defaults intentionally
   fail before the bot opens any socket. DNS names including `localhost`, external
   IPs, URL credentials/query/fragment and unknown/duplicate fields are rejected
3. Build the actual separate executable and run the existing native release harness

```sh
cargo +1.99.0 build --bin eval-bot --target-dir target
cargo +1.99.0 run --bin eval-bot -- --help
# From the Pipecat source checkout's rust/ directory:
cargo +1.99.0 run -p pipecat-evals --bin pipecat-eval -- \
  suite /absolute/path/to/this/project/evals/native-manifest.yaml
```

Use `--offline` for cached dependencies and retain the generated application's own
`Cargo.lock`; subsequent builds can use `--locked`. The absolute local source
paths are location references, not content pins. Neither Cargo.lock nor the
recorded generation choices freeze the source checkout's contents. Record its
exact revision and local modifications when qualifying a build; preserve that
checkout or deliberately update paths and requalify after moving it. Rust's
`rust-version` is an MSRV declaration; select the pinned 1.99.0 toolchain explicitly.

The manifest resolves its scenario/model config beside itself and the compiled
bot under `../target/debug`, independently of the harness's working directory.
It never builds a missing executable or invokes a shell. The executable name uses
the generator host's suffix. Release builds, custom target directories and cross
compilation need a deliberate manifest binary-path edit. Moving the generated
project with its target/config/scenario preserves those relative paths; its Cargo
source dependencies still require the recorded external checkout location.

This path reads no `.env` or provider keys. It sends the fixed nonsecret
Authorization marker `Bearer native-loopback-eval` required by the native client;
the starter does not support secret-authenticated model servers. It uses no proxy,
redirect or automatic retry, with bounded connection/request/idle/event/channel
limits. Model config is a regular JSON file capped at 16 KiB, with an endpoint
capped at 4096 bytes and model at 256 bytes. Config errors omit supplied values.

The two user turns require an `add` call with arguments `{a: 2, b: 3}`, a response
containing `5`, then another response containing the remembered `5`. The bot waits
for the first input rather than generating an unsolicited greeting. These are
deterministic assertions, not semantic proof: argument matching is a shallow
subset and substring matching does not establish arithmetic correctness or exact
tool-call counts. A real model may fail them. Synthetic native integration tests
add request-history and actual tool-result checks; they do not qualify live models.

The default scenario needs no judge or credentials. Adding an `eval:` criterion
requires explicit `judge.eval` configuration (currently the native Jev schema)
or an embedding application's `EvalSession::with_judge`; otherwise harness
preflight fails before launch. There is no implicit Ollama/OpenAI judge discovery.
Jev's explicitly named key is read by the harness process, not the manifest's
child `env` map. `run_release` has no generic injected-judge factory.

The eval listener binds only `127.0.0.1`, rejects browser Origin and non-root or
foreign-authority handshakes, and creates fresh conversation/tool state for each
session. It does not add eval controls to the production browser `/api/start`.
Ctrl-C cancels then awaits the owned host future. The release harness owns and
reaps one direct compiled child; this is not a process-tree sandbox. Its default
2-second process grace can expire before the host's 5-second shutdown budget, so
slow cleanup may require a forced kill. Check scenario pass, process success,
reaped and forced outcomes separately; reaping alone is not success.

All eval files use the generator's ordinary exclusive-write rollback. Cooperative
cancellation, ordinary errors and unwinding clean up only owned files when cleanup
succeeds. Publication is not crash-atomic: concurrent readers can see partial files,
and power loss, SIGKILL or cleanup errors can leave residue.
