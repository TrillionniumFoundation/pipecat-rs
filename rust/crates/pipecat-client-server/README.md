# Native client-template server proxy

Rust replacement for the transport-independent behavior of the shipped Next.js
`/api/start` POST and `/api/sessions/{sessionId}/{path}` POST/PATCH handlers.
`ClientProxy` sends real HTTP requests to a fixed configured bot endpoint; bearer
credentials stay in the server client and are absent from Debug/error messages.
Successful JSON responses and JavaScript-truthy `error` handling follow the source.

Additional defensive boundaries: loopback-IP or localhost-only plaintext, no redirects,
no URL credentials/query/fragment, validated path segments, bounded request and
response bodies, bounded concurrent network requests, and a total async deadline
including permit admission. Awaiting callers are owned by the caller; this is not
an unbounded background job queue. Dropping an operation drops its request and
releases its concurrency permit. Configuration must end in `/start`; base path
removal is suffix-aware rather than a global string replacement.

The `server::serve` host implements the three routes on a caller-bound literal
loopback listener. It refuses non-loopback binding, validates exact browser
origins, limits active connections and header/body sizes, bounds each complete
connection lifetime, and aborts/joins connection tasks on shutdown. It serves
only five preloaded public browser build assets, with correct WASM MIME and
no-store responses; there is no browser-selected filesystem access. Asset loading assumes a trusted,
stable local build directory; canonicalization is not a defense against a hostile
local process racing filesystem replacement. Explicit
allowed origins protect browser CSRF, not user authentication. Non-browser local
processes can forge Origin. Do not expose this development host through a public
reverse proxy or treat it as a multi-user production authorization boundary.

After building `rust/client-templates`, run the example with the generated `dist`
directory and your bot's configured start URL:

```
cargo run -p pipecat-client-server --example serve -- ../client-templates/dist http://127.0.0.1:7860/start 8080
```

The optional BOT_START_PUBLIC_API_KEY environment variable stays server-side.
Press Enter or close stdin to stop. No Node/Python server is required. Browser
WASM, physical microphone/video and live-provider qualification remain separate
from proxy loopback tests.

Intentional hardening relative to the source Next.js templates: session request
bodies must be valid JSON and are reserialized (raw non-JSON passthrough is not
supported); upstream failures return sanitized 502/504 rather than source error
strings; malformed input gets a precise 4xx. Upstream JSON null is rejected,
matching the source handler's failed `data.error` access. Successful array and
primitive JSON retain JavaScript truthiness behavior.

Validation uses real loopback TCP HTTP fixtures for request/response framing,
authorization, session routing, error redaction, redirects, bounds and timeout
permit recovery. No hosted credentials or live provider calls are used.
