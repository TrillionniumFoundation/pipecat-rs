# Azure OpenAI Chat Completions

The native adapter uses the shared OpenAI request builder, bounded SSE decoder,
usage accounting, tool-call validation and processor interface. It invokes no
Python process or Azure SDK.

```rust,no_run
use pipecat_services::{AzureAuth, AzureConfig, ClientConfig, CompletionRequest,
    Context, LlmClient, Message, Role};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let azure = AzureConfig::new(
    "https://my-resource.openai.azure.com/openai/v1",
    AzureAuth::ApiKey("APPLICATION_SUPPLIED_KEY".into()),
)?;
let client = LlmClient::new(ClientConfig::azure(azure))?;
let request = CompletionRequest::new("my-deployment", Context::new(vec![
    Message::text(Role::User, "Hello"),
]));
# Ok(()) }
```

## Endpoints and authentication

| Configured endpoint | POST route | API key | Entra token |
| --- | --- | --- | --- |
| `https://RESOURCE/openai/v1` | `/openai/v1/chat/completions` | `Authorization: Bearer KEY` | `Authorization: Bearer TOKEN` |
| `https://RESOURCE` | `/openai/deployments/DEPLOYMENT/chat/completions?api-version=2025-04-01-preview` | `api-key: KEY` | `Authorization: Bearer TOKEN` |

Trailing slashes are accepted. The resolved request model is the Azure deployment
name and remains in the JSON body. In dated URLs it is encoded as exactly one path
segment; slashes, percent signs, spaces and query delimiters cannot become routing
syntax. Empty, control-containing and standalone dot-segment deployments fail
validation. Sparse runtime model settings select both the path and body together.

`AzureConfig::with_api_version` changes the dated version; v1 ignores it and sends
no `api-version`. URLs must be explicit HTTPS resource roots or `/openai/v1` bases.
HTTP is permitted only for loopback simulations. Userinfo, query strings,
fragments, whitespace, backslashes, percent-encoded base paths and other paths
are rejected. This intentionally stricter boundary does not implement custom
Azure gateway path prefixes or already-expanded completion URLs.

Applications can provide asynchronous Entra authentication:

```rust,no_run
use pipecat_services::{AzureAuth, AzureConfig, AzureTokenProvider};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let provider = AzureTokenProvider::new(|| async {
    // Obtain a token from application-owned, explicitly configured credentials.
    // The Python source recommends scope https://ai.azure.com/.default.
    Ok("APPLICATION_SUPPLIED_ENTRA_TOKEN".to_owned())
});
let azure = AzureConfig::new(
    "https://my-resource.openai.azure.com",
    AzureAuth::EntraTokenProvider(provider),
)?;
# Ok(()) }
```

Pure request validation never calls the provider. Each actual HTTP attempt calls
it once, including the optional retry. The callback is cancelled when the stream
is cancelled or dropped, and is bounded by the original total request deadline.
A slow callback alone cannot trigger a header retry. Callbacks must cooperate
with asynchronous cancellation and avoid blocking the runtime. The application
owns credential selection, scopes, caching and token refresh. There is no
ambient environment-key discovery, managed-identity/metadata lookup, Azure CLI
lookup, default credential chain, or built-in identity SDK.

## Transport, retries and diagnostics

Use `ClientConfig::azure` and retain its provider, endpoint, key and preset
fields. Mutating them into a conflicting combination fails client construction.
Timeouts, event/channel limits and `RetryPolicy` remain configurable. The generic
`OpenAiPreset::Azure` entry directs callers to the dedicated configuration; a
bare endpoint preset cannot select Azure authentication safely.

Azure uses a dedicated internal reqwest client with redirects, environment
proxies and reqwest's internal retries disabled. Custom HTTP clients are rejected
for Azure; generic clients retain their existing behavior. This requires
reqwest 0.12.28 or later. Private networking that requires a proxy or custom TLS
client is outside this API.

Retries default to `Never`. `OnceOnHeaderTimeout` permits one second attempt
before response headers, sharing the original total deadline. It can duplicate
billable provider work. Serialized body and routing are immutable, but Entra
authorization is reacquired for the second attempt. HTTP status failures,
credential callback failures, protocol failures and failures after streaming
starts are not retried.

Azure debug output omits endpoint, credentials and version values. Error
categories and HTTP status codes remain available, but provider/error bodies,
callback error strings, malformed stream data and transport details are replaced
with fixed diagnostics. `Retry-After` is omitted, including numeric values: an untrusted server can
reflect a numeric credential through that header. Unknown finish reasons
become `Other("azure_other")`. Known stop/tool/length/content-filter reasons,
usage and intentional model text/tool output remain available. Applications
must still treat successful generated output and tool arguments as private data.

## Scope and evidence

The source contract is the checked-in Python `services/azure/llm.py` and
`services/azure/common.py`: v1 suffix selection, dated version default, shared
OpenAI Chat request shape and explicit async token provider. The tests pin these
source facts and use real loopback TCP sockets with synthetic Azure responses.
They require no credentials. These are protocol simulations, not a live Azure
service certification.

Azure Realtime, Voice Live, STT/TTS, identity provisioning, content-filter
annotation metadata, deployment discovery and automatic tool execution remain
outside this Chat Completions adapter.
