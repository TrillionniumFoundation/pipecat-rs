# Synthetic EventStream signing corpus

These fixtures are independently calculated with Python's standard library
(`hashlib`, `hmac`, `zlib`, `struct`, `base64`), not the production Rust signer or
an AWS SDK. They contain only the public `AKIDEXAMPLE` synthetic credential pair
and a synthetic session token. Generation does not read credentials, environment
variables, profiles or any network source.

The six request vectors cover regional and IPv4/IPv6 loopback authorities,
explicit local ports, session token present/absent, `%3A` wire / `%253A` canonical
model paths, an unreserved model path, epoch and subsecond timestamps, and UTC
midnight rollover. Every chain has three complete nested chunk events and a
separate signed zero-payload terminal. A second fixture covers all ten primitive
wire-header type codes, including both boolean values. JSON records retain the
canonical requests, scopes, strings to sign, hashes, signatures and full bytes.
The Rust fixture is the same data rendered without a JSON/development dependency.

The retained published fixed string-to-sign is from Amazon's Apache-2.0 source:
[aws-sigv4 1.6.0 event_stream.rs](https://docs.rs/aws-sigv4/1.6.0/src/aws_sigv4/event_stream.rs.html),
`tests::string_to_sign`, decoded-source SHA-256
`7b193cd957f779e95e61280851e43507bd3d9bbbd7f843ba283452eaaf7f0be1`.
It supplies an external fixed oracle in addition to the independent generator.
The corpus does not claim the actual AWS SDK was executed.

From the repository root:

```sh
python3 rust/crates/pipecat-aws-common/tests/fixtures/generate_event_stream_vectors.py --generate
python3 rust/crates/pipecat-aws-common/tests/fixtures/generate_event_stream_vectors.py --verify
```

Optional `--evidence-dir PATH` copies generated artifacts or writes verification
results to the caller-selected directory. Generation and verification are
separate modes so the designated test owner can execute acceptance independently.
Verification checks the published vector, headers, both CRCs, nested JSON/blob/
PCM envelopes, request signature and chain, and rejects missing/reordered/
duplicated events, repeated/missing terminal, changed lengths/CRCs, changed
signature/date/type/order/duplicate headers, altered inner data with repaired
CRCs, and wrong predecessor signatures. It is a signing/framing fixture peer,
not a complete Nova state machine, HTTP/2 peer, or live AWS validation.
