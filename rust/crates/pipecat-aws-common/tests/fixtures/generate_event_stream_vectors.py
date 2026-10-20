#!/usr/bin/env python3
"""Independent, synthetic AWS EventStream vectors using Python's stdlib only.

No Pipecat production code, AWS SDK execution, live credentials or networking.
Run with --generate to write JSON/Rust fixtures. Run --verify separately for the
independent peer and adverse chain/CRC checks. Neither mode invokes Cargo.
"""
import argparse
import base64
import datetime as dt
import hashlib
import hmac
import json
from pathlib import Path
import struct
from urllib.parse import quote, urlsplit
import zlib

ACCESS = "AKIDEXAMPLE"
SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
TOKEN = "SYNTHETIC/TOKEN+="
SDK_SOURCE = "https://docs.rs/aws-sigv4/1.6.0/src/aws_sigv4/event_stream.rs.html"
SDK_SOURCE_SHA256 = "7b193cd957f779e95e61280851e43507bd3d9bbbd7f843ba283452eaaf7f0be1"
SDK_PUBLISHED = "\n".join([
    "AWS4-HMAC-SHA256-PAYLOAD",
    "19731129T213309Z",
    "19731129/us-east-1/testservice/aws4_request",
    "be1f8c7d79ef8e1abc5254a2c70e4da3bfaf4f07328f527444e1fc6ea67273e2",
    "0c0e3b3bf66b59b976181bd7d401927bbd624107303c713fd1e5f3d3c8dd1b1e",
    "f2eba0f2e95967ee9fbc6db5e678d2fd599229c0d04b11e4fc8e0f2a02a806c6",
])


def digest(data):
    return hashlib.sha256(data).hexdigest()


def keyed(key, data):
    return hmac.new(key, data, hashlib.sha256).digest()


def signing_key(day, region, service):
    key = ("AWS4" + SECRET).encode()
    for component in (day, region, service, "aws4_request"):
        key = keyed(key, component.encode())
    return key


def timestamp(seconds):
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def epoch(value):
    return int(dt.datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc).timestamp())


def header(name, typ, value):
    name = name.encode()
    result = bytes([len(name)]) + name + bytes([typ])
    if typ in (0, 1):
        return result
    if typ in (2, 3, 4, 5, 8):
        return result + struct.pack({2: ">b", 3: ">h", 4: ">i", 5: ">q", 8: ">q"}[typ], value)
    if typ in (6, 7):
        raw = value.encode() if typ == 7 else value
        return result + struct.pack(">H", len(raw)) + raw
    if typ == 9:
        return result + value
    raise ValueError("unknown header type")


def frame(headers, payload):
    raw = b"".join(header(*item) for item in headers)
    prelude = struct.pack(">II", 16 + len(raw) + len(payload), len(raw))
    body = prelude + struct.pack(">I", zlib.crc32(prelude)) + raw + payload
    return body + struct.pack(">I", zlib.crc32(body))


def parse_frame(raw):
    """Independent bounded reader, also used in --verify rejection checks."""
    if not 16 <= len(raw) <= 16 * 1024 * 1024:
        raise ValueError("frame size")
    total, header_len, crc = struct.unpack(">III", raw[:12])
    if total != len(raw) or header_len > total - 16 or header_len > 128 * 1024:
        raise ValueError("prelude lengths")
    if crc != zlib.crc32(raw[:8]) or struct.unpack(">I", raw[-4:])[0] != zlib.crc32(raw[:-4]):
        raise ValueError("CRC")
    encoded_headers = memoryview(raw)[12:12 + header_len]
    cursor = 0
    entries = []
    names = set()

    def take(size):
        nonlocal cursor
        if size < 0 or cursor + size > len(encoded_headers):
            raise ValueError("header size")
        part = bytes(encoded_headers[cursor:cursor + size])
        cursor += size
        return part

    while cursor < len(encoded_headers):
        name = take(take(1)[0]).decode()
        if not name or name in names or len(entries) >= 128:
            raise ValueError("header name")
        names.add(name)
        typ = take(1)[0]
        if typ in (0, 1):
            value = typ == 0
        elif typ in (2, 3, 4, 5, 8):
            fmt = {2: ">b", 3: ">h", 4: ">i", 5: ">q", 8: ">q"}[typ]
            value = struct.unpack(fmt, take(struct.calcsize(fmt)))[0]
        elif typ in (6, 7):
            value = take(struct.unpack(">H", take(2))[0])
            if typ == 7:
                value = value.decode()
        elif typ == 9:
            value = take(16)
        else:
            raise ValueError("header type")
        entries.append((name, typ, value))
    return entries, raw[12 + header_len:-4]


def http_request(url, region, token, seconds):
    parsed = urlsplit(url)
    stamp = timestamp(seconds)
    scope = f"{stamp[:8]}/{region}/bedrock/aws4_request"
    values = {
        "content-type": "application/vnd.amazon.eventstream",
        "host": parsed.netloc,
        "x-amz-date": stamp,
    }
    if token is not None:
        values["x-amz-security-token"] = token
    names = ";".join(sorted(values))
    canonical_uri = quote(parsed.path, safe="/-_.~")
    canonical = "\n".join([
        "POST", canonical_uri, "",
        "".join(f"{name}:{values[name]}\n" for name in sorted(values)),
        names, digest(b""),
    ])
    sts = f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{digest(canonical.encode())}"
    signature = keyed(signing_key(stamp[:8], region, "bedrock"), sts.encode()).hex()
    authorization = f"AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, SignedHeaders={names}, Signature={signature}"
    return {
        "host": parsed.netloc, "timestamp": stamp, "canonical_uri": canonical_uri,
        "canonical_request": canonical, "canonical_request_sha256": digest(canonical.encode()),
        "signed_header_names": names, "string_to_sign": sts, "signature": signature,
        "authorization": authorization,
    }


def event_sts(payload, previous, region, service, seconds):
    stamp = timestamp(seconds)
    return "\n".join([
        "AWS4-HMAC-SHA256-PAYLOAD", stamp,
        f"{stamp[:8]}/{region}/{service}/aws4_request", previous,
        digest(header(":date", 8, seconds * 1000)), digest(payload),
    ])


def sign_event(payload, previous, region, service, seconds, nanos, terminal=False):
    sts = event_sts(payload, previous, region, service, seconds)
    signature = keyed(signing_key(timestamp(seconds)[:8], region, service), sts.encode())
    raw = frame([(":chunk-signature", 6, signature), (":date", 8, seconds * 1000)], payload)
    return {
        "seconds": seconds, "nanos": nanos, "truncated_millis": seconds * 1000,
        "terminal": terminal, "inner_hex": payload.hex(), "outer_hex": raw.hex(),
        "signature": signature.hex(), "previous_signature": previous, "string_to_sign": sts,
    }


def json_bytes(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def chunk(event):
    nova = json_bytes(event)
    return frame([
        (":message-type", 7, "event"), (":event-type", 7, "chunk"),
        (":content-type", 7, "application/json"),
    ], json_bytes({"bytes": base64.b64encode(nova).decode()}))


TYPED_HEADERS = [
    ("true", 0, True), ("false", 1, False), ("byte", 2, -128),
    ("short", 3, -32768), ("integer", 4, -2147483648),
    ("long", 5, -9223372036854775808), ("bytes", 6, bytes([0, 255, 128])),
    ("string", 7, "hello / 世界"), ("timestamp", 8, 1234567890123),
    ("uuid", 9, bytes(range(16))),
]


def corpus():
    cases = [
        ("regional_without_token", "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream", "us-east-1", None, "20241010T010203Z", 987654321),
        ("regional_with_token", "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream", "us-east-1", TOKEN, "20241010T010203Z", 1234),
        ("loopback_midnight_token", "http://127.0.0.1:41234/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream", "eu-west-1", TOKEN, "20261231T235959Z", 999999999),
        ("ipv6_midnight_without_token", "http://[::1]:8123/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream", "us-west-2", None, "20261231T235959Z", 500000000),
        ("regional_unreserved_model", "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-2-5-sonic/invoke-with-bidirectional-stream", "us-east-1", TOKEN, "20241010T010203Z", 0),
        ("china_epoch_without_token", "https://bedrock-runtime.cn-north-1.amazonaws.com.cn/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream", "cn-north-1", None, "19700101T000000Z", 1),
    ]
    inner = [
        chunk({"event": {"sessionStart": {"inferenceConfiguration": {"maxTokens": 1024, "topP": 0.9, "temperature": 0.7}}}}),
        chunk({"event": {"audioInput": {"promptName": "prompt-01", "contentName": "audio-01", "content": base64.b64encode(bytes([0, 0, 255, 127, 0, 128])).decode()}}}),
        chunk({"event": {"sessionEnd": {}}}),
    ]
    vectors = []
    for name, url, region, token, stamp, nanos in cases:
        seconds = epoch(stamp)
        request = http_request(url, region, token, seconds)
        previous = request["signature"]
        events = []
        for index, payload in enumerate(inner + [b""]):
            signed = sign_event(payload, previous, region, "bedrock", seconds + index, [999999999, 123456789, 1234, 0][index], index == 3)
            events.append(signed)
            previous = signed["signature"]
        vectors.append({"name": name, "url": url, "region": region, "token": token, "request_seconds": seconds, "request_nanos": nanos, "request": request, "events": events})
    sdk_inner = frame([("some-header", 7, "value")], b"test payload")
    sdk_previous = digest(b"last message sts")
    sdk_computed = event_sts(sdk_inner, sdk_previous, "us-east-1", "testservice", 123456789)
    return {
        "provenance": {"generator": "Python stdlib hashlib/hmac/zlib/struct/base64, no SDK execution", "credentials": "published synthetic AKIDEXAMPLE only", "sdk_source": SDK_SOURCE, "sdk_source_sha256": SDK_SOURCE_SHA256},
        "sdk_vector": {"published_string_to_sign": SDK_PUBLISHED, "independently_computed_string_to_sign": sdk_computed, "inner_hex": sdk_inner.hex(), "published_timestamp_seconds": 123456789, "published_timestamp_nanos": 1234},
        "all_typed_headers_frame_hex": frame(TYPED_HEADERS, b"typed payload").hex(),
        "vectors": vectors,
    }


def rust_string(value):
    # Rust string literals accept the ASCII JSON escaping used by these fields.
    return json.dumps(value, ensure_ascii=False)


def rust_fixture(data):
    lines = [
        "// Generated independently by generate_event_stream_vectors.py. Synthetic credentials only.",
        "pub struct StreamVector {", "    pub name: &'static str,", "    pub url: &'static str,",
        "    pub region: &'static str,", "    pub token: Option<&'static str>,",
        "    pub request_seconds: u64,", "    pub request_nanos: u32,", "    pub host: &'static str,",
        "    pub timestamp: &'static str,", "    pub authorization: &'static str,",
        "    pub events: &'static [EventVector],", "}",
        "pub struct EventVector {", "    pub seconds: u64,", "    pub nanos: u32,",
        "    pub truncated_millis: i64,", "    pub terminal: bool,", "    pub inner_hex: &'static str,",
        "    pub outer_hex: &'static str,", "    pub signature: &'static str,", "}",
        "#[rustfmt::skip]",
        "pub const ALL_TYPED_HEADERS_FRAME_HEX: &str = " + rust_string(data["all_typed_headers_frame_hex"]) + ";",
        "#[rustfmt::skip]",
        "pub const VECTORS: &[StreamVector] = &[",
    ]
    for case in data["vectors"]:
        lines.append("    StreamVector {")
        for field in ("name", "url", "region"):
            lines.append(f"        {field}: {rust_string(case[field])},")
        token = "None" if case["token"] is None else f"Some({rust_string(case['token'])})"
        lines.append(f"        token: {token},")
        for field in ("request_seconds", "request_nanos"):
            lines.append(f"        {field}: {case[field]},")
        for field in ("host", "timestamp", "authorization"):
            lines.append(f"        {field}: {rust_string(case['request'][field])},")
        lines.append("        events: &[")
        for event in case["events"]:
            lines.append("            EventVector {")
            for field in ("seconds", "nanos", "truncated_millis"):
                lines.append(f"                {field}: {event[field]},")
            lines.append(f"                terminal: {str(event['terminal']).lower()},")
            for field in ("inner_hex", "outer_hex", "signature"):
                lines.append(f"                {field}: {rust_string(event[field])},")
            lines.append("            },")
        lines += ["        ],", "    },"]
    return "\n".join(lines + ["];", ""])


def verify_chain(case, frames):
    request = http_request(case["url"], case["region"], case["token"], case["request_seconds"])
    if request != case["request"]:
        raise ValueError("stored request disagrees with independent computation")
    previous = request["signature"]
    terminal = False
    count = 0
    for raw in frames:
        if terminal:
            raise ValueError("after terminal")
        headers, payload = parse_frame(raw)
        if len(headers) != 2 or headers[0][:2] != (":chunk-signature", 6) or headers[1][:2] != (":date", 8):
            raise ValueError("signed header order/type")
        signature, millis = headers[0][2], headers[1][2]
        if len(signature) != 32 or millis < 0 or millis % 1000:
            raise ValueError("signed header value")
        seconds = millis // 1000
        text = event_sts(payload, previous, case["region"], "bedrock", seconds)
        expected = keyed(signing_key(timestamp(seconds)[:8], case["region"], "bedrock"), text.encode())
        if not hmac.compare_digest(signature, expected):
            raise ValueError("chain signature")
        previous = signature.hex()
        if payload:
            inner_headers, content = parse_frame(payload)
            if inner_headers != [(":message-type", 7, "event"), (":event-type", 7, "chunk"), (":content-type", 7, "application/json")]:
                raise ValueError("inner headers")
            encoded = json.loads(content)
            nova = json.loads(base64.b64decode(encoded["bytes"], validate=True))
            if "audioInput" in nova["event"]:
                pcm = base64.b64decode(nova["event"]["audioInput"]["content"], validate=True)
                if pcm != bytes([0, 0, 255, 127, 0, 128]):
                    raise ValueError("nested PCM")
        else:
            terminal = True
        count += 1
    if not terminal:
        raise ValueError("missing terminal")
    return count


def verify(data):
    if data["sdk_vector"]["published_string_to_sign"] != data["sdk_vector"]["independently_computed_string_to_sign"]:
        raise ValueError("SDK published vector disagreement")
    all_typed, payload = parse_frame(bytes.fromhex(data["all_typed_headers_frame_hex"]))
    if all_typed != TYPED_HEADERS or payload != b"typed payload":
        raise ValueError("typed vector disagreement")
    valid, rejected = 0, 0
    for case in data["vectors"]:
        frames = [bytes.fromhex(event["outer_hex"]) for event in case["events"]]
        valid += verify_chain(case, frames)
        mutations = [frames[1:], [frames[1], frames[0]] + frames[2:], [frames[0]] + frames, frames + [frames[-1]], frames[:-1]]
        # Both CRCs, length, signature, date and inner byte changes. Some cases
        # recompute outer CRC to demonstrate authentication, not just integrity.
        for index in (0, 8, 29, 32, 78, len(frames[0]) - 6, len(frames[0]) - 1):
            changed = bytearray(frames[0])
            changed[index] ^= 1
            mutations.append([bytes(changed)] + frames[1:])
            if index not in (0, 8, len(frames[0]) - 1):
                changed[-4:] = struct.pack(">I", zlib.crc32(changed[:-4]))
                mutations.append([bytes(changed)] + frames[1:])
        outer_headers, inner = parse_frame(frames[0])
        signature = outer_headers[0][2]
        millis = outer_headers[1][2]
        # These envelopes have valid framing CRCs but malformed authentication
        # metadata, a different date, or a different independently framed inner.
        for headers in [
            list(reversed(outer_headers)),
            outer_headers + [(":date", 8, millis)],
            [(":chunk-signature", 6, signature[:-1]), (":date", 8, millis)],
            [(":chunk-signature", 7, signature.hex()), (":date", 8, millis)],
            [(":chunk-signature", 6, signature), (":date", 5, millis)],
            [(":chunk-signature", 6, signature), (":date", 8, millis + 1000)],
        ]:
            mutations.append([frame(headers, inner)] + frames[1:])
        inner_headers, inner_payload = parse_frame(inner)
        changed_inner = frame(inner_headers, inner_payload + b" ")
        mutations.append([frame(outer_headers, changed_inner)] + frames[1:])
        wrong_previous = sign_event(inner, "00" * 32, case["region"], "bedrock", millis // 1000, 0)
        mutations.append([bytes.fromhex(wrong_previous["outer_hex"])] + frames[1:])
        for mutated in mutations:
            try:
                verify_chain(case, mutated)
            except (ValueError, UnicodeDecodeError, KeyError, struct.error):
                rejected += 1
            else:
                raise ValueError("mutation was accepted")
    return {"verified_request_cases": len(data["vectors"]), "verified_signed_frames": valid, "rejected_mutated_chains": rejected, "sdk_published_vector": "matched", "typed_header_vector": "matched"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    fixture_dir = Path(__file__).resolve().parent
    if args.generate:
        data = corpus()
        (fixture_dir / "event_stream_vectors.rs").write_text(rust_fixture(data))
        (fixture_dir / "event_stream_vectors.json").write_text(json.dumps(data, indent=2) + "\n")
        (fixture_dir / "aws_sdk_event_stream_string_to_sign.txt").write_text(SDK_PUBLISHED + "\n")
        if args.evidence_dir:
            args.evidence_dir.mkdir(parents=True, exist_ok=True)
            for name in ("event_stream_vectors.rs", "event_stream_vectors.json", "aws_sdk_event_stream_string_to_sign.txt", Path(__file__).name):
                (args.evidence_dir / name).write_bytes((fixture_dir / name).read_bytes())
        print(f"Generated {len(data['vectors'])} independent request cases and {sum(len(c['events']) for c in data['vectors'])} signed frames. Verification is separate (--verify).")
    if args.verify:
        data = json.loads((fixture_dir / "event_stream_vectors.json").read_text())
        result = verify(data)
        print(json.dumps(result, indent=2))
        if args.evidence_dir:
            (args.evidence_dir / "independent-verification.json").write_text(json.dumps(result, indent=2) + "\n")
    if not args.generate and not args.verify:
        parser.error("choose --generate and/or --verify")


if __name__ == "__main__":
    main()
