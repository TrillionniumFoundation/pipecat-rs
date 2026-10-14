use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_browser_client::{image_entry::*, protocol, text_entry::*, Transport};
use serde_json::{json, Value};
use std::io::Write;
fn entry() -> ImageEntry {
    let mut entry = ImageEntry::new(7);
    assert!(entry.accept_ready(
        &json!({"id":"7","label":"rtvi-ai","type":"bot-ready","data":{"version":"2.2.0"}})
    ));
    entry
}
fn chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut bytes = (data.len() as u32).to_be_bytes().to_vec();
    bytes.extend(kind);
    bytes.extend(data);
    bytes.extend(crc32fast::hash(&bytes[4..]).to_be_bytes());
    bytes
}
fn png(raw: &[u8], interlaced: bool) -> Vec<u8> {
    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    z.write_all(raw).unwrap();
    png_compressed(&z.finish().unwrap(), interlaced)
}
fn png_compressed(z: &[u8], interlaced: bool) -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = vec![0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0];
    ihdr[12] = u8::from(interlaced);
    bytes.extend(chunk(b"IHDR", &ihdr));
    bytes.extend(chunk(b"IDAT", z));
    bytes.extend(chunk(b"IEND", &[]));
    bytes
}
fn image() -> Vec<u8> {
    png(&[0, 12, 34, 56], false)
}
fn value(request: &TextRequest) -> Value {
    match &request.payload {
        TextPayload::WebRtc(text) => serde_json::from_str(text).unwrap(),
        TextPayload::WebSocket(bytes) => {
            let Some(protocol::frame::Body::Message(message)) =
                protocol::decode(bytes).unwrap().body
            else {
                panic!()
            };
            serde_json::from_str(&message.data).unwrap()
        }
    }
}
#[test]
fn exact_session_readiness_ticket_cancel_and_close_fences() {
    let mut e = ImageEntry::new(7);
    assert!(e.begin(100, "image/png", Transport::WebSocket).is_err());
    for v in [
        json!({"id":"6","label":"rtvi-ai","type":"bot-ready","data":{"version":"2.2.0"}}),
        json!({"id":"7","label":"rtvi-ai","type":"bot-ready","data":{"version":"2.1.0"}}),
    ] {
        assert!(!e.accept_ready(&v));
    }
    let mut e = entry();
    let first = e
        .begin(image().len() as u64, "image/png", Transport::WebSocket)
        .unwrap();
    assert!(e.begin(100, "image/png", Transport::WebSocket).is_err());
    e.cancel();
    let next = e.begin(100, "image/png", Transport::WebSocket).unwrap();
    assert_ne!(first, next);
    assert!(e
        .prepare(first, &image(), "image/png", "", Transport::WebSocket, 0)
        .is_err());
    assert!(e
        .prepare(next, &image(), "image/png", "", Transport::WebSocket, 0)
        .is_ok());
    e.close();
    assert!(!e.is_current(next));
    assert!(e
        .prepare(next, &image(), "image/png", "", Transport::WebSocket, 0)
        .is_err());
}
#[test]
fn wire_preserves_original_bytes_dimensions_literal_caption_and_options() {
    for transport in [Transport::WebSocket, Transport::SmallWebRtc] {
        for (data, mime, width, height) in [
            (image(), "image/png", 1, 1),
            (
                include_bytes!("fixtures/baseline.jpg").to_vec(),
                "image/jpeg",
                17,
                13,
            ),
            (
                include_bytes!("fixtures/progressive.jpg").to_vec(),
                "image/jpeg",
                17,
                13,
            ),
            (
                include_bytes!("fixtures/subsampled.jpg").to_vec(),
                "image/jpeg",
                17,
                13,
            ),
        ] {
            let mut e = entry();
            let ticket = e.begin(data.len() as u64, mime, transport).unwrap();
            let caption = "  中文 🦀\n\"\\\u{0001}  ";
            let request = e
                .prepare(ticket, &data, mime, caption, transport, 0)
                .unwrap();
            let v = value(&request);
            assert_eq!(v["type"], "send-file");
            assert_eq!(v["data"]["content"], caption);
            assert_eq!(v["data"]["file"]["format"], mime);
            assert_eq!(
                STANDARD
                    .decode(v["data"]["file"]["source"]["bytes"].as_str().unwrap())
                    .unwrap(),
                data
            );
            assert_eq!(v["data"]["file"]["source"]["width"], width);
            assert_eq!(v["data"]["file"]["source"]["height"], height);
            assert_eq!(
                v["data"]["options"],
                json!({"run_immediately":true,"audio_response":true})
            );
        }
    }
}
#[test]
fn raw_mime_and_dimension_budgets_reject_before_read_or_decode() {
    for (transport, limit) in [
        (Transport::WebSocket, MAX_WS_IMAGE_BYTES),
        (Transport::SmallWebRtc, MAX_RTC_IMAGE_BYTES),
    ] {
        for n in [0, limit as u64 + 1, u64::MAX] {
            assert!(entry().begin(n, "image/png", transport).is_err());
        }
        assert!(entry().begin(limit as u64, "image/png", transport).is_ok());
    }
    for mime in ["", "image/gif", "IMAGE/PNG", "image/jpg"] {
        assert!(entry().begin(10, mime, Transport::WebSocket).is_err());
    }
    assert!(entry().begin(10, "image/png", Transport::Daily).is_err());
    assert!(validate_image(&image(), "image/jpeg").is_err());
    let mut oversized = image();
    oversized[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(validate_image(&oversized, "image/png").is_err());
}
#[test]
fn exact_raw_budgets_and_complete_wire_backpressure_include_base64_and_caption_escaping() {
    for (transport, limit) in [
        (Transport::WebSocket, MAX_WS_IMAGE_BYTES),
        (Transport::SmallWebRtc, MAX_RTC_IMAGE_BYTES),
    ] {
        let base = image();
        let mut data = base[..33].to_vec();
        data.extend(chunk(b"vpAg", &vec![0; limit - base.len() - 12]));
        data.extend(&base[33..]);
        assert_eq!(data.len(), limit);
        let mut e = entry();
        let ticket = e.begin(limit as u64, "image/png", transport).unwrap();
        let request = e
            .prepare(ticket, &data, "image/png", "", transport, 0)
            .unwrap();
        let n = request.payload.len() as u64;
        assert!(n > limit as u64);
        assert!(e
            .prepare(
                ticket,
                &data,
                "image/png",
                "",
                transport,
                MAX_BUFFERED_BYTES - n
            )
            .is_ok());
        assert!(e
            .prepare(
                ticket,
                &data,
                "image/png",
                "",
                transport,
                MAX_BUFFERED_BYTES - n + 1
            )
            .is_err());
        assert!(e
            .prepare(ticket, &data, "image/png", "", transport, u64::MAX)
            .is_err());
        assert!(e
            .prepare(
                ticket,
                &data,
                "image/png",
                &"x".repeat(MAX_TEXT_BYTES + 1),
                transport,
                0
            )
            .is_err());
        // Control-character escaping is budgeted on the final wire, not UTF-8.
        assert!(e
            .prepare(
                ticket,
                &data,
                "image/png",
                &"\u{0001}".repeat(MAX_TEXT_BYTES),
                transport,
                0
            )
            .is_err());
        if transport == Transport::SmallWebRtc {
            let room = MAX_WEBRTC_TEXT_WIRE_BYTES - request.payload.len();
            assert_eq!(
                e.prepare(ticket, &data, "image/png", &"x".repeat(room), transport, 0)
                    .unwrap()
                    .payload
                    .len(),
                MAX_WEBRTC_TEXT_WIRE_BYTES
            );
            assert!(e
                .prepare(
                    ticket,
                    &data,
                    "image/png",
                    &"x".repeat(room + 1),
                    transport,
                    0
                )
                .is_err());
        }
    }
}
#[test]
fn strict_png_checks_crc_adler_stream_tail_scanline_tail_truncation_and_animation() {
    assert_eq!(
        validate_image(&png(&[0, 1, 2, 3], true), "image/png").unwrap(),
        (1, 1)
    );
    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    z.write_all(&[0, 1, 2, 3]).unwrap();
    let compressed = z.finish().unwrap();
    let mut adler = compressed.clone();
    *adler.last_mut().unwrap() ^= 1;
    let mut trailing = compressed.clone();
    trailing.push(0);
    let mut crc = image();
    crc[32] ^= 1;
    let mut file_tail = image();
    file_tail.push(0);
    let base = image();
    let mut animation = base[..33].to_vec();
    animation.extend(chunk(b"acTL", &[0; 8]));
    animation.extend(&base[33..]);
    for bad in [
        crc,
        file_tail,
        animation,
        png_compressed(&adler, false),
        png_compressed(&trailing, false),
        png(&[0, 1, 2, 3, 0], false),
        png(&[0, 1, 2], false),
    ] {
        assert!(validate_image(&bad, "image/png").is_err());
    }
    for n in 0..image().len() {
        assert!(validate_image(&image()[..n], "image/png").is_err());
    }
}
#[test]
fn strict_jpeg_rejects_truncated_entropy_retained_eoi_and_trailing_bytes() {
    for fixture in [
        include_bytes!("fixtures/baseline.jpg").as_slice(),
        include_bytes!("fixtures/progressive.jpg").as_slice(),
        include_bytes!("fixtures/subsampled.jpg").as_slice(),
    ] {
        let mut tail = fixture.to_vec();
        tail.push(0);
        assert!(validate_image(&tail, "image/jpeg").is_err());
        let mut missing = fixture[..fixture.len() - 3].to_vec();
        missing.extend([0xff, 0xd9]);
        assert!(validate_image(&missing, "image/jpeg").is_err());
        for n in [2, fixture.len() / 2, fixture.len() - 2] {
            assert!(validate_image(&fixture[..n], "image/jpeg").is_err());
        }
    }
}

#[test]
fn shared_native_rtvi_golden_is_actual_browser_request_for_both_wires() {
    let golden: Value = serde_json::from_str(include_str!("fixtures/send-file.json")).unwrap();
    let bytes = include_bytes!("fixtures/baseline.jpg");
    for transport in [Transport::WebSocket, Transport::SmallWebRtc] {
        let mut e = entry();
        let ticket = e
            .begin(bytes.len() as u64, "image/jpeg", transport)
            .unwrap();
        let request = e
            .prepare(
                ticket,
                bytes,
                "image/jpeg",
                golden["data"]["content"].as_str().unwrap(),
                transport,
                0,
            )
            .unwrap();
        assert_eq!(value(&request), golden);
    }
}
