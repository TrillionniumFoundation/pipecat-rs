use base64::Engine;
use pipecat_images::{ImageError, OfflineRgbDecodeLimits, OfflineRgbDecoder};

fn png(color: png::ColorType, pixels: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
        encoder.set_color(color);
        encoder.set_depth(png::BitDepth::Eight);
        if color == png::ColorType::Indexed {
            encoder.set_palette(vec![23, 47, 89, 123, 145, 167]);
            encoder.set_trns(vec![0, 127]);
        }
        encoder
            .write_header()
            .unwrap()
            .write_image_data(pixels)
            .unwrap();
    }
    bytes
}

#[tokio::test]
async fn public_offline_owner_decodes_native_rgb_palette_grayscale_and_alpha() {
    let decoder = OfflineRgbDecoder::new(Default::default()).unwrap();
    for (color, input, expected) in [
        (
            png::ColorType::Rgb,
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
        ),
        (
            png::ColorType::Rgba,
            vec![1, 2, 3, 0, 4, 5, 6, 127],
            vec![1, 2, 3, 4, 5, 6],
        ),
        (
            png::ColorType::Grayscale,
            vec![23, 47],
            vec![23, 23, 23, 47, 47, 47],
        ),
        (
            png::ColorType::GrayscaleAlpha,
            vec![23, 0, 47, 127],
            vec![23, 23, 23, 47, 47, 47],
        ),
        (
            png::ColorType::Indexed,
            vec![0, 1],
            vec![23, 47, 89, 123, 145, 167],
        ),
    ] {
        let bytes = png(color, &input);
        assert_eq!(
            OfflineRgbDecodeLimits::default().inspect_encoded_image(&bytes, "image/png"),
            Ok((2, 1))
        );
        let output = decoder.submit(bytes).unwrap().await.unwrap();
        assert_eq!(output.image, expected);
        assert_eq!(output.image.capacity(), 6);
        assert_eq!(output.size, (2, 1));
        assert_eq!(output.format.as_deref(), Some("RGB"));
    }
    decoder.shutdown().await.unwrap();
    assert!(decoder.status().joined);
}

#[tokio::test]
async fn existing_synthetic_jpeg_fixture_uses_the_same_bounded_decoder() {
    // Same constant 2x1 grayscale fixture already qualified by azure_http.rs.
    let bytes = base64::engine::general_purpose::STANDARD
        .decode("/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/wAALCAABAAIBAREA/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/9oACAEBAAA/APNq/9k=")
        .unwrap();
    let limits = OfflineRgbDecodeLimits::default();
    let dimensions = limits.inspect_encoded_image(&bytes, "image/jpeg").unwrap();
    let decoder = OfflineRgbDecoder::new(limits).unwrap();
    let output = decoder.submit(bytes).unwrap().await.unwrap();
    assert_eq!(output.size, dimensions);
    assert_eq!(dimensions, (2, 1));
    assert_eq!(output.image, [55; 6]);
    assert_eq!(
        output.image.len(),
        dimensions.0 as usize * dimensions.1 as usize * 3
    );
    assert_eq!(output.image.len(), output.image.capacity());
    assert_eq!(output.format.as_deref(), Some("RGB"));
    decoder.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_truncated_trailing_apng_and_huge_images_never_yield_pixels() {
    let valid = png(png::ColorType::Rgb, &[1, 2, 3, 4, 5, 6]);
    let mut trailing = valid.clone();
    trailing.push(0);
    let mut apng = valid.clone();
    // A forbidden animation-control chunk is rejected before codec/CRC work.
    apng.splice(33..33, [0, 0, 0, 0, b'a', b'c', b'T', b'L', 0, 0, 0, 0]);
    let mut huge = valid.clone();
    huge[16..20].copy_from_slice(&1025u32.to_be_bytes());
    let mut bad_crc = valid.clone();
    bad_crc[29] ^= 1;
    let cases = [
        (b"GIF89a".to_vec(), ImageError::Decode),
        (b"RIFFnotwebp".to_vec(), ImageError::Decode),
        (valid[..valid.len() - 1].to_vec(), ImageError::Decode),
        (vec![0xff, 0xd8, 0xff, 0xd9], ImageError::Decode),
        (trailing, ImageError::Decode),
        (apng, ImageError::Decode),
        (huge, ImageError::TooLarge),
        (bad_crc, ImageError::Decode),
    ];
    let decoder = OfflineRgbDecoder::new(Default::default()).unwrap();
    for (bytes, error) in cases {
        assert_eq!(decoder.submit(bytes).unwrap().await, Err(error));
    }
    decoder.shutdown().await.unwrap();
}

#[test]
fn borrowed_preflight_is_strict_about_mime_but_not_full_codec_certification() {
    let limits = OfflineRgbDecodeLimits::default();
    let mut bytes = png(png::ColorType::Rgb, &[1, 2, 3, 4, 5, 6]);
    for mime in [
        "image/jpeg",
        "image/gif",
        "IMAGE/PNG",
        "image/png; charset=utf8",
        "",
    ] {
        assert_eq!(
            limits.validate_encoded_image(&bytes, mime),
            Err(ImageError::Decode)
        );
    }
    assert_eq!(
        limits.validate_encoded_image(&[], "image/png"),
        Err(ImageError::Decode)
    );
    bytes[29] ^= 1; // Header CRC intentionally left to actual decode.
    assert_eq!(
        limits.inspect_encoded_image(&bytes, "image/png"),
        Ok((2, 1))
    );
}

#[tokio::test]
async fn constructor_and_owned_capacity_cannot_bypass_resource_caps() {
    for limits in [
        OfflineRgbDecodeLimits {
            max_jobs: 0,
            ..Default::default()
        },
        OfflineRgbDecodeLimits {
            max_jobs: 3,
            ..Default::default()
        },
        OfflineRgbDecodeLimits {
            max_encoded_image_bytes: 65537,
            ..Default::default()
        },
        OfflineRgbDecodeLimits {
            max_pixels: 262145,
            ..Default::default()
        },
        OfflineRgbDecodeLimits {
            max_dimension: 1025,
            ..Default::default()
        },
        OfflineRgbDecodeLimits {
            max_decode_bytes: 32 * 1024 * 1024 + 1,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            OfflineRgbDecoder::new(limits),
            Err(ImageError::Invalid(_))
        ));
    }
    let decoder = OfflineRgbDecoder::new(Default::default()).unwrap();
    let mut bytes = Vec::with_capacity(65537);
    bytes.extend_from_slice(&png(png::ColorType::Rgb, &[1, 2, 3, 4, 5, 6]));
    assert!(matches!(decoder.submit(bytes), Err(ImageError::TooLarge)));
    assert!(matches!(
        decoder.submit(Vec::new()),
        Err(ImageError::Decode)
    ));
    assert_eq!(decoder.status().outstanding_jobs, 0);
    decoder.shutdown().await.unwrap();
}
