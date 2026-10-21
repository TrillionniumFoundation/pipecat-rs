use super::*;
use futures_util::StreamExt;
use livekit::webrtc::{
    video_frame::VideoBuffer,
    video_stream::native::{NativeVideoStream, NativeVideoStreamOptions},
};
use pipecat_core::ImageRawFrame;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;

fn config(width: u32, height: u32) -> VideoOutputConfig {
    VideoOutputConfig {
        width,
        height,
        ..Default::default()
    }
}
fn image(width: u32, height: u32, format: &str, bytes: Vec<u8>) -> OutputImageRawFrame {
    OutputImageRawFrame {
        image: ImageRawFrame {
            image: bytes,
            size: (width, height),
            format: Some(format.into()),
        },
        sync_with_audio: false,
    }
}
fn planes(frame: &VideoFrame<I420Buffer>) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let (y, u, v) = frame.buffer.data();
    (y.to_vec(), u.to_vec(), v.to_vec())
}
#[test]
fn video_configuration_is_disabled_and_finitely_bounded() {
    assert!(crate::LiveKitConfig::default().video_output.is_none());
    for (w, h) in [(0, 1), (1, 0), (u32::MAX, 2), (2, u32::MAX), (4096, 4096)] {
        assert!(config(w, h).validate().is_err());
    }
    let mut c = config(3, 3);
    assert!(c.validate().is_ok());
    for capacity in [0, 5, usize::MAX] {
        c.queue_capacity = capacity;
        assert!(c.validate().is_err());
    }
    c = config(3, 3);
    c.max_retained_bytes = 1;
    assert!(c.validate().is_err());
    c = config(3, 3);
    c.max_frame_bytes = usize::MAX;
    assert!(c.validate().is_err());
    c = config(3, 3);
    c.max_retained_bytes = usize::MAX;
    assert!(c.validate().is_err());
    c = config(3, 3);
    c.max_frame_bytes = 16;
    assert!(c.validate().is_err()); // I420 needs 17
}
#[test]
fn video_exact_formats_sizes_and_unsupported_policies_rejected_before_native() {
    let c = config(3, 3);
    for (format, size) in [
        ("RGB", 27),
        ("RGBA", 36),
        ("BGRA", 36),
        ("ARGB", 36),
        ("I420", 17),
    ] {
        let good = image(3, 3, format, vec![0; size]);
        assert!(c.validate_image(&good, None).is_ok());
        for length in [size - 1, size + 1] {
            assert!(c
                .validate_image(&image(3, 3, format, vec![0; length]), None)
                .is_err());
        }
    }
    let mut output = image(3, 3, "RGB", vec![0; 27]);
    for format in [None, Some("rgb"), Some("JPEG"), Some("")] {
        output.image.format = format.map(str::to_owned);
        assert!(c.validate_image(&output, None).is_err());
    }
    output.image.format = Some("RGB".into());
    output.sync_with_audio = true;
    assert!(c.validate_image(&output, None).is_err());
    output.sync_with_audio = false;
    assert!(c.validate_image(&output, Some("")).is_err());
    output.image.size = (1, 9);
    assert!(c.validate_image(&output, None).is_err());
    output.image.size = (3, 3);
    let mut tight = c;
    tight.max_frame_bytes = 27;
    output.image.image = Vec::with_capacity(100);
    output.image.image.extend([0; 27]);
    assert!(tight.validate_image(&output, None).is_err());
}
#[test]
fn both_core_image_views_borrow_pixels_and_preserve_explicit_sync_policy() {
    for detailed in [false, true] {
        let output = image(3, 3, "I420", (1..=17).collect());
        let original_pixels = output.image.image.as_ptr();
        let mut data = if detailed {
            FrameData::OutputImageDetailed(output)
        } else {
            FrameData::OutputImage(output.image)
        };
        let input = video_input(&data).unwrap();
        assert_eq!(input.image.image.as_ptr(), original_pixels);
        assert!(!input.sync_with_audio);
        let frame = to_sdk_video(input, &config(3, 3)).unwrap();
        assert_eq!(
            planes(&frame),
            (
                (1..=9).collect(),
                vec![10, 11, 12, 13],
                vec![14, 15, 16, 17]
            )
        );
        if let FrameData::OutputImageDetailed(output) = &mut data {
            output.sync_with_audio = true;
            let input = video_input(&data).unwrap();
            assert!(input.sync_with_audio);
            assert!(config(3, 3).validate_image(input, None).is_err());
            assert!(to_sdk_video(input, &config(3, 3)).is_err());
        }
    }
}
/// Independent integer oracle for uniform-color blocks, not adapter output.
/// Pinned libyuv 1941 ABGRToYRow_C/ABGRToUVRow_C uses rounded luma bias 4224
/// but chroma bias 32768 (no extra 128). Uniform blocks make subsampling exact.
/// Archive row_common.o SHA256:
/// 69fbfc4a16dccc60def6f9caaa64dbceeeb3ae192c07d7950b9e6aa56914fee8.
fn reference_bt601_uniform([r, g, b]: [u8; 3]) -> [u8; 3] {
    let (r, g, b) = (i32::from(r), i32::from(g), i32::from(b));
    [
        ((66 * r + 129 * g + 25 * b + 4224) >> 8) as u8,
        ((-38 * r - 74 * g + 112 * b + 32768) >> 8) as u8,
        ((112 * r - 94 * g - 18 * b + 32768) >> 8) as u8,
    ]
}
#[test]
fn native_video_packed_orders_match_independent_integer_oracle_on_odd_blocks() {
    let c = config(3, 3);
    for (rgb, anchor) in [
        ([255, 0, 0], [82, 90, 239]),
        ([0, 0, 255], [41, 239, 110]),
        ([0, 255, 0], [144, 54, 34]),
        ([0, 0, 0], [16, 128, 128]),
        ([255, 255, 255], [235, 128, 128]),
        ([17, 83, 201], [82, 189, 90]),
    ] {
        let [r, g, b] = rgb;
        let expected = reference_bt601_uniform(rgb);
        assert_eq!(expected, anchor, "independent fixed-point anchor");
        for format in ["RGB", "RGBA", "BGRA", "ARGB"] {
            for alpha in [0, 255] {
                let pixel = match format {
                    "RGB" => vec![r, g, b],
                    "RGBA" => vec![r, g, b, alpha],
                    "BGRA" => vec![b, g, r, alpha],
                    "ARGB" => vec![alpha, r, g, b],
                    _ => unreachable!(),
                };
                let frame = to_sdk_video(&image(3, 3, format, pixel.repeat(9)), &c).unwrap();
                let (y, u, v) = planes(&frame);
                assert_eq!(y, [expected[0]; 9], "luma {format} {rgb:?} alpha={alpha}");
                assert_eq!(u, [expected[1]; 4], "U {format} {rgb:?} alpha={alpha}");
                assert_eq!(v, [expected[2]; 4], "V {format} {rgb:?} alpha={alpha}");
            }
        }
    }
}
#[test]
fn native_video_color_orders_ignore_alpha_and_match_asymmetric_bt601_fixture() {
    let c = config(4, 2);
    // Two uniform 2x2 blocks, red then blue: chroma must not be swapped.
    let pixels = [[255, 0, 0], [255, 0, 0], [0, 0, 255], [0, 0, 255]].repeat(2);
    let mut expected = None;
    for format in ["RGB", "RGBA", "BGRA", "ARGB"] {
        let bytes = pixels
            .iter()
            .flat_map(|&[r, g, b]| match format {
                "RGB" => vec![r, g, b],
                "RGBA" => vec![r, g, b, 0],
                "BGRA" => vec![b, g, r, 17],
                "ARGB" => vec![99, r, g, b],
                _ => unreachable!(),
            })
            .collect();
        let frame = to_sdk_video(&image(4, 2, format, bytes), &c).unwrap();
        assert_eq!(frame.timestamp_us, 0);
        assert_eq!(frame.rotation, VideoRotation::VideoRotation0);
        let actual = planes(&frame);
        assert_eq!(actual.0, [82, 82, 41, 41, 82, 82, 41, 41]);
        let red = reference_bt601_uniform([255, 0, 0]);
        let blue = reference_bt601_uniform([0, 0, 255]);
        assert_eq!(actual.1, [red[1], blue[1]]);
        assert_eq!(actual.2, [red[2], blue[2]]);
        if let Some(previous) = &expected {
            assert_eq!(&actual, previous);
        }
        expected = Some(actual);
    }
}
#[test]
fn native_video_odd_i420_owned_planes_are_exact_and_initialized() {
    let c = config(3, 3);
    let mut output = image(3, 3, "I420", (1..=17).collect());
    let frame = to_sdk_video(&output, &c).unwrap();
    output.image.image.fill(255);
    drop(output);
    let (y, u, v) = planes(&frame);
    assert_eq!(y, (1..=9).collect::<Vec<_>>());
    assert_eq!(u, [10, 11, 12, 13]);
    assert_eq!(v, [14, 15, 16, 17]);
    assert_eq!(frame.buffer.strides(), (3, 2, 2));
    assert_eq!((frame.buffer.width(), frame.buffer.height()), (3, 3));
    let black = I420Buffer::new_black(3, 3);
    let (y, u, v) = black.data();
    assert_eq!(y, [0; 9]);
    assert_eq!(u, [128; 4]);
    assert_eq!(v, [128; 4]);
    let odd_rgb = to_sdk_video(&image(3, 3, "RGB", [255, 0, 0].repeat(9)), &c).unwrap();
    let (y, u, v) = planes(&odd_rgb);
    assert_eq!(y, [82; 9]);
    assert_eq!(u, [90; 4]);
    assert_eq!(v, [reference_bt601_uniform([255, 0, 0])[2]; 4]);
}
struct MockSink {
    captures: Arc<AtomicUsize>,
    forward: bool,
}
impl VideoSink for MockSink {
    fn capture(&self, _: &VideoFrame<I420Buffer>) -> bool {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.forward
    }
}
#[test]
fn video_gate_adaptation_drop_is_not_network_error_and_terminal_gate_rejects() {
    let captures = Arc::new(AtomicUsize::new(0));
    let mut gate = Gate {
        sink: Some(MockSink {
            captures: captures.clone(),
            forward: false,
        }),
        generation: 2,
    };
    let output = image(1, 1, "I420", vec![80, 90, 240]);
    let c = config(1, 1);
    let cancel = CancellationToken::new();
    assert!(matches!(
        gate.capture(&output, &c, Some(1), &cancel),
        Err(Error::Interrupted)
    ));
    assert_eq!(captures.load(Ordering::SeqCst), 0);
    assert_eq!(
        gate.capture(&output, &c, Some(2), &cancel).unwrap(),
        VideoCaptureOutcome::AdaptationDropped
    );
    gate.sink.as_mut().unwrap().forward = true;
    assert_eq!(
        gate.capture(&output, &c, None, &cancel).unwrap(),
        VideoCaptureOutcome::ForwardedToSource
    );
    cancel.cancel();
    assert!(matches!(
        gate.capture(&output, &c, None, &cancel),
        Err(Error::Closed)
    ));
    gate.sink.take();
    gate.sink.take();
    assert!(matches!(
        gate.capture(&output, &c, None, &CancellationToken::new()),
        Err(Error::Closed)
    ));
    assert_eq!(captures.load(Ordering::SeqCst), 2);
}

/// Actual native source -> local SDK track -> bounded native sink, no SFU/device.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_video_source_track_stream_owns_rgb_and_i420_frames() {
    let c = config(8, 4);
    let camera = NativeCamera::new(&c).unwrap();
    let mut stream = NativeVideoStream::with_options(
        camera.track.rtc_track(),
        NativeVideoStreamOptions {
            queue_size_frames: Some(2),
        },
    );
    for format in ["RGB", "RGBA", "BGRA", "ARGB", "I420"] {
        let bytes = match format {
            "RGB" => [255, 0, 0].repeat(32),
            "RGBA" => [255, 0, 0, 0].repeat(32),
            "BGRA" => [0, 0, 255, 17].repeat(32),
            "ARGB" => [99, 255, 0, 0].repeat(32),
            "I420" => [vec![82; 32], vec![90; 8], vec![240; 8]].concat(),
            _ => unreachable!(),
        };
        let mut output = image(8, 4, format, bytes);
        let frame = to_sdk_video(&output, &c).unwrap();
        output.image.image.fill(0);
        drop(output);
        assert!(camera.source.capture_frame(&frame));
        drop(frame);
        // Initial SDK black keepalive may race first capture; inspect content, not position.
        let received = timeout(Duration::from_secs(2), async {
            loop {
                let frame = stream.next().await.expect("native stream ended");
                let buffer = frame.buffer.as_i420().expect("I420 source preserved");
                if buffer.data().0.iter().any(|byte| *byte != 0) {
                    break frame;
                }
            }
        })
        .await
        .expect("native capture deadline");
        assert_eq!(received.buffer.width(), 8);
        assert_eq!(received.buffer.height(), 4);
        assert_eq!(received.rotation, VideoRotation::VideoRotation0);
        assert!(received.timestamp_us > 0); // SDK capture-time/TimestampAligner, not pipeline PTS
        let (y, u, v) = received.buffer.as_i420().unwrap().data();
        assert_eq!(y, [82; 32]);
        assert_eq!(u, [90; 8]);
        // Packed RGB is converted with libyuv integer chroma; raw I420 must
        // preserve the explicitly supplied 240 byte, not normalize it to 239.
        let expected_v = if format == "I420" {
            240
        } else {
            reference_bt601_uniform([255, 0, 0])[2]
        };
        assert_eq!(v, [expected_v; 8]);
    }
    stream.close();
    stream.close();
    assert!(timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn native_video_bounded_stream_retains_latest_and_close_clears() {
    let c = config(8, 4);
    let camera = NativeCamera::new(&c).unwrap();
    let mut stream = NativeVideoStream::with_options(
        camera.track.rtc_track(),
        NativeVideoStreamOptions {
            queue_size_frames: Some(2),
        },
    );
    for value in 20..30 {
        let frame = to_sdk_video(
            &image(
                8,
                4,
                "I420",
                [vec![value; 32], vec![90; 8], vec![240; 8]].concat(),
            ),
            &c,
        )
        .unwrap();
        assert!(camera.source.capture_frame(&frame));
    }
    // Current-thread runtime: no await since source construction, so keepalive cannot
    // pass its initial check before these captures. It observes captured_frames > 0.
    let first = timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.buffer.as_i420().unwrap().data().0, [28; 32]);
    assert_eq!(second.buffer.as_i420().unwrap().data().0, [29; 32]);
    stream.close();
    assert!(stream.next().await.is_none());
    let mut cleared = NativeVideoStream::with_options(
        camera.track.rtc_track(),
        NativeVideoStreamOptions {
            queue_size_frames: Some(2),
        },
    );
    let frame = to_sdk_video(&image(8, 4, "I420", vec![80; 48]), &c).unwrap();
    assert!(camera.source.capture_frame(&frame));
    cleared.close();
    assert!(cleared.next().await.is_none());
    drop(cleared);
    drop(stream);
    drop(camera);
}
#[tokio::test]
async fn video_publisher_clones_share_terminal_release() {
    let c = config(4, 2);
    let publisher = VideoPublisher::new(NativeCamera::new(&c).unwrap());
    let clone = publisher.clone();
    let output = image(4, 2, "I420", vec![80; 12]);
    let cancel = CancellationToken::new();
    publisher.interrupt();
    assert_eq!(clone.generation(), 1);
    assert!(matches!(
        clone.capture(&output, &c, Some(0), &cancel),
        Err(Error::Interrupted)
    ));
    publisher.close();
    clone.close();
    assert!(matches!(
        clone.capture(&output, &c, None, &cancel),
        Err(Error::Closed)
    ));
    assert!(publisher.0.lock().unwrap().sink.is_none());
}
