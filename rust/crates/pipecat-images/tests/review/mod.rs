//! Independent adversarial acceptance checks; only literal-loopback HTTP fixtures.
use super::*;

#[tokio::test]
async fn ambiguous_post_and_server_failure_are_never_replayed() {
    for status in [None, Some(503)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert_eq!(request.method, "POST");
            assert!(request.headers.contains_key("authorization"));
            if let Some(status) = status {
                write_reply(
                    &mut stream,
                    Reply::json(status, json!({"error":"private"})),
                    false,
                )
                .await;
            }
            drop(stream);
            assert!(
                tokio::time::timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err(),
                "billable request replayed"
            );
        });
        let error = OpenAiImageClient::new(config(origin))
            .unwrap()
            .generate("charge once", &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(
            error,
            status.map_or(ImageError::Transport, ImageError::Http)
        );
        fixture.await.unwrap();
    }
}

#[tokio::test]
async fn response_entry_limit_is_exact_and_does_not_accept_later_images() {
    for count in [16, 17] {
        let mut entries = vec![
            json!({"b64_json": base64::engine::general_purpose::STANDARD.encode(png(png::ColorType::Rgb))}),
        ];
        entries.extend((1..count).map(|_| json!({"ignored":true})));
        let (url, _, fixture) = server(vec![Reply::json(200, json!({"data":entries}))]).await;
        let result = OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await;
        if count == 16 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(ImageError::Protocol));
        }
        fixture.await.unwrap();
    }
    let (url, _, fixture) = server(vec![Reply::json(200, json!({"data":[{}, {"b64_json":base64::engine::general_purpose::STANDARD.encode(png(png::ColorType::Rgb))}]}))]).await;
    assert_eq!(
        OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await,
        Err(ImageError::NoImage)
    );
    fixture.await.unwrap();
}

#[tokio::test]
async fn invalid_settings_prompt_and_cancel_do_not_make_http_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config(format!("http://{}", listener.local_addr().unwrap()));
    c.limits.max_prompt_bytes = 3;
    let client = OpenAiImageClient::new(c).unwrap();
    assert_eq!(
        client.generate("four", &CancellationToken::new()).await,
        Err(ImageError::TooLarge)
    );
    let invalid = ImageGenerationSettings {
        model: Some("x".repeat(1025)),
        image_size: None,
    };
    assert!(matches!(
        client
            .generate_with_settings("x", &invalid, &CancellationToken::new())
            .await,
        Err(ImageError::Invalid(_))
    ));
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        client.generate("x", &token).await,
        Err(ImageError::Cancelled)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn strict_base64_rejects_padding_trailing_bits_and_whitespace() {
    for b64 in ["AAAA=", "AA=A", "AB==", "AAB=", "AAAA\n", "===="] {
        let (url, _, fixture) =
            server(vec![Reply::json(200, json!({"data":[{"b64_json":b64}]}))]).await;
        assert_eq!(
            OpenAiImageClient::new(config(url))
                .unwrap()
                .generate("x", &CancellationToken::new())
                .await,
            Err(ImageError::Protocol),
            "{b64:?}"
        );
        fixture.await.unwrap();
    }
}

#[tokio::test]
async fn png_palette_transparency_and_sixteen_bit_samples_reach_rgb() {
    let mut indexed = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut indexed, 2, 1);
        encoder.set_color(png::ColorType::Indexed);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_palette(vec![23, 47, 89, 123, 145, 167]);
        encoder.set_trns(vec![0, 127]);
        encoder
            .write_header()
            .unwrap()
            .write_image_data(&[0, 1])
            .unwrap();
    }
    let mut sixteen = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut sixteen, 1, 1);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Sixteen);
        encoder
            .write_header()
            .unwrap()
            .write_image_data(&[0x12, 0xff, 0x56, 0xff, 0xab, 0xff, 0, 0])
            .unwrap();
    }
    for (bytes, expected) in [
        (indexed, vec![23, 47, 89, 123, 145, 167]),
        (sixteen, vec![0x12, 0x56, 0xab]),
    ] {
        let (url, _, fixture) = server(vec![inline(&bytes)]).await;
        let image = OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(image.image.image, expected);
        fixture.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_trailing_and_crc_corruption_fail_without_poisoning_client() {
    let original = png(png::ColorType::Rgb);
    let jpeg = include_bytes!("../fixtures/source-cat.jpg");
    let mut crc_bad = original.clone();
    crc_bad[29] ^= 1;
    let mut trailing_png = original.clone();
    trailing_png.push(0);
    let mut trailing_jpeg = jpeg.to_vec();
    trailing_jpeg.push(0);
    let truncated_jpeg = jpeg[..jpeg.len() - 2].to_vec();
    let mut replies: Vec<_> = [crc_bad, trailing_png, trailing_jpeg, truncated_jpeg]
        .iter()
        .map(|v| inline(v))
        .collect();
    replies.push(inline(&original));
    let (url, _, fixture) = server(replies).await;
    let client = OpenAiImageClient::new(config(url)).unwrap();
    for _ in 0..4 {
        assert_eq!(
            client.generate("x", &CancellationToken::new()).await,
            Err(ImageError::Decode)
        );
    }
    assert!(client
        .generate("x", &CancellationToken::new())
        .await
        .is_ok());
    fixture.await.unwrap();
}

#[tokio::test]
async fn hosted_streaming_limit_and_origin_port_are_enforced() {
    let bytes = png(png::ColorType::Rgb);
    let (host, _, download) = server(vec![Reply {
        body: bytes,
        chunked: true,
        ..Reply::json(200, json!(null))
    }])
    .await;
    let (api, _, fixture) = server(vec![Reply::json(
        200,
        json!({"data":[{"url":format!("{host}/image")}]}),
    )])
    .await;
    let mut c = config(api);
    c.hosted_images = HostedImagePolicy::loopback_fixtures(&[&host]).unwrap();
    c.limits.max_encoded_image_bytes = 8;
    assert_eq!(
        OpenAiImageClient::new(c)
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await,
        Err(ImageError::TooLarge)
    );
    fixture.await.unwrap();
    download.await.unwrap();
    let forbidden = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let other = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (api, _, fixture) = server(vec![Reply::json(
        200,
        json!({"data":[{"url":format!("http://{}/x", forbidden.local_addr().unwrap())}]}),
    )])
    .await;
    let mut c = config(api);
    c.hosted_images =
        HostedImagePolicy::loopback_fixtures(&[&format!("http://{}", other.local_addr().unwrap())])
            .unwrap();
    assert_eq!(
        OpenAiImageClient::new(c)
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await,
        Err(ImageError::AddressDenied)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(40), forbidden.accept())
            .await
            .is_err()
    );
    fixture.await.unwrap();
}

#[tokio::test]
async fn permanent_failure_recovers_only_after_changed_settings() {
    let (url, _, fixture) = server(vec![
        Reply::json(401, json!({"error":"private"})),
        inline(&png(png::ColorType::Rgb)),
    ])
    .await;
    let processor = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(url)).unwrap(),
    ));
    let health = processor.health().unwrap();
    let pipeline = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline.queue(Frame::text("bad credential")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while health.is_usable() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    processor
        .update_settings(&ImageGenerationSettingsDelta::default())
        .unwrap();
    assert!(!health.is_usable());
    processor
        .update_settings(&ImageGenerationSettingsDelta {
            model: Setting::Value("reconfigured".into()),
            image_size: Setting::Null,
        })
        .unwrap();
    assert!(health.is_usable());
    pipeline.queue(Frame::text("recovered")).await.unwrap();
    pipeline.end().await.unwrap();
    let (events, report) = pipeline.collect().await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.frame.data, FrameData::UrlImage(_)))
            .count(),
        1
    );
    assert!(processor
        .update_settings(&ImageGenerationSettingsDelta::default())
        .is_err());
    fixture.await.unwrap();
}

#[tokio::test]
async fn noninterruptible_image_survives_interruption_and_metrics_are_real() {
    let (url, mut requests, fixture) = server(vec![Reply {
        delay: Duration::from_millis(60),
        ..inline(&png(png::ColorType::Rgb))
    }])
    .await;
    let processor = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(url)).unwrap(),
    ));
    let sink = Arc::new(Pixels::default());
    let mut cfg = PipelineConfig::default();
    cfg.start.enable_metrics = true;
    let pipeline = Pipeline::new(vec![processor, output(sink.clone(), 1024 * 1024)])
        .start(cfg)
        .await
        .unwrap();
    let mut frame = Frame::text("survive");
    frame.interruptible = false;
    pipeline.queue(frame).await.unwrap();
    requests.recv().await.unwrap();
    pipeline.queue(Frame::interruption()).await.unwrap();
    pipeline.end().await.unwrap();
    let (events, report) = pipeline.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    assert!(events.iter().any(|e| match &e.frame.data {
        FrameData::Metrics(metrics) => metrics
            .iter()
            .any(|m| matches!(m, Metric::Processing { seconds, .. } if *seconds >= 0.04)),
        _ => false,
    }));
    fixture.await.unwrap();
}

#[tokio::test]
async fn malformed_targeted_settings_are_atomic_and_do_not_disable_service() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let processor = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(format!("http://{}", listener.local_addr().unwrap())))
            .unwrap(),
    ));
    let pipeline = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut nested = Value::Null;
    for _ in 0..150 {
        nested = json!([nested]);
    }
    for (index, invalid) in [json!("x".repeat(129)), nested, json!(false)]
        .into_iter()
        .enumerate()
    {
        pipeline
            .queue(Frame::new(FrameData::ServiceUpdateSettings(Box::new(
                ServiceUpdateSettingsFrame {
                    settings: BTreeMap::from([
                        ("model".into(), json!("must-not-apply")),
                        ("image_size".into(), invalid),
                    ]),
                    service: Some(ProcessorReference::Name("OpenAIImageGenService".into())),
                    ..Default::default()
                },
            ))))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while pipeline.total_errors() <= index as u64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            processor.settings().unwrap(),
            ImageGenerationSettings::default()
        );
        assert!(processor.health().unwrap().is_usable());
    }
    pipeline
        .queue(Frame::new(FrameData::ServiceUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                settings: BTreeMap::from([
                    ("model".into(), Value::Null),
                    ("image_size".into(), json!("future-shape")),
                ]),
                service: Some(ProcessorReference::Name("OpenAIImageGenService".into())),
                ..Default::default()
            },
        ))))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while processor.settings().unwrap().model.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        processor.settings().unwrap(),
        ImageGenerationSettings {
            model: None,
            image_size: Some("future-shape".into())
        }
    );
    pipeline.end().await.unwrap();
    pipeline.collect().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
}
