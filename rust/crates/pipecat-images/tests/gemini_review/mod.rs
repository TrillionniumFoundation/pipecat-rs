use super::*;

#[tokio::test]
async fn exhausted_aggregate_budget_rejects_before_another_billable_submission() {
    for kind in 0..3 {
        let pixels = png(png::ColorType::Rgb);
        let reply = inline(&pixels);
        let response_bytes = reply.body.len();
        let (url, mut requests, peer) = server(vec![reply.clone(), reply]).await;
        let mut settings = config(url);
        settings.settings.number_of_images = 2;
        match kind {
            0 => settings.limits.max_total_response_bytes = response_bytes,
            1 => settings.limits.max_total_encoded_bytes = pixels.len(),
            2 => settings.limits.max_total_output_bytes = 6,
            _ => unreachable!(),
        }
        let client = GeminiImageClient::new(settings).unwrap();
        let mut sequence = client
            .generate("bounded", &CancellationToken::new())
            .unwrap();
        let first = sequence.next_image().await.unwrap().unwrap();
        assert_eq!(first.image.image, [255, 0, 0, 3, 4, 5]);
        assert!(requests.recv().await.is_some());
        assert_eq!(sequence.next_image().await, Err(ImageError::TooLarge));
        assert_eq!(sequence.delivered(), 1);
        assert_eq!(sequence.next_image().await, Ok(None));
        assert!(
            tokio::time::timeout(Duration::from_millis(75), requests.recv())
                .await
                .is_err(),
            "budget {kind} submitted another billable generation"
        );
        peer.abort();
        let _ = peer.await;
    }
}

#[tokio::test]
async fn failed_sequence_releases_admission_without_retry_or_new_client() {
    let (url, mut requests, peer) = server(vec![
        Reply::json(503, json!({"error":"do not replay"})),
        inline(&png(png::ColorType::Rgb)),
    ])
    .await;
    let mut settings = config(url);
    settings.limits.image.max_pending = 1;
    let client = GeminiImageClient::new(settings).unwrap();
    let mut failed = client.generate("first", &CancellationToken::new()).unwrap();
    assert_eq!(failed.next_image().await, Err(ImageError::Http(503)));
    assert_eq!(failed.next_image().await, Ok(None));
    assert_eq!(failed.delivered(), 0);
    let first = requests.recv().await.unwrap();
    assert!(std::str::from_utf8(&first.body).unwrap().contains("first"));
    let mut next = client
        .generate("new intent", &CancellationToken::new())
        .unwrap();
    assert!(next.next_image().await.unwrap().is_some());
    let second = requests.recv().await.unwrap();
    assert!(std::str::from_utf8(&second.body)
        .unwrap()
        .contains("new intent"));
    assert_eq!(next.next_image().await, Ok(None));
    peer.await.unwrap();
    assert!(requests.recv().await.is_none());
}
