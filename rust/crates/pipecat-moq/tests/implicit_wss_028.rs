use tokio::io::AsyncReadExt;

#[tokio::test]
async fn implicit_wss_provider_remains_unambiguous_with_moq() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let (client, mut peer) = tokio::io::duplex(4096);
    let reader = tokio::spawn(async move {
        let mut prefix = [0u8; 5];
        peer.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix[0], 22, "expected a TLS handshake record");
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio_tungstenite_028::client_async_tls_with_config(
            "wss://localhost/moq-feature-regression",
            client,
            None,
            None,
        ),
    )
    .await
    .expect("TLS handshake did not observe local EOF");
    assert!(result.is_err(), "the synthetic peer never completes TLS");
    reader.await.unwrap();
}
