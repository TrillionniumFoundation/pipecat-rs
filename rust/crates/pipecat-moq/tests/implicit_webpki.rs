#[test]
fn implicit_webpki_provider_remains_unambiguous_with_moq() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.cert.der().clone()).unwrap();
    let roots = std::sync::Arc::new(roots);
    rustls::client::WebPkiServerVerifier::builder(roots.clone())
        .build()
        .unwrap();
    rustls::server::WebPkiClientVerifier::builder(roots)
        .build()
        .unwrap();
}
