//! Local-only browser host: serve <dist-directory> <bot-start-url> [port].
//! Press Enter or close stdin to shut down all owned connections.
use pipecat_client_server::{
    server::{serve, AssetBundle, ServerConfig},
    ClientProxy, ProxyLimits,
};
use std::{path::Path, sync::Arc};
use tokio::net::TcpListener;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !(2..=3).contains(&args.len()) {
        return Err("usage: serve <dist-directory> <bot-start-url> [port]".into());
    }
    let port: u16 = args.get(2).map(|p| p.parse()).transpose()?.unwrap_or(8080);
    let assets = AssetBundle::from_directory(Path::new(&args[0]))?;
    let token = std::env::var("BOT_START_PUBLIC_API_KEY").ok();
    let proxy = ClientProxy::new(&args[1], token.as_deref(), ProxyLimits::default())?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let origin = format!("http://{address}");
    println!("Native Rust browser host: {origin}\nLocal-only; press Enter to stop.");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let _ = stop_tx.send(());
    });
    serve(
        listener,
        proxy,
        ServerConfig {
            assets: Some(Arc::new(assets)),
            allowed_origins: vec![origin],
            ..Default::default()
        },
        async {
            let _ = stop_rx.await;
        },
    )
    .await?;
    Ok(())
}
