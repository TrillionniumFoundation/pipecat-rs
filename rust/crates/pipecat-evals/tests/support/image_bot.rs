//! Compiled synthetic image bot; never contacts a non-loopback provider.
mod image_app;
use image_app::{ImageBot, InputPolicy};
use pipecat_evals::{
    bot_host::{EvalBotHost, EvalBotHostConfig},
    Cancellation, ImageInputLimits,
};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .ok_or("fixture needs explicit loopback endpoint")?;
    let policy = match args.next().as_deref() {
        None => InputPolicy::RequestImage,
        Some("--ordinary-text") => InputPolicy::OrdinaryText,
        Some("--passive-image") => InputPolicy::PassiveImage,
        _ => return Err("unexpected fixture policy".into()),
    };
    if args.next().is_some() {
        return Err("unexpected fixture argument".into());
    }
    let mut bot = ImageBot::new(endpoint)?;
    bot.policy = policy;
    let host = EvalBotHost::bind(
        "127.0.0.1:0".parse()?,
        EvalBotHostConfig {
            host_timeout: Duration::from_secs(12),
            session_timeout: Duration::from_secs(8),
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await?
    .with_user_images(ImageInputLimits::default())?;
    println!("ws://{}", host.local_addr()?);
    let cancel = Cancellation::new();
    let service = host.serve(&bot, cancel.clone());
    tokio::pin!(service);
    let report = tokio::select! {
        result = &mut service => result?,
        signal = tokio::signal::ctrl_c() => { signal?; cancel.cancel(); service.await? }
    };
    let valid_images = bot.trace.images.lock().unwrap().iter().all(|image| {
        image.id > 0
            && image.request_id > 0
            && image.format.as_deref() == Some("RGB")
            && image.rgb.len() == image.size.0 as usize * image.size.1 as usize * 3
            && image.user_id == "synthetic-eval-user"
            && image.text.is_some()
            && image.append_to_context == Some(matches!(bot.policy, InputPolicy::RequestImage))
            && image.source.is_none()
            && image.destination.is_none()
            && image.request.as_ref().is_some_and(|request| {
                request.text == image.text && request.result_callback.is_none()
            })
    });
    if !valid_images {
        return Err("native image fixture received invalid raw carrier".into());
    }
    eprintln!("image fixture joined: {report:?}");
    eprintln!(
        "{}",
        serde_json::json!({
            "requests": bot.trace.requests.lock().unwrap().len(),
            "rgb_images": bot.trace.images.lock().unwrap().len(),
            "cleaned": bot.trace.cleaned.load(std::sync::atomic::Ordering::SeqCst),
            "dropped": bot.trace.dropped.load(std::sync::atomic::Ordering::SeqCst),
        })
    );
    Ok(())
}
