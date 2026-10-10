//! Test-only native fixture. It never launches commands, reads credentials, or
//! contacts a network service. Production integrations choose their own binary.
mod fixture;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) != Some("--native-test-fixture") {
        return;
    }
    if let Some(path) = args.get(2) {
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }
    let mode = std::env::var("MCP_FIXTURE_MODE").unwrap_or_default();
    let writer = Arc::new(Mutex::new(tokio::io::stdout()));
    let active = Arc::new(Mutex::new(
        BTreeMap::<String, tokio::task::AbortHandle>::new(),
    ));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let message: Value = serde_json::from_str(&line).unwrap();
        let method = message["method"].as_str().unwrap_or_default();
        if method == "notifications/cancelled" {
            if let Some(task) = active
                .lock()
                .await
                .remove(&message["params"]["requestId"].to_string())
            {
                task.abort();
            }
            continue;
        }
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        if method == "initialize" && mode == "hang-initialize" {
            continue;
        }
        if method == "tools/call" && message["params"]["name"] == "side_effect" {
            return;
        }
        let writer = writer.clone();
        let active_tasks = active.clone();
        let mode = mode.clone();
        let key = id.to_string();
        let task_key = key.clone();
        let task = tokio::spawn(async move {
            let method = message["method"].as_str().unwrap();
            if method == "tools/call" && message["params"]["name"] == "slow" {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            if method == "tools/call"
                && message["params"]["name"] == "oversized"
                && mode == "unterminated"
            {
                let mut writer = writer.lock().await;
                writer.write_all(&vec![b'x'; 8192]).await.unwrap();
                writer.flush().await.unwrap();
                return;
            }
            let result = match method {
                "initialize" => fixture::initialize(),
                "tools/list" => fixture::tools(),
                "tools/call" => fixture::call(
                    message["params"]["name"].as_str().unwrap(),
                    &message["params"]["arguments"],
                ),
                _ => json!({}),
            };
            let response = if message["params"]["name"] == "failure" {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"secret-token must not appear in errors","data":{"password":"secret-token"}}})
            } else {
                json!({"jsonrpc":"2.0","id":id,"result":result})
            };
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            let mut writer = writer.lock().await;
            if writer.write_all(&bytes).await.is_ok() {
                let _ = writer.flush().await;
            }
            active_tasks.lock().await.remove(&task_key);
        });
        active.lock().await.insert(key, task.abort_handle());
    }
    for task in active.lock().await.values() {
        task.abort();
    }
}
