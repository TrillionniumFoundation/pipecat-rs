//! Real loopback HTTP LLM -> registered Rust tools -> context -> LLM continuation.
use pipecat::{core::*, processors::tools::*, services::*, turns::*};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
async fn request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(i) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let length: usize = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_tools_finish_before_one_upstream_context_continuation() {
    tokio::time::timeout(Duration::from_secs(5),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config=ClientConfig::new(Provider::OpenAi,"synthetic");config.endpoint=format!("http://{}/completion",listener.local_addr().unwrap());
        let server=tokio::spawn(async move{
            let mut bodies=vec![];
            for round in 0..2 {
                let (mut socket,_)=listener.accept().await.unwrap();bodies.push(request(&mut socket).await);
                let event=if round==0{json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"double","arguments":"{\"n\":3}"}},{"index":1,"id":"b","type":"function","function":{"name":"double","arguments":"{\"n\":4}"}}]},"finish_reason":"tool_calls"}]})}else{json!({"choices":[{"delta":{"content":"6 and 8"},"finish_reason":"stop"}]})};
                let data=format!("data: {event}\n\ndata: [DONE]\n\n");
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{data}",data.len()).as_bytes()).await.unwrap();
            }
            bodies
        });
        let mut context=Context::new(vec![Message::text(Role::User,"Double 3 and 4")]);
        context.tools=Some(vec![pipecat::services::ToolDefinition{name:"double".into(),description:"Double a number".into(),parameters:json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]})}]);
        let pair=LLMContextAggregatorPair::new(context.clone(),Default::default(),Default::default());
        let mut tools=ToolExecutor::new(4,128,1024).unwrap();
        tools.register("double",RegisteredTool::new(Arc::new(|call|Box::pin(async move{
            if call.tool_call_id=="b"{tokio::time::sleep(Duration::from_millis(30)).await;}
            Ok(json!(call.arguments["n"].as_i64().unwrap()*2))
        })))).unwrap();
        let llm=LlmProcessor::new(LlmClient::new(config).unwrap(),CompletionRequest::new("synthetic",context));
        let mut worker=Pipeline::new(vec![Arc::new(llm),Arc::new(tools),pair.assistant()]).start(PipelineConfig::default()).await.unwrap();
        worker.sender().send(Frame::new(FrameData::LlmRun),Direction::Downstream).await.unwrap();
        loop {
            let out=worker.recv().await.expect("pipeline stopped before continuation");
            if let FrameData::LlmContext(context)=out.frame.data {
                if context.messages.last().is_some_and(|message|message.role==ChatRole::Assistant && message.content=="6 and 8") { break; }
            }
        }
        worker.end().await.unwrap();let (_,report)=worker.collect().await.unwrap();assert!(report.errors.is_empty(),"{:?}",report.errors);
        let bodies=server.await.unwrap();
        let history=bodies[1]["messages"].as_array().unwrap();
        let tool_results:Vec<_>=history.iter().filter(|m|m["role"]=="tool").collect();
        assert_eq!(tool_results.len(),2);assert!(tool_results.iter().any(|r|r["tool_call_id"]=="a"&&r["content"]=="6"));assert!(tool_results.iter().any(|r|r["tool_call_id"]=="b"&&r["content"]=="8"));
        let snapshot=pair.context().snapshot().unwrap();assert_eq!(snapshot.messages.last().unwrap().content,Some(Content::Text("6 and 8".into())));
    }).await.unwrap();
}
