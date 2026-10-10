//! Retained old native flush completion cannot release a reused Soniox owner.
#![cfg(feature = "soniox")]
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_soniox::{SonioxSttClient, SonioxSttConfig, SonioxSttProcessor};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Semaphore},
};

struct Hold {
    entered: Semaphore,
    event: Mutex<Option<LocalFlushEvent>>,
}
#[async_trait]
impl FrameProcessor for Hold {
    fn name(&self) -> &str {
        "IndependentHeldTurnStart"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::FlushDetailed(marker) = &frame.data {
            if marker.origin.as_deref() == Some("pipecat.soniox.turn-start") {
                *self.event.lock().unwrap() = marker.event.clone();
                self.entered.add_permits(1);
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn independent_late_held_marker_cannot_revive_previous_setup_after_owner_reuse() {
    tokio::time::timeout(Duration::from_secs(8),async{
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let mut config=SonioxSttConfig::new("independent-soniox");config.endpoint=format!("ws://{}/stt",listener.local_addr().unwrap());config.vad_force_turn_endpoint=false;config.limits.close_timeout=Duration::from_millis(100);
        let (commands,mut rx)=mpsc::channel::<Value>(8);let (stages,mut stage)=mpsc::channel(8);
        let server=tokio::spawn(async move{for index in 0..2 {let mut session=WebSocketSession::accept(listener.accept().await.unwrap().0,SessionConfig::default()).await.unwrap();let Packet::Text(setup)=session.recv().await.unwrap() else{panic!()};assert_eq!(serde_json::from_str::<Value>(&setup).unwrap()["enable_endpoint_detection"],true);stages.send((index,"connected")).await.unwrap();loop{tokio::select!{command=rx.recv()=>{let Some(command)=command else{break};if session.sender().send(Packet::Text(command.to_string())).await.is_err(){break;}},packet=session.recv()=>{if packet.is_none(){break;}}}}let _=session.finish().await;stages.send((index,"closed")).await.unwrap();}});
        let stt=Arc::new(SonioxSttProcessor::new(SonioxSttClient::new(config).unwrap()));let first_pair=LLMContextAggregatorPair::new(Context::default(),stt.recommended_user_config(),Default::default());let hold=Arc::new(Hold{entered:Semaphore::new(0),event:Mutex::new(None)});
        let first=Pipeline::new(vec![stt.clone(),hold.clone(),first_pair.user(),first_pair.assistant()]).start(Default::default()).await.unwrap();assert_eq!(stage.recv().await.unwrap(),(0,"connected"));commands.send(json!({"tokens":[{"text":"stale first setup","is_final":true},{"text":"<end>","is_final":true}]})).await.unwrap();hold.entered.acquire().await.unwrap().forget();first.cancel("reuse with retained old marker").await.unwrap();let (_,report)=first.collect().await.unwrap();assert_eq!(report.state,WorkerState::Cancelled);assert_eq!(stage.recv().await.unwrap(),(0,"closed"));assert!(first_pair.context().snapshot().unwrap().messages.is_empty());
        let second_pair=LLMContextAggregatorPair::new(Context::default(),stt.recommended_user_config(),Default::default());let mut second=Pipeline::new(vec![stt,second_pair.user(),second_pair.assistant()]).start(Default::default()).await.unwrap();assert_eq!(stage.recv().await.unwrap(),(1,"connected"));hold.event.lock().unwrap().as_ref().unwrap().set();tokio::time::sleep(Duration::from_millis(60)).await;assert!(second_pair.context().snapshot().unwrap().messages.is_empty(),"old marker borrowed replacement owner");assert!(first_pair.context().snapshot().unwrap().messages.is_empty());commands.send(json!({"tokens":[{"text":"fresh second setup","is_final":true},{"text":"<end>","is_final":true}]})).await.unwrap();loop{let frame=second.recv().await.unwrap();if matches!(frame.frame.data,FrameData::LlmContext(_)|FrameData::LlmContextDetailed(_)){break;}}
        let context=second_pair.context().snapshot().unwrap();assert_eq!(context.messages.len(),1);assert_eq!(context.messages[0].content,Some(pipecat_services::Content::Text("fresh second setup".into())));second.cancel("reuse verified").await.unwrap();let (_,report)=second.collect().await.unwrap();assert_eq!(report.total_errors,0);assert_eq!(stage.recv().await.unwrap(),(1,"closed"));server.await.unwrap();
    }).await.expect("held marker retained or revived an old source owner");
}
