use pipecat_core::*;
use pipecat_network_bus::PythonJsonCodec;
use pipecat_workers::{BusMessage, MessageData};
use std::collections::BTreeMap;
#[test]
fn native_gemini_parts_have_no_invented_python_bus_representation() {
    for part in [
        MessagePart::GeminiText {
            text: "heard".into(),
            thought: false,
            signature: Some("secret".into()),
            incomplete: false,
        },
        MessagePart::GeminiText {
            text: "heard".into(),
            thought: false,
            signature: None,
            incomplete: true,
        },
        MessagePart::GeminiToolMetadata {
            tool_call_id: "call".into(),
            signature: Some("secret".into()),
        },
    ] {
        let context = LlmContextData {
            messages: vec![ContextMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Parts(vec![part]),
                name: None,
                tool_call_id: None,
                tool_calls: vec![],
                extra: BTreeMap::new(),
            }],
            tools: ToolSet::NotGiven,
            tool_choice: None,
        };
        let frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context,
            speculation: false,
        }));
        let message = BusMessage {
            source: "test".into(),
            target: None,
            data: MessageData::Frame {
                frame: Box::new(frame),
                direction: Direction::Downstream,
                bridge: None,
            },
        };
        let error = PythonJsonCodec::default()
            .from_bus_message(&message)
            .unwrap_err();
        assert!(!format!("{error}").contains("secret"));
    }
}
