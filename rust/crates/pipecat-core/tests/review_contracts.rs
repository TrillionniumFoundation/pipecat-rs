//! Public serde identity must be isolated from local runtime bookkeeping.
use pipecat_core::*;
use std::sync::Arc;

#[tokio::test]
async fn separately_decoded_errors_have_fresh_identity_but_clones_share_identity() {
    for data in [
        FrameData::Error(ErrorFrame::new("plain")),
        FrameData::ErrorDetailed(DetailedErrorFrame {
            error: "detailed".into(),
            fatal: false,
            processor: None,
            exception: None,
            category: None,
        }),
    ] {
        let original = Frame::new(data);
        let encoded = serde_json::to_value(&original).unwrap();
        let first: Frame = serde_json::from_value(encoded.clone()).unwrap();
        let second: Frame = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(&first).unwrap(), encoded);
        let worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
            .start(PipelineConfig {
                error_history_capacity: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        let sender = worker.sender();
        for frame in [first.clone(), first, second.clone(), second] {
            sender.send(frame, Direction::Downstream).await.unwrap();
        }
        sender.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert_eq!(
            report.total_errors, 2,
            "two deserializations are distinct local identities even with equal public IDs"
        );
        assert_eq!(report.evicted_errors, 2);
    }
}

#[test]
fn frame_deserialization_keeps_the_public_serde_struct_name() {
    use serde::Deserialize;
    struct FrameNameProbe<D>(D);
    impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de> for FrameNameProbe<D> {
        type Error = D::Error;
        fn deserialize_any<V: serde::de::Visitor<'de>>(
            self,
            visitor: V,
        ) -> Result<V::Value, D::Error> {
            self.0.deserialize_any(visitor)
        }
        fn deserialize_struct<V: serde::de::Visitor<'de>>(
            self,
            name: &'static str,
            fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, D::Error> {
            assert_eq!(
                name, "Frame",
                "typed serde formats must not see a private helper type name"
            );
            self.0.deserialize_struct(name, fields, visitor)
        }
        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf
            option unit unit_struct newtype_struct seq tuple tuple_struct map enum identifier ignored_any
        }
    }
    let original = Frame::text("typed format contract");
    let json = serde_json::to_string(&original).unwrap();
    let mut deserializer = serde_json::Deserializer::from_str(&json);
    let restored = Frame::deserialize(FrameNameProbe(&mut deserializer)).unwrap();
    assert_eq!(restored, original);
}
