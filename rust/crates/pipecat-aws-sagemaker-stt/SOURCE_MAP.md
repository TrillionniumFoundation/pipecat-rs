# Source and independent wire map

Pinned source: `ProfHepta/pipecat@af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- [`src/pipecat/services/nvidia/sagemaker/stt.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/nvidia/sagemaker/stt.py), Git blob `0ac95ae6009795679793f1c882fe548e73dad082`: default configured model/language, setup session, append+commit per `run_stt` chunk, NIM session.end, delta/completed mapping and whitespace gate. Native files: config/protocol/processor/app. The Python reconnect and ambient SDK credential lifecycle are explicitly excluded, not falsely reproduced.
- [`src/pipecat/services/aws/sagemaker/bidi_client.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/aws/sagemaker/bidi_client.py), blob `d23138d8a6fa0ef09feca0097166683da376aa34`: host:8443, request Bytes UTF8, optional model path/query absent in NVIDIA source. Native files: config/protocol/transport.

Independent AWS contract: `aws/aws-sdk-js-v3@e37bf8c2898e5e85201648f81dd07974e7d7e51c`. This is not a claim that Python resolves that SDK version.

- [Service model](https://github.com/aws/aws-sdk-js-v3/blob/e37bf8c2898e5e85201648f81dd07974e7d7e51c/codegen/sdk-codegen/aws-models/sagemaker-runtime-http2.json), blob `35bf61f726c0576f18ac0bb918a0b4c302f3836d`: `sagemaker` signing, H2, commercial regional :8443, endpoint-name limits, PayloadPart union and blob eventPayload.
- [Generated schemas](https://github.com/aws/aws-sdk-js-v3/blob/e37bf8c2898e5e85201648f81dd07974e7d7e51c/clients/client-sagemaker-runtime-http2/src/schemas/schemas_0.ts), blob `5ea0e1b62107756521eab68499d5466ddc8367f8`: POST operation URI; Bytes payload; DataType, CompletionState, P event headers; ModelStreamError/InternalStreamFailure.
- [Smithy EventStream](https://smithy.io/2.0/aws/amazon-eventstream.html), [eventPayload](https://smithy.io/2.0/spec/streaming.html#eventpayload-trait): raw payload envelope rather than structure JSON/base64 wrapper.

Native baseline `f278b40d9118939f29f6fb687ea11cf75b1bdbdc`. New transport owns an isolated adaptation of existing `pipecat-aws-nova-sonic/src/transport.rs` retained-owner/flow-control mechanics, with SageMaker regional authority/port/path/header/input-pair policy and no Nova module import or shared visibility changes. Its existing lifecycle fixture patterns are adapted, not independent proof of SageMaker semantics. `tests/common` independently implements CRC/framing/header/HMAC validation of captured bytes and emits raw-blob responses without using the production signer/codec. Fixed vectors plus actual app/CLI peers bind the new path/config/audio/terminal contract.

Linux descriptor/path verification follows the reviewed `pipecat-aws-sagemaker` file-lifecycle design, copied into this crate's owned CLI helper, not imported via hidden source inclusion. Public dependencies remain core/common only; there are no services/turns/media/provider/facade edits or SDK additions. Cargo.lock adds only the local package edges using already locked dependencies.
