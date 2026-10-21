# Source map and bounded divergence

Python repository ProfHepta/pipecat, exact commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. These are source references, not
claims about installed SDK versions or live hosted behavior.

| Python source | Git blob | Native surface |
|---|---|---|
| src/pipecat/services/deepgram/sagemaker/stt.py | 28643c7fb0edfb04274de9627727ceccf4cb3aa7 | config query, raw transport,5s KeepAlive, VAD Finalize, CloseStream, channel results |
| src/pipecat/services/aws/sagemaker/bidi_client.py | d23138d8a6fa0ef09feca0097166683da376aa34 | SageMaker8443 route, binary/UTF8 PayloadPart |
| src/pipecat/services/deepgram/stt.py | 0881926407d2f3407bc1fead2b2cec316fe885ee | immutable finite subset and field declaration order |
| src/pipecat/services/settings.py | a81809e8cc4f8760e70d9ae0f874473ba2f38f48 | source default/extra/NOT_GIVEN comparison; arbitrary merge explicitly unsupported |
| src/pipecat/transcriptions/language.py | 47a57eccc8d6fb6ce3c02bf6b061d74f9e909314 | exact accepted Language value set in src/language.rs |
| src/pipecat/frames/frames.py | 79dea3e32f5097a048d03b49f48031811fa1eca6 | final/interim and finalized default |
| src/pipecat/services/stt_service.py | 1198fb92413fcde1db1a24a93271bb0d47b133ed | pending finalize confirmation boolean |

Public source URLs have the form
https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/deepgram/sagemaker/stt.py
with the corresponding path substituted from the table.

AWS model independent pin `e37bf8c2898e5e85201648f81dd07974e7d7e51c`,
sagemaker-runtime-http2.json blob `35bf61f726c0576f18ac0bb918a0b4c302f3836d`,
generated schemas blob `5ea0e1b62107756521eab68499d5466ddc8367f8`: bidirectional
route, raw event payloads, model invocation/query header limits. No SDK execution
or dependency upgrade is used.

Native reference at repository baseline
`368354b0ee6ab98a2bd0dc22daa4f72d4f886933`: pipecat-aws-sagemaker-stt152 owned
transport, pipeline completion/output guard and Linux retained-descriptor helper.
Adapted copies are owned here, not path-included private internals. Existing152,
151, Nova/core and facade sources remain unchanged. The only shared implementation
change is additive common auth routing signing, retaining the old no-extra API.

Intentional native restrictions/improvements: explicit route/credentials only;
bounded immutable settings rather than arbitrary legacy deltas; strict JSON and
response-language failure; finite PCM and timeline; response drain after End;
retained joins, cancellation and no-retry/no-replay; strict file identities and
no-overwrite output. No microphone, SDK retries/refresh, reconnect, multichannel,
all settings, hosted qualification or full class parity is asserted.
