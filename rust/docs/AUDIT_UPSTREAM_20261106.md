# Pipecat 全范围差异清单、证据边界与 Hepta HMI 决策审计（2026-11-06）

## 1. 结论与审计口径

**建议以 Hepta 内生 HMI Organ 为产品边界，以现有 Rust 原生媒体/runtime crates 为可复用实现；继续改造 adapter，使其连接原 Hepta conversation/owner。不要为接入语音再次建立独立 LLM 对话、工具执行循环或权威会话数据库。** 当前 Rust 工程已有大量原生实现，称其只有薄包装不准确；把这些组件、静态映射或累计测试数称为完整 Pipecat 等价物、生产 Organ 也不准确。

完整 Rust 重写可以是库层长期目标，但不应成为完成当前 HMI 的前置条件。优先封闭同一 thread 的文本提交、增量输出、回连及去重，再封闭有真实来源和真实权限的语音链路。未经端到端验证的 provider、视频、跨平台设备能力须继续明确标记。HMI 不应取得比已有 owner/observation ingress 更宽的权限；原只读 organ trait 保持只读，写入必须经过既有 owner 的已授权接缝。

本报告做了两种不同深度的工作，不能混称：

- **范围盘点为全量**：对固定原始基线和当前官方提交的每个受版本控制 Python 文件记录路径与 Git blob；对冻结 Rust HMI 提交的全部 crate、manifest、各 crate 的 Rust 文件数及 `tests/` 目录下集成测试文件盘点（不把内嵌于 `src/` 的单元测试另行枚举）；比较分支血缘和官方新增路径。机器清单见 [audit-upstream-20261106.json](audit-upstream-20261106.json)。
- **语义审计为全类别、重点实现深读**：核对运行时、取消、网络、设备、语音、视频、provider、工具、评测等实际入口和明确限制，并复核既有迁移/验收记录。没有对全部 provider 的全部方法进行等价证明，也没有在本轮运行全 workspace、真实服务或物理验收。

因此不输出“完成百分比”。文件数、frame class 数、crate 数、编译通过、测试总数分别只证明它们自己的范围。以下“已有”表示冻结源码中有对应实现，具体是否运行过、在哪里运行、是否接入当前生产 owner 必须另看证据。

## 2. 固定版本与分支，禁止合并资格

| 对象 | 本次固定身份 | 作用与限制 |
| --- | --- | --- |
| 原始迁移基线 | [`af9f9da9d7afddaa8811c0e0251b32566c980bdf`](https://github.com/pipecat-ai/pipecat/commit/af9f9da9d7afddaa8811c0e0251b32566c980bdf)，2026-10-08 | 原 `ProfHepta/pipecat` 库存使用的基线；在官方仓库独立解析成功 |
| 当前官方 main 观测 | [`6fa9cf28886036d5d6c7dfd05ed3d6960245241a`](https://github.com/pipecat-ai/pipecat/commit/6fa9cf28886036d5d6c7dfd05ed3d6960245241a)，2026-11-06 | 当前比较点；不是原 Rust 工程承诺过的固定基线 |
| 官方最新 release 观测 | [`v1.15.0`](https://github.com/pipecat-ai/pipecat/releases/tag/v1.15.0)，2026-10-29 05:50:54Z 发布 | release 与当前 main 不是同一个承诺面 |
| Rust main | `11c9927e0595ec9474fb9157ac56166313b8ac8e` | 旧主线；不能把 HMI/后续 native 功能默认算进主线 |
| 本次 Rust 语义审计冻结点 | [`work/hmi-closeout-20261029` / `ec40f982205cb2c87f61094d78d163c5c2febb39`](https://github.com/TrillionniumFoundation/pipecat-rs/tree/ec40f982205cb2c87f61094d78d163c5c2febb39) | 本报告 Rust 引用均以此提交为准；本轮正在进行的工作区改动不混入其资格 |
| native parity 分支 | `work/native-parity-20261012` / `ad4a21fff16c608176163ecf044758746a8e0207` | HMI 是其后继，HMI 多 17 个提交；该分支最后提交明确写着 participant audio controls **source-only** |
| cloud MCP 分支 | `work/cloud-mcp-parity-20261013` / `3b7e965653dfff9d74c1c7ea934888b508d7137b` | 与 HMI 分叉：HMI 独有 173 个、该分支独有 36 个提交，不能拼接测试通过记录 |

main 到 HMI 为 190 个后继提交、无 main 独有提交。继承代码不等于继承整个新树的运行资格。`ProfHepta/pipecat` 在当前 connector 的 `get_repo` 返回 404，只表示该入口未能解析；不据此断言仓库被删除或不再存在。

固定原始基线有 1,434 个 Python 文件，其中 package 645、tests 338、examples 426、其他 25；当前官方有 1,656 个，分别为 750、423、454、29。全树差异为 1,073 files changed、87,568 insertions、19,224 deletions，包含目录重组，不能据此计算功能增长率。冻结 HMI 有 62 个 Rust crate。这些只是可复核范围边界。

### 旧文档应怎样使用

- [`inventory.json`](inventory.json) 的 `not_migrated` 是 2026-10-09 初始库存，不是 2026-11-06 缺失清单。
- [`COVERAGE.md`](COVERAGE.md) 记录历史 checkpoint，不能用其中旧缺口否定后来源码。
- [`MIGRATION.md`](MIGRATION.md)、[`MIGRATION_REVIEW_20261014.md`](MIGRATION_REVIEW_20261014.md) 是逐批记录；必须同时检查最终 crate 的 README、实现、测试和明确限制。例如 Gemini paired history、cyclic sprite、评测音频链路已有后来实现，不应继续沿用较早“尚未实现”的结论。
- `docs/DRAFT_PROVENANCE.json` 仍有 `UNQUALIFIED_WORKING_BRANCH_SOURCE` / `full_gate: NOT_RUN`，绑定的是更早源快照，不能改写成当前 HMI 全门禁通过。
- [`HMI_REVIEW_20261023.md`](HMI_REVIEW_20261023.md) 和 [`HMI_NATIVE_CLOSEOUT_20261029.md`](HMI_NATIVE_CLOSEOUT_20261029.md) 记录不同阶段、不同设备/owner/模型证据。模型文件后来可用不抹去原阶段的阻塞，物理 DMA 后来运行也不补全真实听感。

## 3. 证据等级

| 等级 | 实际能证明什么 | 不能代替什么 |
| --- | --- | --- |
| I：库存/静态映射 | 文件、类型、配置、测试入口存在，固定 blob 可追溯 | 方法实际运行、provider 兼容、效果完成 |
| S：实现与消费者深读 | 特定入口如何流控、终止、降级、记录错误 | 未执行分支、native ABI、生产行为 |
| F：离线/loopback fixture | 该源码在所记录环境对受控对端满足断言 | 云服务、真实鉴权、NAT、硬件、生产 owner |
| N：实际 native engine | 所固定库/模型真的推理或处理 PCM | 输入是真人当前说话、识别正确、用户听到了 |
| O：owner 进程证据 | 实际 Hepta owner 进程/既有存储/请求身份及恢复路径 | 当前生产会话和真实外部效果，除非使用的确是它们 |
| D：物理设备证据 | 选定设备的实际 native capture/output 路径 | 插孔连接、房间声学、人听清、无回声、OS 撤权 |
| P：生产 Organ | 同一真实 owner/thread + 权限 + 物理/网络 + 独立效果与用户感知证据 | 不能由前述等级简单求和，必须在同一端到端运行中闭合 |

本专题上游盘点审计新增的证据为 I/S，没有重跑 F/N/O/D/P。下文历史测试结果均为“既有文档记录”，不是本专题新增的通过回执。本轮主实现另行运行的 Core 等软件测试，由主实现报告绑定其实际源码与命令记录，不混入本专题冻结点的资格。

## 4. 全类别差异矩阵

本节 Rust 路径相对于 `rust/crates/`，均绑定 `ec40f982…`；Python 路径相对于官方基线 `src/pipecat/`。最新官方变动另列于第 6 节。

| 能力类别 | 原 Pipecat 的实际面 | 冻结 Rust 已有实现与证据路径 | 差异、开放项及 HMI 边界 |
| --- | --- | --- | --- |
| Frame 类型与 payload | `frames/frames.py`；dataclass/mixin、可变对象、优先级与 interruptible | `pipecat-core/{COMPATIBILITY.md,src/frames.rs,src/frame_payloads.rs}`、`tests/native_contract_examples.rs`；固定基线 134 个 Frame-derived 类有专门表示，161 个 native variant | 类型存在不代表所有 consumer 处理；本地 callback/ref 不可序列化；Rust owned bytes 不复刻 Python 对象共享/反射。最新官方已 142 个，见第 6 节 |
| 异步双向 pipeline | `processors/frame_processor.py`、`pipeline/{pipeline,task}.py`；normal/system、Start、task manager | `pipecat-core/README.md`、`tests/{runtime,admission,drain,terminal_stream_admission}.rs`；Tokio、独立 Start/system/normal lane | native 内部入队 fail-fast；外部等待容量。故意不复制无界缓冲；必须向 owner 暴露 pressure/failure |
| 生命周期、错误、health | Start/End/Cancel/Stop/flush、permanent error、ready/usability、task cleanup | `pipecat-core` 的 health revision、WorkerReport、child drain、startup/shutdown timeout；`tests/{sync_health_error_admission,health_generation,review_usability}.rs` | fatal/panic/cleanup failure 不能被下游消费错误帧后掩盖。Stop 保留资源但无通用重新 attach/resume API；非协作阻塞不受 async timeout 强制中止 |
| 取消与 interruption | 取消当前处理，丢弃可中断队列，保留/传播系统控制 | `pipecat-core/README.md` 的 invocation epoch、tracking task、retirement；`pipecat-core/tests/review_generations.rs`、`pipecat-pipeline/tests/independent_held_cancel.rs` | native epoch 是进程内因果权限，反序列化不会复活。远端/设备已经生效不能“撤销”；取消工具请求不是工具 rollback |
| 串并行与同步 | nested Pipeline、ParallelPipeline、SyncParallelPipeline、分支 dedupe/order | `pipecat-pipeline/README.md`、`tests/{parallel,nested_shutdown,independent_held_cancel,review_lifecycle}.rs` | sentinel 只覆盖同步处理输出，不等于任意 detached background quiescence；有 branch/frame/ID/deadline 上界；缺 sentinel 显式失败 |
| 服务切换与容错 | 手工/健康切换、LLM service routing | `pipecat-pipeline/tests/{switching,llm_switching,service_switching_review}.rs`、core compound health | 已有 native 切换，不应再次列成缺失；切换不授权自动重放已可能产生效果的请求。provider 特定恢复仍逐个适配 |
| 心跳、idle、观测 | task heartbeat、idle timeout、事件 callback/observer | `pipecat-core/{HEARTBEATS.md,WORKER_IDLE.md}`、`pipecat-observers` | native opt-in，observer bounded try-send，丢失有计数/不完整区间；慢 observer 不阻塞 pipeline。Python callback/decorator、动态注册和所有外部 exporter 无通用等价 |
| 音频基础处理 | PCM、WAV、G.711、DTMF、resampler、音量、混音/滤波 | `pipecat-audio/src/{pcm,wav,g711,resample,dtmf,volume}.rs`、media transport mixer/filter/recording | native sinc 不等于 SOXR 数值/quality 模式；不把压缩流重标为 PCM。应从真实 format/rate/channel 建立转换链 |
| VAD 与 turn analysis | Silero、SmartTurn、VAD start/stop、策略组合 | `pipecat-vad/README.md`、模型 provenance、pipeline tests | 真 ONNX/native 调用已有；输入可能为合成。真人中文、噪声、回声、重叠说话/误触发仍须目标设备验证；不能把 SpeechStarted 当 speaker authentication |
| 降噪与专有算法 | RNNoise、Krisp/相关策略、任意 Python filter | `pipecat-rnnoise/{README,PROVENANCE}.md` | RNNoise 原生库与模型可 pin；C 不可安全抢占、load 有稳定文件要求；专有 Krisp/任意 Python plugin 不应声称复刻。降噪≠AEC |
| 本地 STT | Whisper、Moonshine 等本地服务 | `pipecat-whisper`、`pipecat-moonshine`；HMI `local_speech` | 已有真实 native 模型。HMI Whisper 输出 final transcript，不是增量 partial STT；VAD、模型 load/infer/取消边界分别可测 |
| 本地 TTS | Piper、Kokoro、Pocket TTS、XTTS 等分别有 source service | `pipecat-piper`，HMI 使用 ONNX/eSpeak；其余不因都叫本地 TTS 而自动覆盖 | 音色/语言、phonemizer/license、真实发音质量必须独立选择和验收。真实模型生成 PCM 不等于扬声器可听 |
| 云端分段/流式 STT | 各厂商 HTTP/WS、utterance/finalization/keepalive、diarization | `pipecat-speech`、`pipecat-streaming-speech` 及 AssemblyAI/Gladia/Soniox/Speechmatics/Deepgram Flux 等专属 crates | 有具体协议 native 实现，不是统一万能 API。HTTP/WS 和 provider final/segment timing 不可互换；每个模型/语言/鉴权/重连单独资格 |
| 云端 TTS 与多片段 | 聚合文本、流式 PCM、timestamp、settings、取消、context IDs | `pipecat-text`、`pipecat-speech`、`pipecat-streaming-speech`、`pipecat-elevenlabs`、`pipecat-gradium` 等 | TTS 字符 alignment 不是播放回执。增量 LLM token 不应直接逐 token 执行 speak；HMI 只采用 owner 的 committed assistant items |
| LLM 请求与历史 | Chat/Anthropic/Gemini、schemas、工具、thinking、caching、multimodal | `pipecat-services`、`pipecat-responses`、`pipecat-core` continuation/history carriers | 专门实现和 compatible preset 必须分开。Hepta 路径的模型/历史由原 owner 负责，不能把这些通用 bot 能力变成第二个 agent |
| Realtime speech-to-speech | OpenAI/Azure/Gemini/xAI/AWS/Inworld 等专属会话协议 | `pipecat-realtime`、`pipecat-azure-realtime`、`pipecat-gemini-live`、`pipecat-grok-realtime`、`pipecat-aws-nova-sonic`、`pipecat-inworld-realtime`、`pipecat-azure-voice-live` | session-resume/response ID、input preroll、tool cancellation、history 各自有契约；不可由一个 provider 的 loopback 通过推出另一家可用 |
| 用户/assistant turn 聚合 | VAD/STT/LLM completion、eager turn、summarization、tool result/context | `pipecat-turns`、`pipecat-processors`，`pipecat-turns/COMPLETION.md` | 已有较多 native turn 策略；高级 eager/completion 绑定特定 processor，Responses/所有 realtime 不自动适用。通用上下文不得取代 Hepta conversation |
| 工具、MCP、worker/flows | function handler、MCP、worker registry、flow state/actions、LLMWithBackend | `pipecat-mcp`、`pipecat-workers`、`pipecat-agent-workers`、`pipecat-flows`、`pipecat-aws-agentcore` | MCP discovery 不是执行授权；stdio 只管理直接子进程。Hepta HMI 不自行调用模型/tool executor，不把 flows state 升成权威 thread state |
| 本地麦克风/扬声器 | PyAudio/local transport，设备回调和生命周期 | `pipecat-local-audio` 用 CPAL，HMI `pipecat-hepta/src/local_speech.rs` | callbacks 有 bounded allocation，是 soft realtime；驱动 open/play/drop 可阻塞；断开为 terminal，显式重开。平台库可编译≠各平台权限/设备合格 |
| 共享媒体输出与多目的地 | `transports/base_output.py` 音频 FIFO、PTS、image、sprite、mixer、record | `pipecat-media-transport` 各 destination queue/fence、录音、spoken ledger、`CYCLIC_VIDEO.md` | 现已有 cyclic image/sprite，不能列为全缺；不含摄像头/codec/resize/device sink。有限 flush 不等待无限动画；PCM timestamp 本身不重排音频硬件 clock |
| 原始音频、记录与来源 | audio buffer recording、user audio frames、context observations | media recording + `pipecat-hepta/src/media_store.rs`，`tests/media_store.rs` | exact captured PCM、digest/sample span、配额/路径约束已有；这是媒体证据存储，不是会话库。digest 只证明 bytes，不证明用户身份/录音授权；owner 必须验证来源再准入 |
| WebSocket/TCP/HTTP | server/client transports、session callbacks、timeouts | `pipecat-transports`、`pipecat-runner`、`pipecat-client-server` | native bounded network 并非 auth/session authority；Origin 只限浏览器 CSRF。标准 runner 每连接新 context 且默认不开放重连接受入口，不能直接当 Hepta 同一 thread 续接 |
| SmallWebRTC | aiortc、audio/video、ICE、SDP、客户端适配 | `pipecat-webrtc` native ICE/DTLS/SRTP/RTCP/SCTP、真实 loopback UDP、音频/data | 明确缺 video/camera/screen、adaptive jitter、PLC、Opus FEC、同步 playout clock、既有 peer ICE restart/track replace；outgoing mono。TURN/NAT/browser/物理另验 |
| LiveKit / MoQ | LiveKit tracks/audio/video/room lifecycle；MoQ transport | `pipecat-livekit`、`pipecat-moq` | 存在 native 实现及 bounded sampling/request 生命周期，不能称完全无视频；SFU/live remote video、最终 participant control 源码门禁仍未获整体资格。SDK decoder/native queue 不在全进程内存上界内 |
| 其他实时 transport/avatar | Daily、HeyGen、LemonSlice、Tavus、Vonage/WhatsApp、local Tk、Simli 等 | 见源目录盘点；protocol codec/adapter 与托管连接分开 | 没有查到覆盖所有这些入口的专属 native 实现；不得把 RTC/audio 有输出称作这些 SDK 的 callback/房间/渲染/鉴权全等价 |
| 电信 serializer | Twilio、Plivo、Telnyx、Vonage、Exotel、Genesys AudioHook、protobuf | `pipecat-protocol/README.md`、严格长度/format/sequence schema | serializer 不是运营商账号、call-control、REST hangup。carrier mark/clear ack 不是声学播放。当前官方新增 Jambonz 另列 |
| RTVI / 浏览器客户端 | RTVI ready、text/audio、事件、metrics、function/UI 等 | `pipecat-protocol` RTVI 2.2/legacy 1.x；`rust/client-templates`、`pipecat-client-server` | 协议编解码、WASM 构建、合成 peer 均不能替代真实浏览器 microphone permission、中文 IME、autoplay、回连、用户可见状态验收 |
| 图像生成与视觉输入 | OpenAI/Gemini/Azure/Fal 等服务、图片请求/抓帧、file inputs | `pipecat-images` 原生 OpenAI/Gemini/Azure；media/LiveKit image request；Realtime/Gemini sampled RGB→JPEG | 图像生成不是双向实时视频；default image byte budget/hosted URL policy必须配置。HMI camera权限/抓帧时效/AV同步仍独立 |
| 消息总线/分布式 | bus envelopes、worker registry、Redis/PGMQ、serialization | `pipecat-network-bus`、`pipecat-workers`；原 envelope 保留、125/134源 frame 条件覆盖 | callback/对象引用及 9 类受限，不能 pickle 任意 Python graph。网络 ack≠应用完成；中途 publish unknown不自动重试；Redis Cluster/Sentinel无对应实现 |
| 评测、simulation、CLI/脚手架 | script/suite、audio/image scenarios、judging、templates、examples | `pipecat-evals`、`pipecat-project`、`pipecat-cli`、`pipecat-classifiers`；音频→音频、记录音频、bot capture/judge、用户图片路径均已存在 | 原来“仅文本 eval”已过期。fixture judge/合成媒体不是真实用户质量；直系子进程 ownership不等于 sandbox或完整进程树；全部 Python examples没有自动成为 native apps |
| 运维、性能、tracing | latency/usage/turn metrics、日志、OTEL/observer、部署集成 | native metrics/reducers、bounds、明确 log redaction、多 source-pinned门禁记录 | 没有证据支持“Rust 一定更低端到端延迟”或跨平台性能已达标；traceparent字段不自动安装OTEL exporter，TTFB/总时延/听感须定义端点 |
| Hepta 内生 Organ | 原 Pipecat 通用 bot 框架不提供此项目的 conversation/owner 权威 | `pipecat-hepta` + Hepta原 adapter/owner/签名 observation ingress；见 HMI文档 | 这是新增的产品整合契约，不能通过“原 Pipecat parity”自动取得。真实 owner、独立 signer、device policy、物理感知、恢复和shadow必须连到同一thread |

矩阵中类型/入口存在与实现可执行分开陈述。`pipecat-core` 的全部 source frame 表示不等于 network bus 的任意 frame 可传，也不等于 provider 接受该 frame；这三个层级有不同允许集。

## 5. Provider 全量目录核对

原基线有 62 个 provider 目录，当前官方 63 个。下表覆盖当前官方全部目录；这是按实际 native 入口归类的接入面清单，**不表示目录下所有 STT/TTS/LLM/专属 metadata 均等价**。`P` 表示明确的 Chat Completions preset，不代表该厂商的完整服务；`N` 表示存在专门 native 入口；`—` 表示本次冻结 Rust 接入面盘点未发现专属实现，不能由 generic endpoint 推定通过。

| 官方目录 | Rust 接入面 | 本次边界判断 |
| --- | --- | --- |
| anthropic | N `pipecat-services` | Messages/历史/签名/cache；live provider另验 |
| assemblyai | N `pipecat-assemblyai` | STT路径；最新上游新增TTS不可自动计入 |
| asyncai | — | 需单独迁移/选择替代服务，不冒充兼容 |
| aws | N Bedrock/Nova Sonic/speech/SageMaker/AgentCore系列 | 各AWS协议、credential/role与region分别验 |
| azure | N services/realtime/voice-live/images | Azure Chat/Realtime/Voice Live/图片；不是所有Azure Speech服务总覆盖 |
| baseten | P | Chat preset，非全部厂商扩展 |
| bland | — | 无专属native资格 |
| camb | — | 无专属native资格 |
| cartesia | N speech/streaming-speech | HTTP/WS TTS、STT/turn相关具体路径 |
| cerebras | P | Chat preset |
| crusoe | P | Chat preset |
| deepgram | N speech/streaming-speech/deepgram-flux | STT source是WS listen；不虚构REST替代等价 |
| deepseek | P | Chat preset |
| elevenlabs | N speech/elevenlabs | HTTP STT/TTS及专属流式路径，diarization等按实现限定 |
| fal | — | 不能把其他图片生成实现算作Fal |
| fireworks | P | Chat preset |
| fish | — | 无专属native资格；最新expression adapter另属新面 |
| funasr | — | 无专属native资格 |
| gladia | N `pipecat-gladia` | 专属流式STT协议 |
| google | N services/gemini-live/images | Gemini选定协议；Vertex/File API/Google built-in tools等明确限制 |
| gradium | N `pipecat-gradium` | 专属语音协议 |
| grok | P/alias，另有grok-realtime | 原兼容module与xai别名，不重复算一家 |
| groq | P | 不能由Chat preset推出该厂商STT/TTS |
| heygen | — | avatar/transport行为未被通用WebRTC覆盖 |
| hume | — | 无专属native资格 |
| inception | P | Chat preset |
| inworld | N `pipecat-inworld-realtime` | 指定实时协议；不能外推所有source服务 |
| keenable | — | 无专属native资格 |
| kokoro | — | Piper不能证明Kokoro等价 |
| mem0 | — | 若选用记忆服务仍不能成为Hepta第二会话权威 |
| meta | — | 无专属native资格 |
| minimax | N `pipecat-speech/src/minimax.rs` | 具体TTS/语言情绪字段，非全部厂商API |
| mistral | P | Chat preset；TTS/新expression面另审 |
| moondream | — | 无专属native资格 |
| moonshine | N `pipecat-moonshine` | 本地STT、模型/native provenance另验 |
| nebius | P | Chat preset |
| neuphonic | — | 无专属native资格 |
| novita | P | Chat preset |
| nvidia | N services专属reasoning，亦有preset | 不覆盖最新Omni及所有STT/TTS |
| ollama | P | 本地compatible endpoint不等于全部Ollama扩展 |
| openai | N services/responses/realtime/speech/images | 路径丰富但各有明确不支持项；生产owner仍由Hepta决定 |
| openclaw | N `pipecat-openclaw` | 专属协议/frames，不等于授权执行任意工具 |
| openrouter | P | Chat preset |
| perplexity | P | Chat preset，厂商搜索metadata须按实际decoder核对 |
| piper | N `pipecat-piper` | ONNX/eSpeak，模型/voice/license/语言质量分别验 |
| pocket_tts | — | 无专属native资格 |
| qwen | P | Chat preset |
| resembleai | — | 无专属native资格 |
| rime | — | 最新tts_v1同样不能推定已有 |
| sambanova | P | Chat preset |
| sarvam | N services专属routing/header/capabilities，亦有preset | Chat范围；不能扩为全部speech |
| simli | — | avatar渲染/远端生命周期独立 |
| smallest | — | 最新LLM入口是新比较项 |
| soniox | N `pipecat-soniox` | 专属STT；新expression面另审 |
| speechify | N `pipecat-speech/src/speechify.rs` | 指定TTS接口 |
| speechmatics | N speech/speechmatics | TTS与专属语音路径分别限制 |
| tavus | — | 不能由媒体共享层推定conversation/avatar服务 |
| together | P | Chat preset |
| ultravox | — | 无专属native资格 |
| whisper | N `pipecat-whisper` | 本地native推理；中文已见真实样本错误 |
| xai | P Chat，N grok-realtime | 各协议分别资格 |
| xtts | — | 无专属native资格 |
| zoom_ai | —（当前官方新增） | 本次固定原基线不含；最新STT/realtime需另行迁移 |

可复核的 preset 名单实际来自 `pipecat-services/src/presets.rs::OpenAiPreset::ALL`（21项，包含有专属增强的Azure/NVIDIA/Sarvam/OpenAI），不从名字相似推断。HTTP speech、专属streaming/realtime、图像gen是不同consumer；一条绿测试不能覆盖同名provider全部服务。

## 6. 固定基线之后的官方新增与语义变化

### 6.1 确认新增的 frame 合约

使用和本工程 exporter 相同的 AST 继承归一化思路（展开 `ast.Subscript` 泛型基类），原基线包括 `Frame` 共134个，当前官方142个，旧类未删除，新增8个：

| 新类 | 当前官方来源 | Rust冻结点核查结果 |
| --- | --- | --- |
| `UserBackchannelFrame` | `frames/frames.py` | 无同名/明确对应consumer链路资格 |
| `BotBackchannelFrame` | 同上，继承TTSSpeakFrame | 不能仅因能TTS就宣称backchannel时序等价 |
| `LLMContextUserTurnFrame` | 同上 | 与既有context消息frame不是自动等价 |
| `UserTurnResponseCancelFrame` | 同上 | 需要与Hepta原turn取消契约明确映射，不能扩大权限 |
| `UserTurnCompletionFrame` | 同上 | Rust已有completion config/机制，但新frame及consumer链路未作最新差分证明 |
| `TTSMetadataFrame` | 同上 | 需要新增metadata consumer/serializers核对 |
| `PerceptionMetadataFrame` | 同上 | 与最新expressive/perception链路有关 |
| `ExpressionMetadataFrame` | 同上 | 与最新expressive/expression链路有关 |

一手源码：[固定基线 frames.py](https://github.com/pipecat-ai/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/frames/frames.py)、[当前官方 frames.py](https://github.com/pipecat-ai/pipecat/blob/6fa9cf28886036d5d6c7dfd05ed3d6960245241a/src/pipecat/frames/frames.py)。故现有“134类全表示”的结论仅对旧基线成立，不可写“当前原Pipecat全部frame已完成”。

### 6.2 新增目录不是无意义重命名

当前官方 diff 除结构迁移外，还包括下列实际新增面，机器清单保留完整路径变化：

- `expressive/` 的 expression/formatter/instructions/perception/policy/spoken 及 `processors/{perception_reader,tone_reader,tone_tagger}`；LLM/TTS adapters 重新布局并新增多provider expression adapter。
- `audio/speech_segment_buffer.py`；新的 user-stop classifier、audio context manager、word timestamp tracker。
- `pipeline/llm_with_backend.py` 重组为 `pipeline/llm_with_backend/`，含 connector、predictive delegation、prompts、request strategies；不能仅拿旧文件存在推定新取消/交接语义一致。
- `serializers/jambonz.py`；`services/assemblyai/tts.py`、Google interactions/vertex、NVIDIA Omni、Smallest LLM、ZoomAI realtime/STT、Rime `tts_v1` 等新入口。
- evals conversation/observer/fastapi transport/simulation judge；classifier、security/redaction、image/model utils、tagged LLM text parser 等变化。

这些新面要么属于后续通用库的迁移工作，要么不是当前 HMI 必需项。HMI不应为追赶所有新provider而延后原thread正确性和真实设备闭环。

### 6.3 应纳入差分回归的上游问题

[官方 v1.15.0 release](https://github.com/pipecat-ai/pipecat/releases/tag/v1.15.0) 与当前固定源码提供以下回归候选。它们是已知上游语义变动，不是在未复现时指控Rust有相同bug：

| 上游变化 | Rust审计/回归动作 |
| --- | --- |
| LLMWithBackend 在用户“never mind”时取消/交接 | 证明原Hepta owner的取消身份、已发生effect状态与前端文本一致；不得让HMI自行用新LLM补答 |
| OpenAI Responses WS 用独立stream_id lanes，新的请求不等待中断旧流结束 | Rust当前序列化/terminal drain设计是更窄契约；测试迟到旧事件不归属新请求、continuation不串，明确延迟取舍 |
| segmented STT仅在用户未再开口时finalize；setup timeout后清理transcription task | 对STT generation/segment/utterance边界补差分回归，防旧segment终结新讲话或漏掉owned task |
| NVIDIA TTS配置load移离eventloop | 所有native load/device open应在监督media process，不允许阻塞CNS；async包装本身不能证明无阻塞 |
| AudioVolumeTracker/分析工具下BufferError与TTS whitespace/order/tags/timestamp修复 | 用明确输入序列回放，不以不同实现名或更快语言跳过；记录源差异与修复范围 |
| 当前 FrameProcessor 的全lane pause-until-ready及单frame-in-progress取消辅助 | 核对Rust已有pause/epoch是否提供所需用户可见语义，不能用相近名字宣布最新API完全等价 |

## 7. 流控、错误、取消、回连与性能的正确语义

### 7.1 流控不是“所有 queue 都 bounded”一句话

`pipecat-core` 默认 normal/system/Start/output/background 分别有容量，内部push/queue在压力下fail-fast以避免双向等待环；外部sender等待后重新检查admission/terminal状态。通用core容量主要按**帧数**计，不按payload字节；保留一个大图像、PCM或metadata仍可造成大内存。media、wire、provider有各自byte上界，不组成对任意decoder/native SDK/allocator的全进程RSS保证。

HMI应分别记录：输入callback丢失/pressure、识别排队、owner admission拒绝、输出queue pressure、observer丢失和最终呈现缺口。不能把无数据统一变成成功空转录；不能以丢弃旧身份/去重记录来腾容量后静默接受重放。慢消费者必须可导致明确失败/背压，而不是无界累积。

### 7.2 取消有三个不同完成点

1. **逻辑退役**：旧turn/utterance/output epoch不再允许新输出入队。
2. **本地工作结束**：native任务、网络reader、子pipeline、media子进程已停止/被join；不等于阻塞C ABI可抢占。
3. **外部效果对账**：已经送到设备buffer、provider、原owner的tool effect是否发生，由相应owner/证据决定。

HMI已有先退役epoch再请求原turn取消的边界。停止播报不能写成工具撤销；不确定命令结果须是UNKNOWN，并保留原request身份去对账。以新request ID重试可能重复发生效果。

### 7.3 回连必须连接原权威来源

标准 `pipecat-runner` 为独立bot运行器：每session建立fresh context，有一次性connection capability，正常入口不提供同一listener回连接续。它可以演示协议/媒体，不能原样接成Hepta thread持久化层。

HMI回连应让原owner给出同一thread快照/事件cursor/原请求去重结果；进程内presentation ledger只能限制本incarnation输出，不能代替durable request dedupe。恢复时建立新的合法incarnation/generation，旧token/PCM/输出ack不复活。网络delivery/queued ack与owner admission/commit分开，否则客户端重发会重复准入。

provider回连又是另一层：Gemini的可选session-resumption需要有效saved handle，无自动action replay，无handle重建/无损恢复承诺，当前idle黑洞还需要调用方cancel/finish；Responses continuation有自己的history/options条件。**provider session ID既不是Hepta thread ID，也不是Hepta owner authority。**

### 7.4 性能证据必须从用户动作到感知端点

Rust源码与有界队列没有证明端到端低延迟。应该独立记录输入动作/首个音频sample、VAD start/stop、STT final、来源验证、原owner准入、首个delta、assistant item commit、TTS首PCM、设备callback、实际声学播放/可见输出。若没有最后一项，就只报告截至callback/submit的时间。

p50/p95/p99必须附样本数、warmup、设备/驱动/OS/CPU、模型hash、采样率/声道、语言/噪声、负载、failure/timeout/cancel比例及原始样本。丢掉失败样本只计算成功请求会高估体验。Windows、macOS、Linux需各自证据，不能把CPAL跨平台API或单机编译当作跨平台验收。

## 8. 现有 HMI 证据到底已经到哪里

### 已有、值得保留的实现

`pipecat-hepta` 本身没有LLM/tool loop、不授予设备权限、不创建conversation DB。HmiSession绑定principal/device/organ generation/logical session/turn/utterance/monotonic sequence/output epoch；raw audio通过private media root、exact PCM digest与sample span受控引用。local-speech可启用真正Whisper、Silero、Piper ONNX/eSpeak、CPAL。`LocalSpeech`由监督media process承载，不能放进CNS event loop。

已有Hepta接入与owner进程记录应被承认。2026-10-23记录包含实际owner进程重启与压力、签名audio ingress、lease/恢复回归；不能继续笼统宣称coordinator没有注册。但这些受控进程验证不是当前生产owner+当前物理用户语音的一次完整P级运行。[来源：HMI_REVIEW_20261023.md](HMI_REVIEW_20261023.md)

多assistant item已由原bridge汇聚，最多16,384 UTF-8 bytes，endpoint按≤4,096 bytes分段、PCM≤250ms块；超过限制不截断，partial output unknown不自动重播，token delta只用于显示。多个item到一次claim再到多个TTS片段的具体身份/顺序仍需端到端验证；不能把“有循环处理多个字符串”当作多个独立工具/owner output的正确定序。[来源：HMI_NATIVE_CLOSEOUT_20261029.md](HMI_NATIVE_CLOSEOUT_20261029.md)

### 已有测量与尚未证明的部分

| 记录 | 真实证据 | 仍未证明 |
| --- | --- | --- |
| owner软件路径 | 2026-10-23记录40次签名临时PCM observation，预热后32软件样本；文本终态p50/p95/p99为415121/478714/620428µs | 不含PCM准备、UI、实体采音、STT/TTS/播放；不是麦克风到耳朵延迟 |
| Linux设备capture | ALC897实际DMA，3秒48,000帧、peak2002、RMS283.2、47,991 nonzero samples | 有sample不证明接入了可用麦克风、听得懂当前用户 |
| 设备连接检查 | 当时 Input Source Front Mic，mic/line/headphone相关jack状态off | 不能把设备枚举/DMA当作插孔已连接或声学路径闭合 |
| acoustic probe | 公共中文音频回放与7秒capture，最高normalized correlation 0.012356、offset1702ms、RMS26.47 | 不足证明可听、回声消除、真实barge-in或实际端到端延迟 |
| media进程故障 | revoke_capture终止、malformed JSONL失败、SIGKILL缺terminal记UNKNOWN | 应用级revoke不是OS权限撤回；SIGKILL/queued ack不证明音频完整播放 |
| 中文真实语料STT | WeNet AISHELL人声文件、固定source/audio hash，Whisper base与small实际执行 | 不是现场真人麦克风；两模型均有词错，不证明中文交互合格 |
| 模型质量/时延取舍 | base输出“廣州市佛地产中间协会分析。”，small输出“广州市法地产中介协会分析。”，参考“广州市房地产中介协会分析”；原样本STT2.703328s与9.224479s | small改善但仍错，且慢；合成噪声8样本/单Linux不支持跨平台SLO或大样本识别质量结论 |
| neuron shadow | 已明确保留deterministic/live gates | 尚未运行可支持自适应收益的真实基线/shadow对照 |

这张表引用既有原始记录，不重新制造测量。AISHELL音频源固定为 `be6ad97fdb512589df113042f7ff3a29d392fb38`、`test/resources/aishell-BAC009S0724W0121.wav`，记录sha256 `2f9fc9c912bb71c85fb286cb88b599c81efb8f727c727a5ea8f6d1c89c55ac13`。原始私有PCM不应公开进入仓库或neuron输入。[完整来源与边界](HMI_NATIVE_CLOSEOUT_20261029.md)

旧完整 native checkpoint 记录 3,389 个 workspace tests 通过、0 失败、55 ignored，绑定 tested projection `1ccb07f7d11e283abde4712d9d72b2fd97e17486`。但同一资格文档还保留了后来 canonical source `167994707ab2b84a7ea3e9de423488752b5dbbbe`、tested projection `2cbbb058e1092f5113b27ff522795211c421c6d6`、tree `3e91f1f6c62671fe8e6504859de34f0bbc9ef339` 的完整门禁：**3,477 通过、1 失败、55 ignored，exit 101**。失败为 `pipecat-webrtc::review_lifecycle::repeated_peer_shutdown_schedules_release_both_udp_ports`，原错误 `UDP leak schedule=5, iteration=24`；该记录明确保留未解决状态，后续局部通过没有清除此失败。该导出精确组合树的 full gate 仍记为 `NOT_RUN`。本专题没有重跑此历史门禁，也不据历史失败推断当前新源码必然失败；本轮未覆盖 WebRTC 的 Core 等局部测试不能代替该门禁的关闭证据。[来源：docs/DRAFT_QUALIFICATION.md](../../docs/DRAFT_QUALIFICATION.md)

HMI 专门记录了 46 个 Hepta adapter 测试、pipecat-hepta 选定测试与 2 个实际 native 测试。不能把各阶段数字相加后宣称当前全部 feature/平台通过。native parity 最后的 LiveKit participant controls 仍标记 source-only。[来源：HMI closeout](HMI_NATIVE_CLOSEOUT_20261029.md)

## 9. 三条路线的正确边界

| 路线 | 适合承接的工作 | 不应承接的权威/承诺 | 当前决策 |
| --- | --- | --- | --- |
| 继续Rust adapter | 原thread text/stream/cursor/reconnect/dedupe；native媒体子进程；identity/epoch桥接；受控音频来源；错误与效果证据投递 | 自建thread/agent/history、重新执行原owner工具、凭本地字符串授予设备/写权限 | **立即继续**，先完成文本闭环，再真实语音；它是HMI的接缝，不应仅做演示 |
| 完整Rust Pipecat库重写 | 通用frame/pipeline、provider/transport、shared media、CLI/evals的可复用替换；按固定上游逐项差分 | 把最新所有provider/SDK/API、live、跨平台和生产Organ合并成一次完成声明 | **独立长期项目**；冻结契约/必要依赖，避免HMI被全量迁移拖住 |
| Hepta内生HMI Organ | device discovery/policy、UI/音视频呈现、来源观察、原owner准入与恢复、用户感知、shadow证据 | 第二会话数据库、第二模型决策loop、扩大只读organ trait、把capture/spoken ack当授权/effect完成 | **产品目标边界**；复用现有Rust库与原owner，不另起权威 |

所谓“内生”不要求把所有库重写进Hepta monorepo。关键是生命周期、权限、会话、证据和恢复归属既有Hepta owner；库作为可替换依赖使用。允许本地bounded presentation ledger和受控媒体asset store，但它们是缓存/证据，不能成为conversation真相来源。

## 10. 按用户要求的顺序建立验收门

| 阶段 | 必须独立保留的证明 | 失败/UNKNOWN必须如何表现 |
| --- | --- | --- |
| T1：同一thread文本提交 | 真实owner身份、原thread ID、现有authority/lease、原数据库/ledger identity；一次request ID的准入和commit；中文UTF-8与IME组合输入 | 不允许将新建bot context作为续接；签名/权限不匹配拒绝 |
| T2：增量输出与多个item | delta与committed item分开；同turn多个item/segment的顺序、边界、identity；窗口关闭再开仍来自原owner | old epoch输出不再入队；partial结果不能伪造完整成功 |
| T3：断网/回连/去重 | 断在发前、owner准入后ack前、delta中、commit后；用原request ID查回原结果；owner进程重启后同thread继续 | 不能把连接恢复当作操作失败；不确定时不换ID重试effects |
| A1：真实mic→VAD/STT→observation | 选定物理设备/系统权限、真人当前讲话、raw PCM受控引用、VAD/STT实际模型和时间、来源receipt、原owner接收 | STT错误、quota、设备断开、用户撤权保留真实原因；不输出无来源成功final |
| A2：同thread→TTS→speaker | owner committed answer、全部片段、TTS实际模型/语言、device callback、独立声学/人工可听证据、取消尾音 | callback只记submitted；无法确认播放记UNKNOWN/相应层级，不写Heard |
| V1：双向音视频 | 真摄像头/远端画面、音视频时钟/codec/路由、丢包/乱序、NAT或实际SFU、receive-only policy、各平台设备 | 小RGB fixture、图像生成、audio-only WebRTC不能通过此门 |
| R1：恢复与异常 | 热拔插、OS权限撤销、驱动错误、provider timeout/blackhole、media进程crash、owner重启、跨thread攻击/重放 | 精确request/operation/epoch归属；无隐式device/model/cloud替代 |
| P1：性能与体验 | Linux/macOS/Windows分别样本，p50/p95/p99、失败率、资源峰值/长稳、噪声/重叠说话、真人中文/多语言质量 | 只测组件就只写组件，不外推端到端和其它平台 |
| N1：neuron shadow | baseline已通过；使用owner允许的特征/观察，shadow只给建议；记录候选/decision/latency、与确定性基线差异 | 不输入原始私有音频到neuron；shadow无写权限、无实际接管效果；无数据不宣称收益 |

## 11. 可复现检查与下一轮差分测试入口

**本专题上游盘点审计**执行的是以下静态核对，JSON 保留结果与每个库存文件的 Git blob。此处不描述本轮主实现另外执行的 Core 等软件测试；它们由主实现报告记录：

```sh
git rev-list --left-right --count ec40f982205cb2c87f61094d78d163c5c2febb39...ad4a21fff16c608176163ecf044758746a8e0207
git rev-list --left-right --count ec40f982205cb2c87f61094d78d163c5c2febb39...3b7e965653dfff9d74c1c7ea934888b508d7137b
git diff --shortstat af9f9da9d7afddaa8811c0e0251b32566c980bdf 6fa9cf28886036d5d6c7dfd05ed3d6960245241a
git ls-tree -r af9f9da9d7afddaa8811c0e0251b32566c980bdf
git ls-tree -r 6fa9cf28886036d5d6c7dfd05ed3d6960245241a
```

AST核对解析`frames.py`的全部ClassDef，递归继承到Frame，先把泛型基类Subscript展开；不import官方Python包，不触发provider/model下载。Rust crate盘点来自冻结提交的Cargo.toml，不依赖root正在修改的工作区。

后续运行须沿既有项目环境，对**实际最终源码**记录完整命令/exit/source tree/日志digest；以下是有意义的聚焦入口，**本报告没有执行或宣称通过**：

| 目标 | 已有可用测试/文档入口 | 仍要加的独立端到端证据 |
| --- | --- | --- |
| runtime压力/取消/错误 | `pipecat-core/tests/{admission,drain,review_generations,terminal_stream_admission,sync_health_error_admission}.rs` | slow真实owner/UI消费与media子进程并发，队列字节/音频尾部指标 |
| nested/parallel teardown | `pipecat-pipeline/tests/{parallel,nested_shutdown,independent_held_cancel,review_lifecycle}.rs` | 与真实device/provider持有资源组合 |
| HMI身份/多片段/来源 | `pipecat-hepta/tests/{session,presentation_lifecycle,media_store,speech_segments}.rs` | 原owner/thread真实请求、中文UI、真实mic/speaker |
| provider恢复 | Realtime `review_recovery/cancel_race_runtime`；Gemini `reconnect/paired_history/async_tools`；Responses crate测试 | 真实provider/凭据、网络丢失/blackhole/late frame，不能复用fixture结果 |
| 网络与serializers | `pipecat-network-bus/tests/{codec,frame_coverage,redis_tcp,pgmq_tcp,worker_runtime}.rs`；protocol/transports测试 | 非loopbackTLS/运营商/真实Redis或PG/SFU；真实鉴权与effect对账 |
| WebRTC/视频 | `pipecat-webrtc/tests/{localhost,receive_only,review_lifecycle}.rs`；media cyclic tests；LiveKit `VALIDATION.md` | 公网ICE/TURN、真正browser、双向camera/video、同步音频 |
| 评测/指标 | `pipecat-evals/tests/{audio_to_audio,bot_audio_judge,simulation,user_images,recorded_suite}.rs`；observers `turn_tracking/reducers` | 真实用户结果、独立声学/视觉观察、丢失事件标记、不含假judge verdict |

对纯文档/库存不新写镜像测试；本轮确认路径/JSON一致即可。代码修复的具体门禁、剩余失败、最终push SHA由本轮主实现报告承接，不能事后把本报告的冻结证据更名为新代码已测。

## 12. 关键一手来源索引

以下固定链接是核心判断的一手证据；机器清单给出更完整的path/blob。滚动main、live文档与release不替代固定提交。

- [官方原始 FrameProcessor](https://github.com/pipecat-ai/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/processors/frame_processor.py)；[当前 FrameProcessor](https://github.com/pipecat-ai/pipecat/blob/6fa9cf28886036d5d6c7dfd05ed3d6960245241a/src/pipecat/processors/frame_processor.py)。
- [官方原始 base_output](https://github.com/pipecat-ai/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/transports/base_output.py)；[官方当前 source tree](https://github.com/pipecat-ai/pipecat/tree/6fa9cf28886036d5d6c7dfd05ed3d6960245241a/src/pipecat)。
- [Rust core contracts](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-core/README.md)；[frame兼容表](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-core/COMPATIBILITY.md)。
- [Rust standalone runner边界](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-runner/README.md)；[WebRTC缺口](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-webrtc/README.md)。
- [media transport](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-media-transport/README.md)；[cyclic video明确限制](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-media-transport/CYCLIC_VIDEO.md)。
- [Hepta adapter权威与来源边界](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/crates/pipecat-hepta/README.md)；[2026-10-29物理/模型与生产阻塞记录](https://github.com/TrillionniumFoundation/pipecat-rs/blob/ec40f982205cb2c87f61094d78d163c5c2febb39/rust/docs/HMI_NATIVE_CLOSEOUT_20261029.md)。

**验收状态：源码盘点与上述语义边界完成；当前生产 Organ 尚不能由已有证据宣告完成。** 继续工作应优先增加同一真实owner/thread的完整文本与物理语音链路证据，而不是扩大迁移统计、复用别的分支通过数或降低真实设备/用户感知门槛。
