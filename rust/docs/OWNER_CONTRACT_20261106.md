# Hepta owner / HMI 契约审计（2026-11-06）

## 结论与审计范围

继续建设的边界是 **Hepta 原有 owner 与 conversation + Rust 媒体 Core + 由原 owner 组合的 HMI Organ**。Rust 负责有界媒体执行、设备状态、取消和效果回报；原 owner 继续决定身份、权限、输入 admission、会话历史、模型及工具执行。不能把媒体进程的内存状态变成第二套权威会话库，也不能通过扩展只读 organ trait 获得设备、网络或观察写权限。

现有代码已具有同一 thread 的签名文本入口、canonical queue 去重与恢复、增量输出、取消，以及泛型媒体/来源观察接口。**实际 Agentd 注册的 HMI port 仍是 `TextPort + NoAudio`；不存在由这些代码自动建立的生产麦克风到扬声器链路。**本轮连接绑定、命令解码和进程内请求准入修补不能代替这项生产接线，更不能代替真实 owner、物理设备、权限、跨平台恢复和用户感知证据。[实际注册代码][text-port]

本报告为代码与已有证据的审计，不代表本轮新执行了物理设备验收。

| 审计对象 | 固定版本 | 用途 |
| --- | --- | --- |
| `hepta-private-ci`，`work/hmi-closeout-20261029` | [`7bbdd715957e72274bc13209c4a619913e880129`](https://github.com/TrillionniumFoundation/hepta-private-ci/commit/7bbdd715957e72274bc13209c4a619913e880129) | 本文 Hepta 源码判定的依据 |
| `hepta-private-ci`，`audit/hmi-jsonl-cloud-20261023` | [`965e1b26217378bbe7378aefd5c9c8923eb1a6e7`](https://github.com/TrillionniumFoundation/hepta-private-ci/commit/965e1b26217378bbe7378aefd5c9c8923eb1a6e7) | 已有 cloud audit 的基线，不能与别的 Hepta 工作线资格混用 |
| `pipecat-rs`，本轮修改前的 `work/hmi-closeout-20261029` | [`ec40f982205cb2c87f61094d78d163c5c2febb39`](https://github.com/TrillionniumFoundation/pipecat-rs/commit/ec40f982205cb2c87f61094d78d163c5c2febb39) | 原媒体实现与历史设备证据基线 |
| 本轮 Rust wire boundary | [media_command.rs](../crates/pipecat-hepta/src/media_command.rs)、[endpoint](../crates/pipecat-hepta/examples/local_speech.rs)、[回归测试](../crates/pipecat-hepta/tests/media_command.rs) | 与本文同一可审查分支的增量；历史设备证据不会因此自动变成本轮设备资格 |

## 1. 唯一权威与只读 Organ 边界

| 能力 | 权威所在位置 | Rust 媒体 Core 的职责 |
| --- | --- | --- |
| thread / conversation / canonical message | 现有 Agentd、App Server、AuthBus evidence 与 Core queue | 接收并核对原 owner 给出的标识，不创建另一套会话事实 |
| 当前 issuer、key epoch、thread allowlist、device / organ generation | 现有 owner 的 live authority 检查 | 固定受托 stream 与连接；标签相同不能冒充授权 |
| 最终用户观察与来源 receipt | 原 sourced-observation ingress / 签名音频入口 | 保留原始 PCM 的受控引用、返回识别结果与来源，等待原 owner 验证 |
| turn、output epoch、取消及工具效果 | 原 App Server / turn owner | 立即停止旧本地输出，回报取消请求与不确定状态，不宣称撤销已提交工具效果 |
| capture、VAD/STT、TTS、设备提交 | 原 owner 授权的媒体进程与设备 port | 有界执行、保留错误、按当前 scope 阻止过期音频 |
| 只读 organ handler | `TrustedReadOnlyOrganV1` | 保持同步、无 I/O、无 capability、无 spawn、无模型/外部效果调用 |

`TrustedReadOnlyOrganV1` 明确不是 sandbox。它是经过审查的只读宿主契约，不能添加 `submit`、`capture`、`speak` 或文件/网络写操作来绕过已有 owner。`OrganStateMigrationV1` 只是由状态所有者提供的有界交接 callback，也不把数据库、文件或凭据访问交给只读宿主。[只读契约][readonly-organ]

媒体命令集合与只读 organ trait 分属不同边界。HMI 的外部效果必须由现有 owner 的受限 port 承担；仅仅实现一个 trait、填满配置标签或启动一个 Rust 二进制，不构成生产注册或权限证明。[owner ports][owner-types]

## 2. 同一 thread 的真实文本入口

现有启用点是 `AgentdConfig::with_hmi_text()` / `--hmi-text`，端点为原 `run_root/hmi-text.sock`。它从原 Agentd identity 取得原 App Server UDS，核对实际 `codex_home`，读取已经存在的 thread，并把实际 thread ID、App Server session ID 和版本绑定到 `HmiBindingV1`。逻辑会话使用 `thread:<existing-thread-id>`；不创建第二个 conversation owner。[会话实现][text-session] [部署材料契约][owner-template]

JSONL 输入使用严格字段解析：

| 命令 | 数据 | 行为 |
| --- | --- | --- |
| `attach` | 原 `AuthBusTextIngress` | 认证并绑定，不 admission 新请求 |
| `submit` | 原 `AuthBusTextIngress` | 经既有签名 AuthBus ingress、evidence 与 relay 送入同一 Core queue |
| `submitAudio` | 原 `SignedAudioObservationV1` | 验证原签名、当前 policy、受控 PCM 来源，再进入原观察/文本 admission 链 |
| `reconcileAudio` | 原 `SignedAudioObservationV1` | 在当前权限下读原观察，不插入新的 delivery |
| `reconcile` | 原 `deliveryId` 与确切 `text` | 查原 canonical queue/outcome，不换新 ID 重发 |
| `cancelRecovered` | 原 `deliveryId` 与确切 `text` | 先退役本地输出，再请求取消原 queue entry / turn |
| `interrupt` / `reconnect` / `stop` | 无替代会话标识 | 按已有 owner 状态取消、重新附着或终止 |

生产 AuthBus canonical client message ID 是 `hepta.authbus:<delivery_id>`。新提交前查询原 evidence；已有 delivery 不重新打开 presentation gate。新 delivery 的 admission fence 由原 owner 创建，结果不明时保留原 ID 做 reconcile，不把 timeout 当作“没写入”。adapter 的通用 `hepta.hmi.message.v1` helper 不能代替签名 AuthBus 入口。[text session][text-session] [adapter requests][hmi-requests]

签名音频的 `committedAudioObservation` 只证明原观察已经提交；`admissionReady:false` 明确表示后续 readiness 失败。它不证明 Core admission、模型完成、扬声器播放或工具效果。取消响应同样保留 `terminalObserved:false`，不能把“请求取消成功”升级成“已观察到终止”。[部署与恢复语义][owner-template]

### 增量输出与重连的实际能力

`hmi_events.rs` 只认领同一 thread、匹配 canonical client message ID 的 turn；按 item ID 聚合 `AgentMessageDelta` 与最终 AgentMessage，并区别 completed、failed、interrupted、unknown。事件 lag、协议/连接错误或需要真实 owner 批准的 server request 会关闭输出或 fence。[事件路由][hmi-events]

`HmiConnectionV1::reconnect` 撤销所有旧 handles，有界关闭旧 client，保留原 binding 重新附着同一 thread，并让输出保持关闭，直到观察到新 admission。当前 HMI 协议 **没有可供 UI 续传 token deltas 的 replay cursor**。canonical submission/outcome reconcile 和“逐 token 无损恢复”是不同能力；不能用前者宣称后者已完成。补齐续传应读取同一 owner 的消息/事件证据，不建立新的权威会话存储。[连接实现][hmi-connection]

## 3. 媒体进程必须共享原 owner 的连接 incarnation

连接值的真实来源链是：

`RemoteAppServerClient::connection_id()` → `HmiConnectionV1` → `HmiRequestsV1::connection_id()` → `HmiJsonlMediaV1::bind(...)`。

`HmiJsonlMediaV1::bind` 的参数是已由 owner 创建且授权的 reader/writer、同一个非零 connection ID、capture generation 与有界 timeout。该 ID 不能从收到的 media JSON 或首个 scope 学习。发送 `turn` / `speak` 前，Hepta adapter 自身也再次核对 scope connection。[JSONL owner adapter][owner-media]

本轮 endpoint 的 argv 契约是：

```text
local_speech CONFIG_JSON OWNER_CONNECTION_ID
```

原 owner 的实现方应按以下顺序接线：

1. 使用原 identity / grants 连接已有 thread，取得当前 `HmiRequestsV1`。
2. 读取其 `connection_id()`；同一个十进制 `u64` 同时传给媒体 argv 与 `HmiJsonlMediaV1::bind`，不可各自生成或用测试常量代替。
3. 使用原 owner 授权的 config、device 与 capture generation 创建私有管道和媒体子进程。
4. 将现有 authority、media、observations ports 交给 `HmiOwnerBridgeV1`；持续运行 owner reader 与事件路由。
5. 连接替换时先 fence 旧 admission、停止并回收旧进程和管道，再建立新绑定；不把旧 scope 重标为新 connection，不重播未知效果。

argv 缺失、零或不可表示的 connection ID 会在读取配置、加载模型或打开设备之前失败；完整 stream 校验也在 `LocalSpeech::load` 前执行。相同 connection 数字仍不是身份认证或 OS permission。它只防止在已经由原 owner 控制的媒体边界内混用连接。[本轮 endpoint](../crates/pipecat-hepta/examples/local_speech.rs)

App Server 的 connection ID 来自进程内 `AtomicU64`，不是跨 owner 重启的持久全球唯一 ID。跨重启仍依赖真实 owner、授权管道、device/organ/capture generation 和既有恢复证据。[connection mint][remote-client]

## 4. request ID、流控与取消安全

### 发送端允许请求 ID 乱序

固定 Hepta 实现使用 `AtomicU64::new(1)` 和 `checked_add` 分配 `request_id`，但分配发生在 writer semaphore **之前**。并发调用可先写出 ID 2 再写出 ID 1；Busy 或前置失败也会消耗 ID。因此 ID 是 ack correlation，不是执行顺序、turn sequence 或持久效果 receipt。[owner command()][owner-media]

pending 上限为 8，不等于数值乱序窗口为 8：ID 在 Busy 判断前分配，大量被拒调用仍可能产生大缺口。接收端不能用 `id > high_water`，也不能据此臆定一个很小的数值窗口。

### 本轮有界命令准入

`MediaConnectionBinding` 固定完整 stream 和非零 connection。`turn` / `speak` 在丢失 connection 字段的 `OutputIdentity` 转换前校验准确 incarnation；`verify_source` / `release_source` 校验完整 stream。所有相关命令必须有非零 `request_id`。原 owner 的输出 utterance sequence 也要求从 1 开始，不能与可乱序的 request ID 混淆。[owner sequence 检查][hmi-requests] [本轮 gate](../crates/pipecat-hepta/src/media_command.rs)

`MediaCommandGate` 使用有界、无淘汰的内存 ID 集合，在任何外部效果之前准入。允许乱序和大缺口；同一个 ID 不能改 operation 后重用。失败或效果未知的 ID 不释放，不自动重试，也不伪造“已经成功”的 ack。endpoint 在 gate 错误时进入统一退役/清理路径。`interrupt` / `shutdown` 是只能通过原可信 pipe 到达的无相关 ID 停止命令，不允许 admission 新输出。[本轮 gate](../crates/pipecat-hepta/src/media_command.rs)

endpoint 的预算为 **每个媒体连接进程 65,536 个相关命令**，达到上限关闭该连接，不静默淘汰旧 ID。它是明确的生命周期预算，不是无限在线承诺。长期运行必须由原 owner 负责安全轮换；当前没有生产媒体 launcher，因此不能宣称达到预算后的自动重建已经完成。该集合不存 conversation、输入正文或工具结果，不参与跨进程权威去重。

### JSONL 与终态

`MediaCommandDecoder` 把半行字节保留在 caller 持有的状态中，每次最多消费一行，包含换行的大小上限为 65,536 字节。`fill_buf().await` 之后的 `push` 与 `consume` 之间无 await；切换到 VAD、取消或其他 select 分支不会丢失已读半行。中文、emoji 与转义换行可跨读取边界。非法 JSON、未知字段、超长行及不完整 EOF 都终止 decoder，不扫描到下一行继续执行。[decoder 与测试](../crates/pipecat-hepta/src/media_command.rs) [回归用例](../crates/pipecat-hepta/tests/media_command.rs)

媒体事件队列有界，当前 endpoint 使用 32 项 `try_send`；stdout 单次写有 2 秒期限。接收方关闭/过载、解码错误、连接不匹配或重复 request 会进入停止旧输出、取消输入 pipeline 和清理 native owner 的路径，保留错误。`media_terminal` 仍需区分主错误、local stop、input、native cleanup 与 source verification 的结果。[endpoint](../crates/pipecat-hepta/examples/local_speech.rs)

**这些局部期限不是所有故障都已证明具有总清理时限。**当前 cleanup 会等待 source `spawn_blocking` 的 `JoinHandle`；阻塞文件 I/O 本身没有由这里提供的截止时间。owner 的 command deadline 能 fence 请求，并不等于证明子进程已退出。生产 supervisor 仍需独立的退出/强制回收证据，不能用 JSONL 单元测试或 stdout timeout 代替。

## 5. 原始音频、观察来源与输出回报

`HmiCapturedAudioRefV1` 绑定 principal/device/device generation/organ/organ generation/logical session、capture generation、sequence、asset ID、SHA-256、字节长度、采样率、声道数与 sample 区间。引用的原始 PCM 必须在识别器添加补零前保留；digest 证明字节相同，不证明说话人、许可或听感。[owner source 类型][owner-types] [本仓库 MediaStore](../crates/pipecat-hepta/src/media_store.rs)

`HmiOwnerTranscriptV1.connection_id` 由 owner reader 针对其拥有的媒体进程盖章。观察 digest 绑定域 `hepta.hmi.source-observation.v1`、完整 source 与文本。`HmiOwnerObservationsV1` 必须核对真实 grant、canonical client message ID 和确切 digest；`ReconcileOnly` 不得插入观察。可信 receipt 来自原 ingress，不能接受媒体/UI 自报“写入成功”作为权威记录。[owner ports 与 transcript][owner-types]

当前私有媒体 profile 有 Unix owner/权限与目录约束、配额和 release 流程。Windows ACL 不因 Rust 代码可编译而被视作已验证；需独立实现和平台证据。[owner 部署契约][owner-template]

完成态 TTS 由 `take_completed_speech` 在原 owner 内认领：仅 completed turn 可生成一次 speech，按观察顺序组合多个 AgentMessage item；partial、failed、unknown、interrupted 不进入 TTS。当前 item 上限 32、speech 上限 16 KiB，超限明确返回错误，不静默截断。媒体分句与 PCM 分块是执行策略，不是第二次会话 admission。[completed speech][hmi-requests]

`Generated`、`Queued`、`Submitted`、`DeviceObservedPlayback`、`Interrupted`、`Unknown` 必须保留差异。CPAL / 设备 buffer 提交最多支持 `Submitted`；不能把它写成用户已听见。`DeviceObservedPlayback` 需要独立可观察证据。[presentation 契约][owner-types]

## 6. 生产 Organ 缺口与验收证据

实际 Agentd 的 `bridge` 当前调用 `attach_text`，`TextPort.capture_generation()` 为 0，`speak` 和 `verify_source` 明确拒绝音频职责，`NoAudio.observe_final` 也拒绝。`submitAudio` 已有签名观察入口，但它不会自动启动媒体子进程或把 `TextPort` 变成扬声器 owner。生产组合必须使用原有真实 authority、受控媒体管道和 sourced-observation ingress，不能用 permissive mock 冒充。[实际注册实现][text-port]

| 验收层 | 已有证据所支持的范围 | 尚不能宣称的结果 |
| --- | --- | --- |
| 文本提交 / 增量 / canonical 去重 | 实际 Agentd、App Server、签名、SQLite/Core 的进程 fixture；模型 SSE 为合成 fixture | 当前生产 owner 已接入，或 token deltas 跨重连无损续传 |
| 真实麦克风 | 历史 Linux ALC897 捕获片段及采样统计 | 可懂的真实发言已经过 STT、原 owner、同 thread 与扬声器全链路 |
| 中文 STT/TTS | 历史真实模型处理合成清洁/噪声中文输入、长输出与 interruption fixture | 物理中文说话者、真实回声/环境噪声和用户听感达到生产要求 |
| 带来源的观察 | 签名、policy、原始 PCM 引用、receipt / reconcile 代码和 fixture | 生产 device issuer、真实权限和物理来源已组合验证 |
| 多个输出片段 | owner 多 item 聚合、媒体分句、分块提交 | 所有实时多模态片段、音视频双向同步与恢复均已合格 |
| 取消 / 回连 / 去重 | scope / epoch / request gate，原 delivery reconcile，旧输出 fence | 工具效果可撤销、未知播放可重播，或生产媒体进程自动轮换已完成 |
| 平台与性能 | 历史 Linux 单项与 cloud 组件记录 | macOS/Windows/Linux 的同负载端到端时延、热插拔、权限撤销、恢复及听感均有独立证明 |
| neuron shadow | 确定性的安全边界与预留集成方向 | 已接入生产 shadow、学习控制已通过验收，或隐私/权限/紧急停止可交给学习策略 |

历史证据见 [cloud audit](HMI_CLOUD_AUDIT_20261023.md)、[signed audio audit](HMI_SIGNED_AUDIO_20261023.md)、[native closeout](HMI_NATIVE_CLOSEOUT_20261029.md)。native closeout 记录的物理设备当时没有运行 Agentd owner；这些分立证据不能拼写成一次已经完成的生产 E2E。

下一阶段需要由原 Supervisor/owner 提供并验证：现有 issuer 与 replay witness、精确 thread allowlist、真实 device/organ/capture generations、实际 OS grants、受控媒体目录与模型 provenance；随后把媒体 child、`HmiJsonlMediaV1`、原 authority 和 observation ingress 接入同一个 owner 生命周期。按同一原 thread 记录麦克风采样、VAD、STT、观察 receipt、Core admission、模型首输出/完成、TTS、设备提交、可观察播放与用户确认的时间线；故障恢复保留原 ID 与未知效果，不靠新会话或重放掩盖失败。[材料与验收约束][owner-template]

[readonly-organ]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-control-plane/src/organ_runtime.rs
[owner-types]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-codex-adapter/src/hmi_owner_types.rs
[text-port]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/src/hmi_text.rs
[text-session]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/src/hmi_text_session.rs
[hmi-connection]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-codex-adapter/src/hmi.rs
[hmi-requests]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-codex-adapter/src/hmi_requests.rs
[hmi-events]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-codex-adapter/src/hmi_events.rs
[owner-media]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-codex-adapter/src/hmi_media_jsonl.rs
[remote-client]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/app-server-client/src/remote.rs
[owner-template]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/config-templates/hmi/README.md
