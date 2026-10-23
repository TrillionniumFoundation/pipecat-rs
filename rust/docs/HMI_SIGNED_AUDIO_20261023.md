# 第三轮：签名音频来源与 owner 生命周期

原六阶段产品验收仍未全部通过。本轮在原保存的 Pipecat Codex 云环境实际完成显式签名音频 observation/device-authority profile，并补齐真实 Agentd 进程重启、崩溃后的同请求恢复和取消，以及有限软件压力/资源增长回归。测试使用受控临时 PCM 和仓库已有签名测试身份；模型是 HTTP SSE fixture，不是实体语音验收。

## 环境与已有进度

工作目录 `/workspace`，环境 `ccarenv_b64_Y2NhcmVudl85ZDBiZTQ3MjU3MjQ4MTkxOWY1N2Y5M2Y1ZDZhZWY1Mw`。Pipecat Cargo/rustc 1.99.0；Hepta 1.95.0。启动实测见 [initial-identity.json](/workspace/pipecat-hmi-audit-20261023/owner-v3/initial-identity.json)。两个 checkout 没有 `.agents/skills`，沿用 Hepta AGENTS 约束；本轮没有 Cargo 依赖变化。

继续点为 Pipecat `e12e0d66c04e2cb6191a46aa8d4c1a548ea975f9` 和 Hepta `12eae070d143f6f135e77f9e2cfc234e6108e776`。原候选 `3f616e7` / `3741d9272`、第一轮 `5357b8a` / `53f6189be`、第二轮提交均保留为祖先。没有从 main 冒充继续，没有触碰 native-parity，也没有推送、合并或部署。

前两轮 99 条检查记录、全部失败、补丁、bundle 和两个归档原样保留。第三轮记录单独放在 `/workspace/pipecat-hmi-audit-20261023/owner-v3/`。完整当前源码身份、提交、补丁和摘要以 [handoff.json](/workspace/pipecat-hmi-audit-20261023/owner-v3/handoff.json) 为准。本轮 Pipecat 运行源码不变，只更新审计文档。

## 已实现

- 新增独立 `AudioObservationBodyV1` / `SignedAudioObservationV1`。不改变文本 payload 或 qualification envelope 的含义。使用原注册 issuer 的真实 Ed25519 验证、原 AuthBus 持久 outbox 和 replay checkpoint。
- 签名覆盖 owner spawn、thread、最终文本、设备策略摘要及媒体引用。引用绑定 principal、device/organ/capture generation、logical session、sequence、采样区间、摘要、长度和 PCM 格式。采集域与文本域隔离，且不包含目标 thread，防止同一采集序号被移植到另一会话。
- `--hmi-audio-authority-file` 只读取已存在的私有 owner 策略。当前 thread allowlist、issuer/key epoch 和设备/capture 策略在媒体异步读取之后、admission/dispatch/publication 边界重新检查。代码不开设备、不申请 OS 权限、不签发凭证或真实设备 grant。
- 媒体仅接受现有私有 owner 根目录内的精确 PCM16LE 文件；验证 owner、软链/硬链、inode、字节长度、SHA-256 和半开采样区间。每段最多 1 MiB，16 kHz mono。原音频不逐帧进入事件账本、neuron 或模型输入。
- 最终文本沿原 `hepta.authbus:<delivery_id>` 进入同一 Core thread；原签名来源 metadata 留在 canonical outbox。独立 audio observation receipt 区分 queued/leased/queueAccepted/expired/quarantined，不表示模型完成或播放。
- `attach` 使用当前签名身份建立新连接而不提交模型；`reconcile` / `reconcileAudio` 只读恢复原 Core admission/原音频 observation。音频历史读取仍要求原签名、expiry 和当前设备/thread 策略；旧 spawn 只能作为已入账元数据，不能授权新 admission。
- `cancelRecovered` 先关闭本地输出，再按原 ID 只读对账，删除精确队列行或 interrupt 精确 turn。队列竞态可再做一次只读查询；不会重建 missing/cancelled 输入。ack 始终不冒充 terminal observed 或工具回滚。
- 修复 SIGKILL 后遗留 HMI socket 导致可选组件被隔离的问题：只清理 owner 私有、探测已拒绝连接、inode 未变的 socket；拒绝 live socket、symlink、regular file 或漂移路径。RuntimeTasks 的隔离日志同时保留具体原因。

接口、策略字段与边界见 [AUDIO_OBSERVATIONS.md](/workspace/hepta-private-ci/codex-rs/hepta-agentd/AUDIO_OBSERVATIONS.md)。生产凭据和设备配置不会由这些 API 自动生成。

## 实际测试终态

| 验证 | 结果与范围 | 原始日志 |
| --- | --- | --- |
| 原 owner 产品回归 | 最终 10/10 通过，0 跳过：原 AuthBus 2、原 HMI 2、新音频/生命周期/压力 6。真实 Agentd 和其进程内 App Server、私有 UDS、签名、SQLite 和 Core；PCM/模型端为 fixture | [product](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-v3-final-product-regression.log) |
| 音频身份与来源 | 错误签名、principal、device/organ/capture generation、采样区间、摘要、文本域签名、软链/硬链均拒绝。合法输入只有 1 条 canonical observation 和 1 次模型请求；实际重启后 replay 仍拒绝、原 observation 可只读恢复 | 同上 |
| 活动撤权 | 模型请求已开始后撤销设备策略，UI 关闭并保留原因，未发布晚到文字；后续原输入和新输入均拒绝 | 同上 |
| 完整进程替换 | 正常 restart 和受 supervisor 控制的强制 crash 均断言实际 PID/spawn 改变。原活动 turn 冷恢复为 interrupted；另一持久队列行按原 ID 取消并保持 cancelled；总模型请求仍为 1 | [强化队列回归](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-restarted-queued-input-cancellation.log) 及最终 product |
| adapter 全包 | 最终 42/42，0 跳过，包括原双向媒体/epoch/失败上下文和取消 fence | [adapter](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-v3-adapter-complete-regression.log) |
| RuntimeTasks | 44/44 定向通过，另 125 项筛除；覆盖隔离、失败 latch、取消、retirement、清理和 writer generation | [runtime tasks](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-v3-runtime-task-failure-regression.log) |
| 软件压力 | 原默认 manifest 预算下，一个活动请求期间拒绝 32 个竞争输入；拒绝 65537 字节帧后仍接受合法新会话，模型只调用一次 | 最终 product |
| adapter 严格 Clippy | all-targets / no-deps / `-D warnings` 退出 0；依赖的既有 warning 保留 | [strict adapter](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-v3-adapter-strict-clippy.log) |

资源回归共 40 次签名音频观察/独立连接周期，前 8 次 warmup，后 32 次测量；每段 3200 字节。最终样本 RSS 为 256884736 → 268128256 字节，FD 97 → 99，线程 43 → 44，均在事先定义的 +32 MiB/+4 FD/+4 线程范围内。40 条 outbox 记录与 128000 字节媒体留存均符合预期；outbox 继续使用原 4096 行硬上限，媒体生命周期/配额由原媒体 owner 管理。

32 个 software send-to-text-terminal 样本 p50/p95/p99 为 415732 / 591903 / 592694 微秒，计时从 PCM 准备和 UI 连接之后开始。它们不是 microphone-to-speaker 延迟、真实模型性能或长期稳定性验收。原始样本和各 checkpoint 资源量保留在最终 product 日志与交接 JSON。

资源采样仅针对实际 Agentd/内嵌 App Server 进程，不包含测试进程中的 HTTP fixture、Cargo 或尚未运行的 STT/TTS/设备进程。

## 真实失败、lint 与格式

首次完整 crash 测试实际失败并经仓库默认重试仍失败。新 PID 已就绪但旧 socket 阻止 HMI 重绑；诊断见 [首次失败](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-full-process-lifecycle-first.log)、[隔离诊断](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-crash-socket-diagnosis.log)。修复过程中一次 `Result<PathBuf, io::Error>` 比较错误导致 E0369，修正后通过；没有延长 deadline 或删断言。

Clippy 发现测试中两处直接创建 SQLx pool 违反仓库 shim 约束，已改用 codex-state 只读 pool 并完成最终回归。标准 Clippy 诊断中没有新增源码诊断，但这不替代严格门禁。

本轮 Agentd `-D clippy::all` 仍真实退出 101、8 项既有错误。五个相关模块与原候选字节一致：automation_effect_host:58；cognitive_context:105/507；cognitive_retrieval_learning:52；intelligence_product:584/593/722；plasticity_host:704。这些 enum/参数 API 不在本次 HMI 输入路径中，未扩大重构或 suppress。精确源码摘要、模块规模与范围见 [baseline-lint-scope.json](/workspace/pipecat-hmi-audit-20261023/owner-v3/baseline-lint-scope.json)，本轮终态见 [strict Agentd](/workspace/pipecat-hmi-audit-20261023/owner-v3/owner-v3-agentd-strict-clippy.log)。adapter strict、scoped fix、格式检查与已测试源码的规范化等价证据以 handoff 中的实际退出码为准；未把 standard Clippy 退出 0 当作严格门禁通过。

测试之后按仓库要求运行 scoped fix/fmt；不反复重跑已通过的 native/provider 矩阵。没有调整 sandbox、真实设备或部署权限，没有生成持久私钥。

最终两包 scoped fix 退出 0，既有 warning 未隐藏。仓库 just fmt 因源码已提交而为空操作；随后按相同 toolchain/recipe 显式格式化并检查全部 20 个变更 Rust 文件，退出 0。稳定版 rustfmt 对 nightly-only imports_granularity 参数的警告保留，未更换工具链。规范化比较确认最终文件与已测试源码锚点 `c1d5f6e7c` 完全一致，见 [格式等价证明](/workspace/pipecat-hmi-audit-20261023/owner-v3/final-format-equivalence.json)；按仓库要求，fix/fmt 后未再重跑测试。

## 未运行、阻塞与最小剩余输入

- Whisper 原 Hugging Face CONNECT 403 保持 denied，未重试、未换镜像/机器。仍需获准的可信多语言 Whisper artifact/摘要，以及可信 Piper manifest、voice/模型和运行依赖。
- 本环境没有 `/dev/snd`。中文/目标语言真实语音、噪声/扬声器回声、实体抢话、拔插、OS 设备权限撤销和实际播放仍未验；文件及协议测试不能升级为真实设备通过。
- 真实 STT/TTS 进程崩溃、完整语音压力、长时资源增长和真实端到端分位延迟仍未验收。软件 owner 进程重启/崩溃、同请求恢复、防重、取消与有限压力已完成，不能继续将它们泛称“缺设备所以未测”。
- 实际启用需要已有 fleet/AgentId/generation、已注册独立签名 producer、原 trust/checkpoint、目标 thread，以及用户采用的平台/设备身份与当前权限来源、owner 预建媒体 root/quota/持久 capture generation。新增策略由原 owner 提供，程序不替用户授予设备权限。
- Agentd 严格 lint 的 8 项独立基线错误仍阻挡“整包严格 lint 全绿”；修复需要审阅对应已有 API，不应混入本轮音频来源接线。
- neuron shadow 与学习策略仍未接入，等确定性真实语音路径和原故障/性能验收完成后再评估。权限、紧急停止和设备隐私边界不进入可学习控制。

下一步在用户实际采用的、获准的运行配置中连接原媒体 producer/签名方与本 profile，完成原范围真实语音、模型故障、设备和长期性能验收。未采用 WebRTC，不扩展商业 provider 全量 parity 门槛。所有不可在云端证明的部分仍保持未验收。
