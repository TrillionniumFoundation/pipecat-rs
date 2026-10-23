# Pipecat / Hepta HMI 云端续审：2026-10-23

原任务的完整产品验收仍未通过。指定的已保存 Codex 云环境已实际运行 Cargo、原生模型与协议测试；owner 双向路由和采集音频来源绑定已实现；第二轮进一步把文本 coordinator 注册进实际 Agentd owner，并通过真实进程回归。生产用户身份、完整语音注册及真实模型/设备链尚未验收。

## 身份、分支与工具

环境为 `ccarenv_b64_Y2NhcmVudl85ZDBiZTQ3MjU3MjQ4MTkxOWY1N2Y5M2Y1ZDZhZWY1Mw`，工作目录 `/workspace`，Linux x86_64。Pipecat 使用 Cargo/rustc 1.99.0；Hepta 使用仓库固定的 1.95.0。工具激活脚本为 `/workspace/.pipecat-env/activate.sh`。没有使用 Dot 临时工作区、用户电脑或 GitHub Actions 代替执行。

| 仓库 | 已核验原分支与候选 | 本地续审分支 |
| --- | --- | --- |
| `/workspace/pipecat-rs` | `work/hepta-hmi-20261023` → [3f616e743619277e64dff499d870c826b58a89c2](https://github.com/TrillionniumFoundation/pipecat-rs/commit/3f616e743619277e64dff499d870c826b58a89c2) | `audit/hepta-hmi-cloud-20261023` |
| `/workspace/hepta-private-ci` | `work/hmi-jsonl-20261023` → [3741d927273a3acecda02be39c41998834c1e732](https://github.com/TrillionniumFoundation/hepta-private-ci/commit/3741d927273a3acecda02be39c41998834c1e732) | `audit/hmi-jsonl-cloud-20261023` |

启动时 Pipecat 的干净 `work` / 缓存 `origin/main` 位于 `11c9927`，已保留，没有将它冒充候选继续。另一个 `work/native-parity-20261012` 重写任务未检出、未改写、未推送。所有修复只保存在本地续审提交和补丁，没有推送、合并、部署、激活或改变权限。

两个 checkout 均不存在 `.agents/skills`；已检查 Hepta `AGENTS.md` 和 `.codex/skills`。Hepta 测试均走 `just test`，没有修改 sandbox 行为，也没有扩大只读 organ/gateway 的权限。原有静态检查及 23 个远端文件核对进度保留。

身份实测见 [cloud-identity.log](/workspace/pipecat-hmi-audit-20261023/cloud-identity.log)，工具安装来源、摘要、拒绝及环境边界见 [environment.json](/workspace/pipecat-hmi-audit-20261023/environment.json) 和 [tool-provenance.log](/workspace/pipecat-hmi-audit-20261023/tool-provenance.log)。所有命令的原始输出、退出码和耗时见 [results.json](/workspace/pipecat-hmi-audit-20261023/results.json)。最终提交、补丁、bundle 与摘要见 [handoff.json](/workspace/pipecat-hmi-audit-20261023/handoff.json)；原 23 文件的候选 blob 与当前摘要见 [candidate-scope.json](/workspace/pipecat-hmi-audit-20261023/candidate-scope.json)。

## 本次实际实现

第一阶段先修复候选的四处编译错误：Pipecat 两处 E0308 锁守卫借用，Hepta 两处 E0695 跨标签块的 continue。该阶段的规范化等价校验只证明编译修复和机械格式修正，见 [phase-1 校验](/workspace/pipecat-hmi-audit-20261023/source-change-equivalence-recipe.log)。它不代表下面新增功能仍只是格式修正。初始辅助校验器遗漏 rustfmt recipe 参数的两次失败均保留。

后续源码工作包括：

- Hepta 保留整个成功 turn 的有序文本，以一次有上限的 speech claim 聚合多个输出 item。失败、未知、已打断或重复 claim 均不播报；超过 4096 UTF-8 字节返回明确失败并保留完整 terminal text，不截断、不重跑模型/工具。
- `HmiOwnerBridgeV1` 实现双向 coordinator：核验连接/主体/设备与 organ generation，核验实际媒体来源，取得既有 observation ingress 的匹配 receipt，再复核当前授权，进入原 Core thread。结果始终保留输入、来源、receipt、admission、失败阶段与原因。撤权发生在 observation 入账之后时，保留已提交 receipt，关闭 capture，不能虚构回滚。
- `HmiJsonlMediaV1` 实现实际字节流 transport 和相关回执：turn、speak、精确 epoch retirement、来源核验、显式媒体释放、capture 关闭。8 个 pending 请求、64 KiB 行长和 ack deadline 为硬上限。半帧取消会丢弃其 writer 并唤醒 reader；错误/丢失 ack 不能升级为本地停止成功。并发用 permit 控制，数据锁不跨 I/O 等待。
- Whisper 在既有受拥有的原生推理 worker 内，成功解码后、释放 admission permit 前保留精确采集 PCM 前缀，不包含人为追加的 decoder padding。取消或存储失败不产生 sourced final。默认未配置路径的序列化输出保持不变；Rust `Transcript` 新增可选 `audio_source` 字段，外部用 struct literal 构造者需要补充该字段。
- `MediaStore` 绑定 scope、单调 capture generation、sequence、SHA-256、采样范围及 16 kHz mono PCM16LE。现有私有目录由 owner 提供；拒绝 symlink、路径逃逸、覆盖、generation 重用。跨重启仍计入 quota；只有显式确认释放才回收容量。失败写入保留占用并失败关闭，不自动驱逐音频。目录/祖先必须由既有 owner 独占且保持不变；当前私有目录实现仅接受 Unix 语义。
- 媒体示例配置 store 后发 `source_transcript`，未配置时的 `unbound_transcript` 仅供显示。来源核验/释放在受拥有、退出时 join 的 blocking 任务执行，主循环仍可处理本地抢话。原始音频不逐帧写入 neuron、会话库或持久事件账本。
- 本地抢话先关闭输出 admission，再异步请求原 Hepta turn interrupt。native stop 与 owner echo 使用同一 epoch floor；旧 reader/turn 不能重开或停止新输出。重复停止保留未知/设备错误，且本地失败不吞掉远端 interrupt。工具效果仍沿原 operation 对账。
- queued ack、submitted、interrupted 和 unknown 分开保留。CPAL 提交只证明写入缓冲，native decoder 不产生 device-observed-playback 或 heard。`media_terminal` 在 capture、存储和 engine cleanup join 后给出完整错误与计数；writer 按 deadline 排空 terminal。OS/native 卡死仍需要既有 supervisor 收敛，不能宣称硬实时。

Pipecat 运行源码已保存于本地 `354d116`（后续仅类型别名格式/文档收尾），Hepta 运行源码已保存于本地 `f95f0bce9`；最终完整 HEAD 和分阶段提交列表以 handoff 为准。按实际差异拆成编译修复、来源回调、受控存储、媒体控制、语音聚合、媒体传输及 owner 路由/测试的小提交，每阶段可独立审阅。

对应入口为 [owner coordinator](/workspace/hepta-private-ci/codex-rs/hepta-codex-adapter/src/hmi_owner.rs)、[media link](/workspace/hepta-private-ci/codex-rs/hepta-codex-adapter/src/hmi_media_jsonl.rs)、[source store](/workspace/pipecat-rs/rust/crates/pipecat-hepta/src/media_store.rs) 及 [Whisper source callback](/workspace/pipecat-rs/rust/crates/pipecat-whisper/src/source.rs)。

## 已运行、可复核的验证

| 检查 | 真实终态与边界 | 原始证据 |
| --- | --- | --- |
| Runner 默认特性 | 111 次测试执行通过；含复用集成用例，不当作 111 个设备场景。失败摘要、原因、计数和清理上下文回归已运行 | [runner](/workspace/pipecat-hmi-audit-20261023/pipecat-runner-baseline.log) |
| 实际 Silero / ONNX Runtime | 36 通过，使用 bundled 模型进行原生推理；语音/静音 fixture，不是麦克风验收 | [Silero](/workspace/pipecat-hmi-audit-20261023/pipecat-silero-native.log) |
| 采用的 HMI / CPAL / Whisper / Piper 组件 | 96 通过、26 忽略。被忽略的真实模型用例仍未运行 | [native components](/workspace/pipecat-hmi-audit-20261023/pipecat-native-components-linked.log) |
| 新 Pipecat sourced path | 17 通过（2 gate、4 实际文件存储、11 session）；全部 local-speech targets 构建。example harness 的 0 tests 不算设备通过 | [sourced state](/workspace/pipecat-hmi-audit-20261023/pipecat-final-sourced-state-tests.log) |
| Whisper 精确来源回调 | 3 通过，验证原始前缀、不含 padding、取消/失败不发 sourced final；推理结果是 fixture | [source callback](/workspace/pipecat-hmi-audit-20261023/whisper-source-callback-rebuilt-tests.log) |
| Hepta adapter | 最终全包 41/41、0 跳过；最新 HMI 定向 20/20（新增半帧取消），覆盖中文聚合、防重、撤权、晚到设备失败、旧连接、ack/行长/deadline 与完整双向协议回路 | [final regression](/workspace/pipecat-hmi-audit-20261023/hepta-complete-final-regression-fixed-state-name.log)、[HMI 20](/workspace/pipecat-hmi-audit-20261023/hepta-owner-notify-regression.log) |
| 实际 Core 持久队列 | 1 通过、1014 个其他用例被筛除。实际 App Server request processor、LocalQueueStore、dispatcher 与 rollout，断言重复 admission 只产生 1 条用户记录和 1 次 /responses；模型 HTTP SSE 是 fixture | [Core](/workspace/pipecat-hmi-audit-20261023/core-queue-payload-binding-dev-small.log) |
| Pipecat 严格 Clippy / 格式 | 启用 local-speech，全部目标，`-D warnings` 退出 0；fmt 检查退出 0 | [Clippy](/workspace/pipecat-hmi-audit-20261023/pipecat-sourced-path-clippy-final.log)、[fmt](/workspace/pipecat-hmi-audit-20261023/pipecat-source-format-check-final.log) |
| Hepta Clippy / 格式 / Bazel lock | 初次 just fix 的新增警告随后修复；最终 strict Clippy `--all-targets --no-deps -- -D warnings`、just fmt 检查均退出 0。上游 memory 两个既有警告保留。Bazel batch 模块锁更新退出 0、无漂移；不是 Bazel 全套构建 | [Clippy](/workspace/pipecat-hmi-audit-20261023/hepta-owner-strict-clippy.log)、[fmt](/workspace/pipecat-hmi-audit-20261023/hepta-owner-source-format-check.log)、[Bazel](/workspace/pipecat-hmi-audit-20261023/hepta-owner-bazel-lock-batch.log) |

真实 Silero 模型 SHA-256 为 `597d30b3ec076608d059477bb14cfeffdf951bf5cae370d38f65d33bbfe82004`；实际 ORT 1.21.0 库 SHA-256 为 `a0df84fae0f09d9eca3d6919f685ba435f1ae6cb628e128b69bcf2a2b965504c`。文件/协议 fixture 不证明真实语音、生产认证或人耳听见。测试按改动定向执行，未反复重跑已通过的 Runner/Silero/商业 provider 全矩阵。

## 保留的真实失败与修复边界

- 原候选确实编译失败，见 [Pipecat E0308](/workspace/pipecat-hmi-audit-20261023/pipecat-local-speech-with-deps.log) 与 [Hepta E0695](/workspace/pipecat-hmi-audit-20261023/hepta-check-baseline.log)。开发环境起初缺少 ALSA 开发元数据/链接；官方 Debian 包仅解压到工作区 sysroot 并连接实际系统运行库，没有配置 ALSA null 或虚拟设备。
- Core 默认 debug 构建真实报 `No space left on device`，测试未运行。待命令退出后仅清理本任务生成的构建产物，保留源码、提交、模型、日志和二进制摘要，再用仓库已有 `dev-small` profile 执行完全相同断言并通过。该 profile 只减少 debug/符号，不减少验收。没有杀 Rust PID、改测试或换机器。
- 本次清理过 Pipecat 构建缓存后留下部分 CMake 目录，Whisper 重建真实失败。使用包级 `cargo clean -p whisper-rs-sys` 修复，之后 [native rebuild](/workspace/pipecat-hmi-audit-20261023/pipecat-sourced-speech-rebuild.log) 退出 0，来源回调 3 项通过。原失败与清理清单保留。
- speech fixture 首次漏了协议必需的 `completedAtMs`；完整回路 fixture 首次漏了撤权前的第二次 retirement；抽取 codec 模块时一次可见性替换产生语法错误，抽取媒体状态时与会话的 Shared 产生名称冲突。所有失败保留，修正 fixture/源码后通过，没有降低预期事件数、断言或 deadline。
- Pipecat 严格 Clippy 的取模/type complexity 失败与 Hepta 的跨 await 锁/expect 警告均保留，随后修复；不把初次退出 0 但有新增警告的 just fix 当作最终门禁。
- Bazel 最初默认 home cache 只读，退出 37。缓存显式放在同一环境允许的 `/workspace` 后成功。再次更新被旧 zombie server 卡住、退出 36；只读 `ps` 确认其为已退出的僵尸进程后，使用同一机器的 batch 模式及工作区 output base 成功。未改权限或重试被拒绝的 home 写入。
- Cargo.lock 的包排序由 Cargo 规范化；语义差异核验见 handoff，未做无关依赖升级。

## 阻塞、未运行与最小外部缺失项

| 状态 | 精确缺失项 / 影响范围 |
| --- | --- |
| 已推进：实际 owner 文本注册 | 第二轮已接入 `AgentdConfig::with_hmi_text()` / `--hmi-text`，由实际 `RuntimeTasks` 监督。复用 `TextTrust`、签名 AuthBus ingress、原 evidence outbox/relay 和原 Core client ID，已跑真实 Agentd/App Server 进程。测试身份来自仓库原有 fixture，HTTP 模型为 SSE fixture；没有发放生产凭证。详见下文第二轮证据 |
| 已推进：同一 thread 的进程级文本闭环 | 实际私有 UDS、实时签名/线程授权、原 SQLite ingress 和 Core rollout 已验证；中文流式输出、取消、重连、防重及下一轮次通过。生产用户/真实模型仍未验；具名 fleet/Agentd 运行环境变量未设置，未将独立测试身份冒充生产 |
| BLOCKED：媒体运行配置 | 缺 owner 预建私有目录、独占/保留策略、quota 与持久单调 capture generation，以及实际采用的平台和设备 ID。当前只实测 Linux 构建，不能当作用户实际部署平台已选定 |
| DENIED：Whisper 模型获取 | 对 `https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.en.bin` 的 proxy CONNECT 返回 403。没有下载、没有重试同一资源或换镜像/环境；后续 JFK fixture 获取未执行。这只是英文 native regression 的尝试，不能作为中文模型验收。需要获准的可信多语言 Whisper artifact、摘要与实际语音 fixture |
| BLOCKED：Piper 实际推理 | 缺可信 Piper manifest、模型、字典/voice 和原生运行依赖的完整 artifact 身份；TTS 实际推理 NOT_RUN |
| BLOCKED：实体音频设备 | `/dev/snd` 不存在；麦克风、扬声器、噪声/回声、真实中文/目标语言、抢话尾音、拔插、权限撤销均 NOT_RUN。没有切换到用户电脑、Actions 或其他环境绕过 |
| NOT_RUN：产品故障/性能 | 真实模型崩溃、Hepta/Agentd 重启、端到端断线恢复、实际队列压力、长期资源增长与 p50/p95/p99 均未验收。没有以编译耗时或 synthetic fixture 耗时代替 |
| NOT_RUN：neuron shadow | 尚未接入学习控制；先完成确定性路径和故障验收。权限、紧急停止与隐私边界始终不进入可学习控制 |

另一项保留的访问拒绝为 `https://api.github.com/repos/casey/just/releases/latest` CONNECT 403（未下载/未重试）。Cargo 源码安装 just 1.59.0 和 cargo-nextest 0.9.149 是仓库明确允许的独立路径，在同一环境成功，来源见安装日志。系统 apt 配置读取的 permission-denied 亦未重试。

第二轮已完成实际 owner 文本 hook 和进程级闭环。下一步源码工作是显式设计/接入签名音频来源 observation profile，复用实际 owner 的权限与原 ingress；现有 `AuthBusTextBody` 没有媒体字段，`KernelEvidenceAppend` 是 qualification envelope，不能偷换为音频 observation。文本 port 的 NoAudio 会明确拒绝音频观测；真实设备授权、模型与完整语音运行输入仍待就绪。模型和设备具备后，按原范围完成语音、故障及延迟/资源验收。未采用 WebRTC，不增设商业 provider 全量 parity 门槛。

原任务：[会话](https://chatgpt.com/c/6adb0f2f-7460-83e8-b88c-11e9da68323e)，ID `01a167b6-4fcc-7722-9808-9489f1564005`。


## 第二轮：实际 Agentd owner 文本注册与进程资格

本轮继续使用原保存云环境、原 audit 分支；Pipecat 前一轮 `5357b8a` 与 Hepta `53f6189be` 均保留为祖先。没有切换环境、推送或改写原候选分支。相关 `.agents/skills` 仍不存在，沿用仓库 AGENTS 约束。

实际注册入口是 Hepta `hepta-agentd/src/config.rs::with_hmi_text`、`main.rs::--hmi-text`、`runtime.rs::RuntimeTasks::spawn_optional("hmi-text",...)`。运行代码在 `hmi_text.rs` / `hmi_text_session.rs`，它直接使用真实 `AgentdState`，不是从 UI 字符串构造的授权。`TextAuthority::check_current` 调用 generation/readiness fence，并从实际已附着的 `TextIngress` 重读 `TextTrust`。首个输入和后续输入均需原有 issuer 的 Ed25519 签名和 thread allowlist；验证发生在接入和原 ingress，产出原来的真实 outbox/queue receipt。

媒体 coordinator 的 text 模式已接线：文本输出不调用 speech/audio observation。UI scope 的 principal 来自实际注册 issuer，本地 Agent UI 标识绑定 key epoch 与 Agent spawn；这些字段不构成麦克风权限。新 UDS 默认关闭、启用后仅属于原 Agent owner，未生成密钥/注册或扩张既有只读 gateway/organ。测试仅在云环境内的既有 disposable FleetHarness 中启用。

HMI 跟踪原 `hepta.authbus:<delivery_id>`，通过原 `authbus_ingress::submit` 和 `authbus_dispatch` 入原 Core thread；没有另发 `hepta.hmi:*` 队列请求。重连后的重复签名请求查询已有 canonical delivery，不重开输出或挡住下一次新输入。等待 ingress 查询期间的本地打断用 owner-minted epoch fence 防止旧请求重新打开输出。writer 与 session future 都由 runtime 直接拥有；强制服务退出不会遗留分离的 writer 任务。

真实进程首次运行揭示新建 Core thread 的 lazy rollout 尚未物化，`thread/read` 可用但 `thread/resume` 报 no rollout。修复仅覆盖显式 `excludeTurns:true`、无 path/history/page override、已经加载且非 ephemeral 的同一 thread：使用 Core 自己的 materialize/flush，然后继续原订阅。默认历史 resume 的原拒绝回归保持不变，没有插入预热对话、伪造历史或另建 thread。

| 资格 | 结果与准确边界 | 证据 |
| --- | --- | --- |
| 实际 Agentd / App Server 的 HMI 进程回路 | 2/2 通过，0 跳过；真实私有 UDS、实际签名验证/实时撤权、原 SQLite outbox/checkpoint、Core 持久 turn；模型 HTTP SSE 为 fixture | [最终进程回归](/workspace/pipecat-hmi-audit-20261023/agentd-hmi-owned-futures-regression.log) |
| 重连后的重复及后续新轮次 | 原输入在重复/重连/只读恢复后仍只有一次模型请求；新的不同 sequence 恰好再调用一次，且成功返回，没有被旧输入挡住 | [强化回归](/workspace/pipecat-hmi-audit-20261023/agentd-hmi-recovered-next-turn.log) |
| 原签名文本入口 | 原有 2/2 通过；最初新 HMI 的 2 个失败与原入口通过出现在同一日志中，未把部分成功当作全包成功 | [首次真实进程失败/原入口结果](/workspace/pipecat-hmi-audit-20261023/agentd-hmi-owner-process-tests-fixed-listener.log) |
| App Server resume 兼容边界 | 2/2 通过，1014 个其他用例被筛除；旧默认拒绝和新显式订阅分别验证 | [resume](/workspace/pipecat-hmi-audit-20261023/app-server-initial-resume-regression.log) |
| adapter 全包与新增取消 fence | 原 41/41 通过；新增 fence 1/1 定向通过（41 筛除），不是声称最后一次全跑 42 项 | [全包](/workspace/pipecat-hmi-audit-20261023/hmi-external-owner-adapter-regression.log)、[fence](/workspace/pipecat-hmi-audit-20261023/hmi-admission-fence-regression.log) |
| Clippy / 格式 | 三包 just fix 退出 0（原有警告保留）；App Server + adapter 全目标 strict `-D warnings` 退出 0。Agentd `-D clippy::all` 实际退出 101，8 个诊断所在源码均与原候选及第一轮 HEAD 字节一致，未抑制或冒充通过；见精确路径/行号/摘要证据。仓库 just fmt 因源码已提交而为空操作；另用精确 recipe 显式格式化/检查全部 17 个变更 Rust 文件，均退出 0；规范化比较证明与已测试 fe6900449 仅有格式差异，见 owner-v2-final-format-equivalence.json | [strict 通过](/workspace/pipecat-hmi-audit-20261023/app-server-hmi-strict-clippy.log)、[Agentd 失败](/workspace/pipecat-hmi-audit-20261023/agentd-hmi-product-clippy.log)、[基线核验](/workspace/pipecat-hmi-audit-20261023/agentd-baseline-lint-evidence.json) |

编译时真实发现 codex_uds wrapper 的 accept 返回值/可变借用差异，修复后继续；日志保留。运行中的路径别名创建有只读文件系统警告，未修改系统目录或权限。磁盘一度剩 124 MiB；确认无 Rust 构建运行后，仅删除本任务的约 19.8 GB incremental cache，未触碰 native CMake 目录、源码、模型、已完成测试二进制与证据。后续 CARGO_INCREMENTAL=0 控制缓存增长，验收断言、特性、原执行环境均不变。

本轮没有运行真实生产用户、完整 Agentd 进程重启/崩溃恢复、物理音频、中文语音推理、噪声/回声、拔插或端到端分位延迟；连接 reconnect 不能冒充整个 Agentd 重启。已有 /dev/snd 缺失和模型 CONNECT 403 保持原阻塞，未重试/绕过。

下一步的最小真实输入：原 owner 的实际 fleet/AgentId/generation、已经存在的 trust/checkpoint 与独立签名 producer、目标 thread；语音另需实际平台/设备身份与权限来源、可信 Whisper/Piper artifact/manifest、媒体 root/quota/generation 和显式签名来源 observation profile。只检查了具名环境变量的存在性，未打印凭证，见 [运行输入存在性](/workspace/pipecat-hmi-audit-20261023/owner-runtime-input-presence.json)。`AuthBusTextBody`（agent-protocol/src/authbus.rs）只含 spawn_generation/thread_id/text；`KernelEvidenceAppendIngress`（agent-protocol/src/evidence.rs）和 evidence_host.rs 明确使用 qualification envelope；channel_ingress migration 只有 schema/validation，不能凭表名当作运行中的 audio ingress。

第二轮补丁、最终源码身份与全部新失败/成功日志见 `/workspace/pipecat-hmi-audit-20261023/owner-v2-handoff.json`；第一轮交接和归档保留。

第二轮源码锚点 `fe6900449` 的 17 个 Rust 变更经过规范化比较，与最终源文件完全一致；fmt 后按仓库要求未再跑已通过测试。第一轮报告/patch/bundle/归档不覆盖，第二轮独立交接使用 owner-v2 前缀。
