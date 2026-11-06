# Rust Core / Hepta HMI：运行路径加固与全量能力账（2026-11-06）

> 本页保留前一阶段的源码、结果与当时边界。后续完整 workspace 推进、已恢复的原 ALC897 环境、实际 SFU 与 browser 运行及新增失败，见 [HMI 全量推进报告](HMI_FULL_SWEEP_20261106.md)。下文“尚未重跑／入口未恢复”等陈述属于本页原时间点；原 WebRTC invocation-25 与生产组合门槛仍须按后续报告分别结算，不能用后来的通过抹去。

## 结论

本轮把三个缺口推进到了实际运行路径：**pipeline 的 payload 字节准入、Linux 原生媒体进程监管、WebRTC DTLS 启动与关闭的生命周期修复**。Hepta 本地语音输入已经启用严格预算；公开的 `local_speech CONFIG_JSON OWNER_CONNECTION_ID` 默认入口已经在 Linux 启动受监管 native child。原 Hepta conversation/turn、签名观察、权限和模型／工具执行 owner 仍是唯一权威。

冻结源码的选定包测试、严格 Clippy、实际 native build 和无设备入口故障检查已完成。**生产 HMI、物理录放音、全 workspace release 和全部 Pipecat parity 仍为 HOLD。** WebRTC 压力测试中的一次 data-channel 建立超时保留为未归因失败；本轮不能宣称历史随机故障全部解决。

本报告接续 [前一轮 Core 审计](HMI_CORE_AUDIT_20261106.md)、[全部上游／provider 差异](AUDIT_UPSTREAM_20261106.md) 和 [原 Hepta owner 契约](OWNER_CONTRACT_20261106.md)。旧记录保留其原源码与运行环境，本报告不把它们改写为当前通过数。

## 1. 固定源码与交付身份

| 项目 | 精确身份 |
| --- | --- |
| 本轮父基线 | `5e889453a6956b03c6d0745663a87b0e604adebd` |
| 实际测试的本地代码提交 | `772e3ca152c53a33f70b2f47bab8610efd6e5ce8` |
| 发布的代码提交 | `6367d62babf97cab96a756b58427c8ee5fac5f15` |
| 两者相同的完整 Git tree | `652aceec8a2dbf70acc6d58dcfc39af12e710f82` |
| 既有审查入口 | [Draft PR #2](https://github.com/TrillionniumFoundation/pipecat-rs/pull/2)，`work/hmi-core-audit-20261106` |
| Hepta companion | `7bbdd715957e72274bc13209c4a619913e880129`；本轮只读，未修改或部署 |
| 最终实际 native executable SHA-256 | `212bf44ae88bee0c0640fa109ea308404a284f3b9157f8388737a1cc6858d973` |

发布通过已授权 GitHub Git Data API 完成；在移动原分支前逐 blob 核对完整 tree，并以原 head 作 lease。随后独立 public `git fetch` 核对发布代码 tree。Git 提交元数据不同，测试源码树相同。后续证据提交仅改文档／证据；根 README 仍为 0 bytes。[发布对账](qualification-hmi-followthrough-20261106/publication-code.json)

## 2. 冻结源码上的最终验证

所有下表运行使用 Rust 1.99.0、edition 2021、locked/offline 依赖；开始和结束的 commit/tree 一致，工作树干净。`results.json` 与 `final-gates.json` 保留精确 argv、环境项、日志摘要、代码文件摘要及退出码。

| 检查 | 结果 | 范围 |
| --- | --- | --- |
| `pipecat-core` + `pipecat-pipeline` | **480 passed / 0 failed / 0 ignored** | 含 13 项新 payload 回归与 2 个 doc tests |
| `pipecat-hepta --features local-speech --all-targets` | **65 passed / 0 failed / 3 ignored** | 含 24 项 supervisor 回归和 3 项真实父死亡顶层测试 |
| `pipecat-webrtc --all-targets` | **46 passed / 0 failed / 0 ignored** | 20 lib、16 localhost、4 receive-only、6 lifecycle；包括最终新增失败诊断 |
| 四个选定包 strict Clippy | **exit 0** | `--all-targets --features pipecat-hepta/local-speech -- -D warnings` |
| 实际 `local_speech` example build | **exit 0** | 原生依赖真实编译链接；未加载模型或设备 |
| 最终 executable 的入口故障检查 | **8/8** | 非法／缺失参数、缺配置、满 stderr、阻塞配置 FIFO 后 shutdown |
| 同一 executable 的父死亡 FIFO probe | **PASS** | exact wrapper/native executable hash 与 PID/starttime，父被杀后原 native 退出，无伪造终态 |
| 格式／脚本 | **PASS** | 修改 Rust 文件 rustfmt；diff whitespace；两个 qualification scripts AST parse |

三个 ignored 的意义不同：两个需要真实 Whisper/Piper 模型或授权物理设备；另一个是被三项父死亡测试显式启动的 subprocess-only helper。没有把物理 ignored 改成 mock，也没有把 helper 内部回显重复计数。不同 feature、子集、重复压力执行和历史 checkpoint 不累加成“唯一覆盖项”。

最终证据入口：[主账](qualification-hmi-followthrough-20261106/results.json)、[完整命令与源码绑定](qualification-hmi-followthrough-20261106/final-gates.json)、[日志索引](qualification-hmi-followthrough-20261106/README.md)、[文件摘要](qualification-hmi-followthrough-20261106/SHA256SUMS.json)。这些检查不是 62 crate 完整 workspace gate。

## 3. 已落实的三组改动

### 3.1 Payload 字节预算与真实 HMI 接线

新增 `Pipeline::with_payload_limits(PayloadLimits)`，不增加 `PipelineConfig` 必填字段。通用 pipeline 兼容默认仍关闭严格 profile，以保留现有 opaque LLM/tool 能力引用；Hepta 的 CPAL → Silero → Whisper 输入明确启用了它。

| 默认严格限额 | 数值／语义 |
| --- | --- |
| 单帧 | compact JSON 计费最多 1 MiB |
| normal queue | 8 MiB，仍同时受 item 数限制 |
| system queue | 1 MiB，仍同时受 item 数限制 |
| Start reservation | 独立一个 frame，最多 1 MiB |
| Cancel / Interruption / Resume reservation | 独立一个 frame，最多 1 MiB，system media 不能占用 |

计量覆盖 frame name、metadata、transport labels 和可序列化 FrameData，通过 capped writer 计数，不生成整份序列化副本。超限、算术溢出、超过 128 层嵌套、opaque callback/native continuation/tool 等不可计量内容明确拒绝，不按零字节放行。JSON 的 byte array 会大于原始媒体字节。

外部 `send().await` 对临时 item/byte 满等待容量；不可能容纳的 frame 立即拒绝。普通内部 `ctx.push/queue` 在满时立即返回错误，不在 processor admission lock 中等待。显式 owned-background downstream wait 保留原 causal/staleness 契约，也会先检查单帧预算。生成输出同样检查单帧上限。

队列项持有 RAII reservation，dequeue、interruption reset、discard 和 drop 释放计费；child pipeline 继承同一限额。每个严格 processor 的 queued charge 数学上界为 normal + system + 2×max_frame，默认 **11 MiB**。这不包含整个 output channel、当前执行调用、String/Vec spare capacity、processor cache、复制、observer/error history、native 模型、caller pending buffers 或消费者保留的输出，**不是 RSS 或全进程 retained allocation 上界**。

HMI 输入当前使用可计量的 20ms PCM、VAD/control DTO、metrics 和 final transcription JSON；超大的结果会明确失败。此变更不把 Whisper 升为流式 partial STT。[完整契约](../crates/pipecat-core/PAYLOAD_ADMISSION.md)

### 3.2 Linux 实际 native 入口的进程监管

公开调用保持 `local_speech CONFIG_JSON OWNER_CONNECTION_ID`。Linux 父进程负责 framing、private 控制回执关联与 child 生命周期；native child 保留原 command scope gate、模型和设备效果边界。private readiness/control sequence 从对外 JSONL 中移除，不改变 owner 的 wire schema，不产生新 conversation/model/tool owner。

默认期限为 startup 120s、Turn/Interrupt/Retire 完成 5s、shutdown/revoke/EOF 10s、pipe write/drain 2s、kill 后等待实际 reap 5s；最多 8 个未确认控制。控制计时在可能阻塞的 child stdin write 之前开始。非关联 VAD event 不能替代某条命令的 private ACK。

独立复审补上两个真实缺口：Turn 在新 epoch 下也可能同步关闭／重开 CPAL，因此纳入控制期限；timer 到期时重读当前 pending deadline，避免已经按时 ACK 的控制被过期快照误杀。对应两项回归已进入最终 24 项 supervisor 测试。

成功要求实际 native ready、真正完整且成功的 `media_terminal` 已转发、无未完成控制、child 成功退出。终态验证要求 `input_errors` 和五个错误字段完整；缺失、异常或仅 process exit 0 都不足以成功。父输入拒绝原因保留到 child 实际终态；没有 native terminal 时不制造 terminal。

超时会请求结束精确 child，再独立等待 exit status。`kill_requested`、观察到 exit 和实际 wait/reap 分开记录；kernel-stuck 未能 reap 保留 UNKNOWN。已提交音频、远端或工具效果不因 kill 被回滚，也不自动重放。

native 初始线程在 runtime、配置／模型／设备之前设置 Linux `PR_SET_PDEATHSIG(SIGKILL)`，设置前后核对真实 PPID。创建它的父线程必须活到 native 退出；实际 current-thread supervisor 满足此前提。只保护这个 native，不覆盖任意后代；credential changes 或清除该 signal 会使保证失效。非 Linux 保留原 direct path，未宣称等效 containment。

实际入口故障检查曾抓到继承 stderr 写满时的退出阻塞。当前诊断最多 4 KiB、best effort 等待 100ms；Linux 已失败进程若日志仍阻塞，只向自身发送 SIGKILL，以显式失败退出。最终 8 项 executable 检查全部通过；阻塞配置 FIFO 的 shutdown 检查观察到期限后 kill 请求与 child status，stdout 没有成功终态。

最终独立父死亡 probe 对 wrapper/native 正在运行的 executable 都重算 SHA，native 停在配置 FIFO open、尚未加载模型或打开音频设备。杀死 wrapper 后观察原 native incarnation 退出，未使用应急 cleanup，stdout 为零。该 harness 不拥有 native 的 waitpid，报告为 exit observation；直接 child 的 waitpid/reap 证据来自另外三项真实进程测试。单次毫秒观察不是性能 SLA。

物理 qualification script 已改为分别识别 wrapper 与 native、核对 PID/starttime/argv/executable、先等真实 capture RUNNING 再执行控制、独立核对 native 退出和 capture 释放。异常清理使用 pidfd 固定目标，缺支持则拒绝清理，不回退到有 PID 复用窗口的数字 PID signal。脚本本轮没有执行物理采集。[入口契约](../crates/pipecat-hepta/README.md) · [最终入口记录](qualification-hmi-followthrough-20261106/final-actual-entry.json) · [最终父死亡记录](qualification-hmi-followthrough-20261106/parent-guard/final-native-entry-fifo.json)

### 3.3 WebRTC DTLS start/stop 生命周期修复

旧路径直到 `DTLSConn::new` 完成才发布 `self.conn`，stop 无法取消尚未发布的构造 future；旧 start 也可能在 stop 后继续进入构造，保留 ICE/UDP 引用。最小修复增加 stop notification：start 先 enable waiter 再检查 durable closed，biased select 让 stop 取消 pending constructor；stop 在第一次 cleanup await 前写 closed 并通知。既有连接和 SRTP/SRTCP publication guard 继续检查关闭状态。

两项回归用实际 localhost ICE/UDP：一项控制 Connecting callback 返回顺序，验证 close 后实际 UDP candidate 可重新绑定；另一项让 ICE 连通、对侧不协商 DTLS，验证 stop 能结束 Pending start。**未修复 vendor 上两项均失败；修复后的最终 package gate 含这两项并通过。** 阶段 30 次重复、共 60 次执行也通过，但不是新增独立用例。

独立审查核对了锁定 `dtls 0.13.0` 的 drop 清理链；没有改变 wire、角色、加密配置或依赖版本。测试会主动释放 application Connecting callback，本补丁不承诺抢占任意永不返回的应用回调。[修复与限制](../crates/pipecat-webrtc/SHUTDOWN_REGRESSION.md) · [baseline 两项失败原文](qualification-hmi-followthrough-20261106/webrtc-baseline-both-regressions.log)

## 4. 仍保留的失败与 UNKNOWN

**WebRTC 建连稳定性仍开放。** 原 stress 在修复后连续 24 次通过，第 25 次在 A/offerer 首个 `wait_data_channel(5s)` 超时，发生在这对 peer shutdown 之前；随后 30 次通过不能清除它。原日志缺少当时的 schedule/iteration 和分层状态；23.83s 是整次 test invocation 时长，不是单次等待耗时。审查没有证明 lost-wakeup、协议 race 或资源压力。public Connected 只汇总 ICE/DTLS，不能证明 SCTP、DCEP、channel Open 和 wrapper publication 全完成。

现在仅补充失败当轮的 schedule、iteration、A/B、monotonic elapsed、双方 state/cancel/error/counters，保留 A 后 B、各 5s 和 UDP 释放 2s 的原判定，不在失败后重试。若再次出现，仍需该轮双方 ICE→DTLS→SCTP→Open→publication 时间线及调度证据归因。[保留的失败](qualification-hmi-followthrough-20261106/webrtc-fixed-repeat.log) · [后续观察](qualification-hmi-followthrough-20261106/webrtc-fixed-repeat-retry.log)

历史完整 workspace 曾有 `UDP leak schedule=5, iteration=24`，本轮 baseline 重复没有重现这个确切随机场景。本轮两个确定性缺陷与完整包成功不能替代未重跑的完整 workspace gate，也不能将历史失败改写为已完全解释。[历史资格记录](../../docs/DRAFT_QUALIFICATION.md)

| 保留项 | 处理与资格边界 |
| --- | --- |
| 第一轮实际入口满 stderr 超时 | 保留 4s timeout 及后续 communicate timeout；修复后绑定新 binary 复验 |
| 第一轮冻结源码 strict Clippy 失败 | `collapsible_if`，原 480/65/46 测试通过但 gate 失败；等价合并条件后重新冻结并完整复跑，不修改通过判定 |
| 初期 ENOSPC、缺 cache/CMake、fixture 编译／candidate 假设问题 | 前置失败原文保留，不算行为通过或有效 negative control；锁定版本未更换 |
| Linux `/proc/task/.../children` 缺失 | 初次 FIFO inventory 失败保留；增加有上限的 PPid scan，再做相同 exact incarnation 核对 |
| 两个远端只读任务 UNKNOWN | 原 desktop `1a1b01f09b3-fc05-6e3b`；本轮 ai395 `1a1b04f07ab-fc05-6fa9`，同 ID 对账未取得状态，未重复派发或改成成功 |

原始记录及源码阶段归属见 [证据索引](qualification-hmi-followthrough-20261106/README.md)。被协调中止的独立 clippy 不算通过；预期故障注入中的 worker panic 也不会掩盖另一项真实测试失败。

## 5. 用户原能力表：本轮之后的完整账

| 能力 | 本轮推进或确认 | 仍需补齐／独立验收 |
| --- | --- | --- |
| 异步 pipeline、串并行、生命周期 | 实际单帧／queued payload 准入，外部 wait 与内部立即失败，控制 reservation，child 继承，HMI input opt-in | 总 RSS、in-flight/native/caller/consumer retention；其它 pipeline 必须自己启用 profile |
| 错误与取消 | Linux 实际入口的控制／退出期限、parent-death、kill 与 reap 分账、终态保真 | native 原线程抢占、已发生效果回滚、非 Linux containment、任意 descendants 和生产恢复对账 |
| 本地语音 | CPAL/Silero/Whisper/Piper 原生路径接预算和 supervisor | final transcript 为主；真实中文、噪声／回声、OS grant、拔插／撤权、录放音与用户听到 |
| 云 STT/TTS/LLM/Realtime | 保留专属协议与兼容 preset，继续使用逐厂商能力矩阵 | preset 不等于完整 API；一家的 fixture 不能证明另一家的 auth/reconnect/finality/限流；本轮无 live provider 晋级 |
| turn、工具、MCP、flows | 原 Hepta owner 保持唯一权威；新父进程不执行模型／工具 | 通用库能力不能变成第二个会话／效果 owner；生产 owner 的真实组合及恢复仍待验证 |
| 共享媒体与评测 | 已有 cyclic image/sprite、音频、用户图片及相关评测，沿用已核对的源码事实 | “没有共享媒体／只有文本评测”的全局描述已过时；simulation/fixture 仍不证明用户质量 |
| 原生 WebRTC | 两条 DTLS 生命周期缺陷和真实 UDP 清理回归已修复；最终 46 项通过 | setup timeout 未归因；video/camera/screen、adaptive jitter、PLC、Opus FEC、统一播放时钟、已有 peer ICE restart／track replacement 仍缺能力或资格 |
| LiveKit／其它实时传输 | 已有原生实现和部分音视频路径 | 本轮没有 SFU、真实远端 video、最终 participant control 的新证据；不能拼接其它分支记录 |
| 浏览器／RTVI | 已有协议、模板、客户端基础 | 真实 mic permission、中文 IME、autoplay、断网恢复、用户可见状态和 owner/media 对账未验收 |
| Hepta HMI | 实际 Linux 媒体入口和输入预算落地；原 scope/epoch fence 保留 | 原 pinned Agentd 仍注册 TextPort + NoAudio；production media、签名 producer、replay witness、grants 与同 thread E2E 未组合 |

进一步源码范围与 provider 项目逐项见 [前一轮全量审计](AUDIT_UPSTREAM_20261106.md)。本轮没有给这些未执行项目补造通过记录，也没有使 neuron shadow 获得权限、隐私或紧急停止控制权。

## 6. 原设备和生产组合的实际阻塞点

新的有界只读检查确认 Pocket4 codec 为 ALC245、ROG 为 ALC294；原历史 ALC897 HMI 环境的主机身份及 callable executor 仍未恢复。Pocket4 可见旧 Python Pipecat / SenseVoice / Melo 项目与模型目录；当前 `hepta-pocket-voice-lab` 容器 devices 为空、network=none、readonly root。旧 SQLite voice ledger 和私有 sockets 不建立本轮 canonical Hepta owner，也不构成 Rust 物理麦克风闭环。

发布的环境记录仅选取本次 job 的验收相关输出，保留原 capture 摘要；其中嵌入的旧部署文字不作为本次模型／声音质量结果。[环境证据](qualification-hmi-followthrough-20261106/environment-followthrough.json)

生产下一道门槛需要同一次运行中的真实组合：原 Supervisor/Agentd 与已配置 identity/thread、签名观察 producer 与 replay witness、当前 device/organ/capture generation、OS grants、采用的不可变 Whisper/Piper/Silero manifests、私有媒体根目录及实际连接的麦克风／扬声器。应先完成同 thread 文本与恢复，再验证真人中文输入→来源 receipt→原 owner→当前 epoch 输出→实际设备和用户听感，随后跑拔插、撤权、崩溃、UNKNOWN 对账及跨平台长期资源／p50/p95/p99。

本轮没有真实模型推理、录音、播放、摄像头、生产切换或权限变更。物理脚本已为精确 native 身份和退出观察做好接线，但没有这些真实生产输入时，不能用 fixture connection、合成音频、空设备或其它部署的成功片段替代验收。
