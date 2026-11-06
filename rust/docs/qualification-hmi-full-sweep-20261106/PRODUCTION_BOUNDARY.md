# 全量推进后的生产边界（2026-11-06）

**原 10/29 ALC897 原生环境及其历史资产已恢复定位；生产 Hepta HMI 的同次运行组合仍未建立。** 本轮取得的浏览器实测，以及独立源码快照上的真实本地 SFU 运行，分别扩展了可确认的工程范围。它们不替代原 owner、原 thread、签名观察、当前设备权限、不可变模型配置和实际麦克风／扬声器共同参与的一次生产运行。[环境恢复](environment/README.md) · [原 owner 契约](../OWNER_CONTRACT_20261106.md)

本文在恢复的 Pipecat 源码 `7105367d7d607653c3ab758f0131c029c4373faf`、tree `19a5c08fc2cbfeabacd344668fc7b25d03a282c2` 上整理。环境记录、浏览器结果、首次 SFU 运行及后续最终 native gate 各自保留自己的源码、工具链和运行身份；本文不把它们合并成一个全量通过数。最终源码 pin、完整 workspace、额外真实模型与 SFU 的最终执行状态，统一以 [本轮主报告](../HMI_FULL_SWEEP_20261106.md) 及其原始 receipts 为准。

## 1. 已恢复的环境和仍未取得的生产入口

在已配置的 `desktop` 上，找回了 `/home/qian-qi/workspace/hepta-hmi-20261029`。只读核对确认 codec 为 Realtek ALC897；原 Pipecat checkout 为干净的 `ec40f982205cb2c87f61094d78d163c5c2febb39`，原 Hepta checkout 为干净的 `7bbdd715957e72274bc13209c4a619913e880129`。这是对原历史环境的恢复定位，不能继续描述为“ALC897 环境完全未找到”。[恢复原始记录](environment/recovery.json)

| 恢复或观测到的材料 | 可以确认的范围 | 仍不能由此推出的结论 |
| --- | --- | --- |
| 两份 Whisper 文件、Silero、Piper voice/config、ONNX Runtime、eSpeak 库，共 7 个普通文件 | 文件大小与新计算 SHA-256 对上历史清单 | 环境探针没有重新遍历并哈希整个 eSpeak 数据目录；不证明生产 owner 已采用该模型组合 |
| 3 份 10/29 历史 native 日志 | 新计算摘要与原发布记录一致，历史记录可以继续追溯 | 哈希核对不是本轮重新执行，更不是当前源码的新通过数 |
| ALC897 codec 与指定 ALSA jack controls | 所读 Front Mic、Rear Mic、Line、Line Out、Front Headphone controls 为 off；SPDIF phantom 为 on | 不据这些开关证明实际声学连通、可懂的中文输入或用户听到输出 |
| HMI 模板目录及 3 份 template JSON | 原部署材料的模板路径存在 | 模板不等于实际 identity、有效 grants、当前签名配置或正在使用的生产 socket |
| 历史 `qualification:owner`、`qualification:offline`、`qualification:alc897` 及 generations 1/2 | 原 fixture 的标识和配置可核对 | 不能采用为本次生产身份，也不能把旧 generation 当作当前设备权限 |

上述环境探针没有录音、播放、打开摄像头或修改权限；该限制只描述环境核对本身，不抹去本轮其它独立浏览器、模型或 SFU 工作的实际执行范围。10/23 cloud executor 与 10/29 desktop 原生环境也保持区分：找回后者没有恢复前者的原 callable entry。[环境记录及范围说明](environment/README.md)

### 对 absence 的解释保持在实际查询范围内

本轮进程观测仅检查可读 `/proc/*/comm` 中匹配 `agentd|codex-hepta|hepta-super|pipecat|local_speech|app-server` 的名称，以及选定 systemd unit 名称；在这个范围内没有命中。`HEPTA_FLEET_ROOT`、`HEPTA_AGENT_ID`、`HEPTA_AGENT_GENERATION`、`HEPTA_AGENT_HOME`、`HEPTA_AGENT_RUN_ROOT` 五项为 false，仅表示**那一次 SSH job 的环境中没有这些变量**。没有读取其它进程的完整环境、私钥或无关凭据，不能据此宣称整机没有真实 owner。[查询范围与 job 身份](environment/recovery.json)

对已观察到的 11/04 工程目录，继续读取了确切的 `command-plan.json`、`environment.json`、`validation-results.json`、`source-after.json`；两次有界读取的四文件大小和摘要一致。内容是 Agentd build、`cognitive_product_e2e`、`supervisord_product_e2e`、修复及格式化记录。两个 original test stage 记录 exit 96、`fresh_junit=false`；没有从这些字段推定原失败原因，也没有重跑这些历史阶段。这四份 JSON 未提供可用于本次运行的 owner／thread／unit／HMI socket／trust checkpoint／audio authority 组合；目录中的其他材料及未观测进程仍属未观测范围。[11/04 公开工程字段与来源](environment/product-inputs.json)

另两次面向原入口线索的窄范围历史检索，没有新增可按同一身份核对的 owner、thread、unit 或 socket 线索。这只是检索结果的限制，不是整机或所有历史系统均无 owner 的证据，也不用于导入其它主题的身份来替代当前 Hepta owner。

### 执行入口中断和历史 UNKNOWN

第一次本地入口中断后，恢复可见的 18 份文件逐一对上原大小和 SHA-256；原远端输出也按原 job 身份重新读取核对。随后本地入口第二次更换，旧 scratch 在新入口下不再可见；本次从已发布 commit/tree 恢复仓库，并取回远端持久证据。**“当前入口看不到旧 scratch”不等于原数据全部永久丢失。** 两次事件分别记录，不能用第二次不可见改写第一次已经完成的摘要核对。[首次恢复核对](environment/recovery.json)

旧 `desktop` job `1a1b01f09b3-fc05-6e3b` 和 `ai395` job `1a1b04f07ab-fc05-6fa9` 的原 ID 对账返回 `INVALID_ARGUMENT / FileNotFoundError`，仍为 unresolved；没有重放、重启或自动重试。后来成功的环境探针不清除这两个 UNKNOWN。[原 ID 对账](environment/recovery.json)

## 2. 原 Hepta 仍是唯一会话和效果 owner

固定 Hepta `7bbdd715957e72274bc13209c4a619913e880129` 的实际 Agentd 注册仍为 `HmiOwnerBridge<TextPort, NoAudio>`：`TextPort.capture_generation()` 为 0，音频 `speak`／`verify_source` 和 `NoAudio.observe_final` 明确拒绝相应职责。已有 `submitAudio` 签名入口不自动创建媒体 child，也不把文本 port 转变成生产麦克风或扬声器 owner。[实际注册代码][text-port] · [owner 审计](../OWNER_CONTRACT_20261106.md)

原 Agentd、App Server、AuthBus evidence 和 Core queue 继续决定 conversation/thread、canonical message、turn/output epoch、输入 admission、模型与工具执行、MCP 和 flows。Pipecat 的 pipeline、媒体处理、WebRTC、LiveKit、客户端以及本地模型实现，可以为原 owner 执行有界工作；它们不能成为第二套会话数据库、权限签发方或工具执行权威。只读 organ trait 也不能被扩展成绕过原 owner 的设备或网络写入口。[权威划分和只读契约](../OWNER_CONTRACT_20261106.md)

当前文本路径的启用点是原 Agentd 的 `--hmi-text`／`with_hmi_text()` 和 `run_root/hmi-text.sock`；其 binding 需要原 App Server UDS、实际 `codex_home`、已有 thread 及 session 身份。下一步应恢复并附着这个既有 owner 的实际入口。新建 qualification owner、新开一个替代 thread 或借用另一套 Python 语音部署，均不能补足本次生产资格。[文本会话实现][text-session] · [部署材料契约][owner-template]

### 下一道准入需要同一次运行中的真实材料

| 必要材料 | 需要能核对的实际关系 | 当前界限 |
| --- | --- | --- |
| 原 Supervisor／Agentd 与执行入口 | 当前 agent identity、spawn generation、run root、实际 unit／socket／受控管道，以及原 App Server 连接 | 已恢复源码和模板；尚未恢复可用于本次生产组合的完整实际入口 |
| 原 conversation/thread | 已存在的确切 thread、allowlist、App Server session 与当前 connection incarnation；提交、取消和恢复沿用原 canonical ID | 历史 fixture 的 labels 和连接数字不能代替真实 binding |
| 签名 producer 和 replay witness | 当前 issuer/key epoch、已注册 producer 的公钥信任、独立 replay checkpoint/witness，以及原 observation ingress 的 receipt/reconcile | 不要求把私钥正文写入报告；需要能验证其由原 owner 正确采用 |
| 当前设备和权限 | device／organ／capture generations、对应 grants、实际 OS 权限和失效／撤销路径 | 历史 generations 1/2、模板和当前 SSH 环境变量均不构成有效授权 |
| 采用的不可变模型配置 | 原 owner 选定的 Whisper／Silero／Piper／ORT／eSpeak 配置及完整 provenance，加载期间保持不可变；数据目录的清单不能以单个库摘要代替 | 找回并核对模型文件只解决资产可得性，不证明生产采用或中文质量 |
| 私有媒体存储及受控输出 | 原 owner 采用的私有 media root、scope、配额、引用验证／release，输出绑定当前 turn/epoch | 目录存在、PCM 哈希或媒体进程自报 receipt 都不足以证明原 ingress 已提交 |
| 实际麦克风和扬声器 | 设备选择、物理连接、实际捕获、设备提交、独立播放观察和用户确认位于同一次时间线 | 正常浏览器 AudioContext、SFU 解码、模型生成与设备 buffer 提交不等于用户听见 |

这些关系遵循原 owner 现有接口和权限。只有拿到该组合，才能依次验证同 thread 文本与恢复、真人中文输入、原始 PCM 来源 receipt、Core admission、模型完成、当前 epoch 的 TTS、设备提交和实际播放；随后验证拔插、撤权、崩溃、取消与 UNKNOWN 对账。无法确认效果时继续保留原 ID，不以重放或替代会话掩盖不确定状态。[材料及验收要求](../OWNER_CONTRACT_20261106.md) · [部署契约][owner-template]

## 3. 浏览器新增结果的准确资格范围

本轮最终浏览器 job `1a1b0db389b-fc05-7360` 已记录 `succeeded`、exit 0、`output_complete=true`，未自动重试、未超时；两份结果均为 passed，`failure=null`，page/cleanup errors 为空。浏览器版本为 Chrome `156.0.8078.79`，使用正常 Chromium sandbox。[执行状态](browser/final/runtime-status.json) · [实际命令](browser/final/results.json)

| 证据 | 本次实际确认 | 不扩展为 |
| --- | --- | --- |
| `protocol-r4`：23 组检查，原 114 条断言保留 | 当前 bundle 与实际 loopback WS/protobuf、配对 WebRTC 的协议／渲染／媒体和清理路径；延迟图片读取实际完成并核对字节摘要 | 真人麦克风、真实厂商、生产 Hepta owner 或正常 autoplay policy 的资格；该 fixture 使用合成设备及 autoplay 放行设置 |
| `input-r3`：4 组检查，text input mode | 真实 Connect click 后原生 AudioContext 为 running，16 kHz、16,000 samples 合成 PCM 实际 start/ended；没有 autoplay 放行 flag，麦克风调用为 0 | 所有浏览器／所有 autoplay policy 都通过，或用户听到了声音；该运行显式 mute audio |
| Chromium CDP 中文输入 | 实际 textarea 产生 composition start/update/end，保留 `中文 🦀` 加换行和 `第二行`；composition 期间未提前发送，显式 Send 只发送一次准确 protobuf 文本 | 操作系统拼音输入法、候选词窗口、系统语言切换或多个浏览器引擎已验收 |
| loopback TCP abort 和显式重连 | 中断后 draft 清空、资源关闭、Send 禁用；300 ms 观察窗口内无自动重连，用户再点 Connect 后旧消息／草稿不重放，新显式消息可发送 | WAN／真实断网／TURN／SFU 生产恢复、无限期不重试、原 Hepta token delta 的无损续传 |

对应原始结果为 [protocol-r4](browser/final/protocol-r4.json) 和 [input-r3](browser/final/input-r3.json)。CDP 路径实测 `compositionend.isTrusted=false`，而 composition start/update 和 composing input 为 true；最终资格保留这个实际 profile，不宣称所有 composition event 均 trusted。先前对此作出过强假设的失败仍在历史记录中；最小独立 textarea 对照与修正说明归入浏览器证据，不把断言假设改写成 OS IME 通过。[历史 input-r2 结果](browser/history/browser-r3-input-r2-results.json)

浏览器 Rust 应用源码和 locked dependencies 来自 `e526aabaf98522b2294e744ac48d75d9ed89a254`，最终 fixture 发布源码为 `478ab3b8e0deca51571c41c2890b519f0faeab94`；两者的区别以及运行前后 fixture／asset 摘要在独立记录中绑定。远端验收 bundle 不包含 Rust `src`，所以 `application_source_present=false` 是准确状态，不能把 bundle 当作现场源码 checkout。[源码绑定](browser/final/source-binding.json) · [运行时摘要](browser/final/input-r3.json)

## 4. SFU 与本地模型各自保留运行身份

首轮真实本地 SFU 的三个 scenario 已在 `ae56473` 源码快照、Rust `1.99.1` 上实际通过，范围分别为 audio/data/leave/rejoin、远端视频解码、participant mute/unmute。它们使用隔离的 pinned 本地 LiveKit server 和合成 tracks，属于该次源码与工具链的独立结果。后续 `7105367` 的有界 reap 改动及最终精确 pin 资格，以本轮主报告中的最终运行记录为准；这里不预填最终通过。[SFU fixture 与范围](../../crates/pipecat-livekit/qualification/LOCAL_SFU.md)

视频判据读取远端解码后的帧，核对尺寸、identity/source 及先后两个不同 RGB pattern；source 仅报告接受帧不能满足它。mute/unmute 判据包含实际 subscription 变化、指定观察窗口内目标 PCM 消失／恢复，以及持续 witness track 和 publisher 工作。因此这是比“服务启动”或“source accepted”更具体的本地传输证据；仍不证明真实摄像头／屏幕源、用户听感、广域网络、托管 SFU 账号或任意 participant policy 都已验收。[scenario 源码](../../crates/pipecat-livekit/tests/local_sfu/media_control.rs)

SFU fixture 的清理也保留独立事实：发出 kill 不等于已 reap；只有 `try_wait` 观察到退出才记录回收成功。每次 server reap/poll 有 3 秒界限，正常 test 返回时未确认回收会失败；已经 unwinding 时记录原失败及 PID，避免二次 panic。每个 peer close 的超时仍是 failure，不能写成已 join。外层 owned-job 的硬期限仍承担异常 kernel/I/O 情况的最后边界。[清理实现](../../crates/pipecat-livekit/tests/local_sfu.rs) · [范围说明](../../crates/pipecat-livekit/qualification/LOCAL_SFU.md)

中文真实模型测试首轮在 `LocalSpeech::load` 返回 `invalid media scope, reference, or quota`。该 fixture 的 `max_segment_bytes=16 MiB` 超过产品固定上限 `2 MiB`；产品在 MediaStore 准入处拒绝，尚未加载 native Silero／Whisper／Piper，也未进入后续八段 STT。前面的文件摘要核对不等于模型加载成功。这项失败应保留为原 fixture 输入错误；新 fixture 只把 segment 配额改到 `2,097,152` bytes，保持原模型摘要、文本和断言，不放宽产品契约。[准入上限](../../crates/pipecat-hepta/src/media_store.rs) · [加载顺序](../../crates/pipecat-hepta/src/local_speech.rs)

修正 fixture 后的执行与质量结果保持独立，见本轮主报告。即使合成中文 TTS→STT 通过，它证明的也是选定模型、合成输入和既定判据下的路径；非空 transcript 不等于字词准确率达标，更不能替代真人中文、回声、物理采集和用户听感。历史 10/29 日志和本轮新执行也不能混算。[历史模型与设备边界](../HMI_NATIVE_CLOSEOUT_20261029.md)

## 5. 用户原能力表的完整边界

| 范围 | 当前可以使用的事实 | 仍需补齐或独立资格 |
| --- | --- | --- |
| 生产 Hepta HMI | 实际 Rust 媒体入口和原 owner 接口已有；原历史环境已定位 | pinned Agentd 仍为 TextPort + NoAudio；同次 owner/thread、签名 producer/replay witness、当前 generations/grants、采用的模型和实际设备尚未组合 |
| 本地语音与设备 | final transcript 路径、真实模型资产及历史证据可核对；本轮模型运行独立记账 | 真人中文、回声、OS 授权、拔插／撤权、实际播放和用户确认；不以合成 transcript 非空证明质量 |
| 云端 provider | 保留逐厂商协议实现与兼容 preset 的区别 | 本轮没有新增真实厂商 auth、重连或 terminal/finality 资格；一个厂商或 loopback fixture 不能代替另一个厂商 |
| turn／工具／MCP／flows | 原 Hepta 仍为唯一 owner | 通用库和客户端不成为第二套会话／工具效果权威；未知效果依原 owner 对账 |
| 共享媒体与评测 | cyclic image／sprite、音频、用户图片评测等已有能力继续按源码记录 | simulation 和 fixture 不作为用户质量证明；不能倒退描述为“没有共享媒体／只有文本评测” |
| 原生 WebRTC 完整实时媒体 | 生命周期修补、诊断和具体 native gate 分别有源码／日志 | video／camera／screen、adaptive jitter、PLC、Opus FEC、统一播放时钟、已有 peer 的 ICE restart／track replacement 仍缺实现或对应资格；不把 LiveKit 的独立视频路径当作这些能力的证明 |
| LiveKit | 首轮本地真实 SFU 的三个合成 scenario 有独立通过；当前 fixture 覆盖远端解码与指定 participant control | 最终 pin 的完整资格见主报告；真实来源、托管／远端部署、网络恢复、设备体验和完整 participant policy 仍需独立证据 |
| 浏览器／RTVI | 最终 loopback 协议、合成媒体、正常 autoplay 的单一路径、CDP 中文和显式重连已有实测 | 真实麦克风／权限、OS 拼音、跨浏览器、用户听感、WAN 恢复和生产 owner 组合仍未验收 |

除本轮明确补充的环境、浏览器和 SFU 范围外，上表沿用 [逐项能力审计](../AUDIT_UPSTREAM_20261106.md) 和 [上一轮完整能力账](../HMI_FOLLOWTHROUGH_20261106.md)，不把未执行条目写成通过。

原 WebRTC 压力测试第 25 次 data-channel 建立超时的 [原始失败](../qualification-hmi-followthrough-20261106/webrtc-fixed-repeat.log) 继续保留；后续成功次数本身不删除它。其归因、本轮新失败与修补、完整 workspace gate 和最终精确源码的重复执行结果，按主报告分账，不由本文的环境恢复或浏览器成功代替。

[text-port]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/src/hmi_text.rs
[text-session]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/src/hmi_text_session.rs
[owner-template]: https://github.com/TrillionniumFoundation/hepta-private-ci/blob/7bbdd715957e72274bc13209c4a619913e880129/codex-rs/hepta-agentd/config-templates/hmi/README.md
