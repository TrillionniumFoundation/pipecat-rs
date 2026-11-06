# Rust Core 与 Hepta HMI：本轮交付、边界和验收（2026-11-06）

## 结论

当前正确产品边界是 **原 Hepta conversation/owner + 内生 HMI 组合 + 可复用 Rust 媒体 Core**。继续推进现有 Rust 适配器，完整 Pipecat 兼容库作为独立工作线维护。当前源码尚不能被宣布为生产 HMI Organ；实际 owner 的媒体进程组合、物理闭环、音视频、权限恢复及跨平台时延仍未完成。

本轮已经固定原分支、全量盘点两版上游和 Rust crate，核对 owner 契约与可达设备环境，并实现了实际媒体入口使用的 Rust 命令 Core。没有新建会话库、签发生产身份、扩张只读 organ trait，或把旧 Python voice ledger 升为会话权威。

完整材料：

- [上游全范围盘点与功能差异](AUDIT_UPSTREAM_20261106.md)：全文件库存、62 个 Rust crate、全部 provider 类别、关键实现与语义限制。
- [机器清单](audit-upstream-20261106.json)：两版 Python 路径/blob、Rust crate/test 入口和分支关系。
- [真实 owner 契约与独立代码审查](OWNER_CONTRACT_20261106.md)：原 AuthBus/thread 入口、连接/请求语义、只读边界、媒体组合缺口。
- [环境只读审计](ENVIRONMENT_AUDIT_20261106.json)：可达主机、Pocket4 旧部署、原专属环境不可调用、保留的 UNKNOWN。
- [本轮软件执行证据](qualification-hmi-core-20261106/results.json)：精确代码身份、命令、原始日志、通过/失败/忽略范围；不是生产设备资格。

## 固定组合

| 对象 | 精确身份 | 本轮作用 |
| --- | --- | --- |
| pipecat 既有 HMI 基线 | `ec40f982205cb2c87f61094d78d163c5c2febb39`，`work/hmi-closeout-20261029` | 新分支的直接父基线，不把未合并分支资格拼进来 |
| Rust Core 实现 | `b7e4c008c35036ac1b209dca97f6b737824f29c0`，`work/hmi-core-audit-20261106` | 本轮共享 decoder、连接绑定、命令去重和实际 endpoint 接线；后续审计提交只增加文档/证据 |
| Hepta companion | `7bbdd715957e72274bc13209c4a619913e880129`，`work/hmi-closeout-20261029` | 本轮只读契约审计，未修改 Hepta 源码或权限 |
| 原 Python 迁移点 | `af9f9da9d7afddaa8811c0e0251b32566c980bdf` | 旧的 134 Frame-derived class 覆盖口径 |
| 当前官方观测 | `6fa9cf28886036d5d6c7dfd05ed3d6960245241a`；release `v1.15.0` | 上游新增与旧迁移点分别列账 |

首页 README 继续保持空白。历史设备、owner 进程 fixture、原 native parity 和本轮软件测试分别绑定原源码；不能累加成当前全功能通过数。

## 已实现的 Rust Core

### 1. 在信息丢失前核对连接

旧 `HeptaOutputScope -> OutputIdentity` 转换只检查 `connection_id != 0`，转换结果不再含该字段。旧 endpoint 只有 stream/turn/epoch 判断，接收端无法区分其余标签相同的不同连接。

本轮用 `MediaConnectionBinding` 固定原 owner 提供的非零 connection，`turn` / `speak` 在转换前核验 exact connection 与完整 stream；`verify_source` / `release_source` 核验完整 stream。零 utterance sequence 与零相关 request ID 在媒体副作用前拒绝。这是接收端边界加固；没有宣称观察到针对生产 owner 的攻击，也不把匹配标签当作认证。

实际启动契约现在是：

```text
local_speech CONFIG_JSON OWNER_CONNECTION_ID
```

owner 必须将同一个 `HmiRequestsV1::connection_id()` 传给 argv 与 `HmiJsonlMediaV1::bind`。原一参命令失败关闭；语音 engine JSON 与 wire command schema 未改。连接 ID 仍是进程内 incarnation，跨 owner 重启还依赖原 grants、私有 pipe 与完整 generation。

### 2. 请求 ID 允许乱序，拒绝重复效果

固定 Hepta sender 在取得写锁前分配 ID，因此合法请求可能 ID 2 先于 ID 1，Busy 还会造成任意大缺口。不能简单采用“编号必须递增”或八项滑动窗口。

`MediaCommandGate` 用无淘汰 BTreeSet，在副作用前记录请求，跨 operation 的同 ID 重用也拒绝。失败/未知结果不释放 ID，不重放 ack。每个 endpoint 的明确预算是 65,536 个相关命令，容量满关闭并要求原 owner 退役/重新建立连接。停止命令不受该集合容量限制。这个有界内存集合没有会话正文，也不提供跨进程持久去重；原 canonical delivery 去重仍属于 Hepta。

### 3. 将有界 JSONL 解码移入共享库

`MediaCommandDecoder` 保留 caller 拥有的半行字节，包含换行的单行上限 64 KiB，每次只消费一条命令。取消/其他 select 分支不丢 UTF-8 半字符；非法 JSON、未知字段、过长行与不完整 EOF 进入 terminal，不能扫描到下一行继续执行。

同一个实现已被实际 `local_speech` reader 调用，不是孤立测试中的第二份解析器。writer 原始 I/O 或 join 错误也保留到终态诊断，不再全部压成一个通用“closed or stalled”。原媒体进程故障脚本与启动说明一并更新了连接参数。

### 4. 验证层级

本轮测试针对真实缺口：相同标签的旧/未来连接、每个 stream 身份维度、零 ID、跨操作重放、任意乱序/缺号、容量耗尽、逐字节中文与 emoji、双命令同读、64 KiB 边界、非法/不完整输入。原 session、store、speech segmentation 和 pipeline 取消/错误测试按实际执行日志保留。

这证明软件入口和选定组合的行为；不证明签名者是真实用户、不证明 OS 麦克风权限、不证明模型识别质量或用户听到。`local-speech` 中明确需要模型/设备的 ignored tests 不得算通过。

最终执行使用仓库要求的 Rust 1.99.0。实际测试的本地提交为 `70e29b500698dc93cbb56af0d10c848517d69e48`，发布提交为 `b7e4c008c35036ac1b209dca97f6b737824f29c0`；两者完整 Git tree 均为 `a0da019c63ba0e413ca45307a54cb6306bb20393`。CLI 缺少 GitHub 凭据后，使用已授权 GitHub Git Data API 发布相同 blob/tree，随后通过独立 `git fetch` 核实一致；提交元数据不同不改变测试源码。

| 本轮检查 | 结果 | 精确范围 |
| --- | --- | --- |
| `pipecat-core` | 387 passed | 既有 Core 单元、集成及文档测试 |
| `pipecat-hepta` 默认 feature | 35 passed | 含本轮 12 个命令边界回归 |
| `pipecat-pipeline` | 80 passed | 含 2 个文档测试；三 crate 总计 502，0 failed，0 ignored |
| `pipecat-hepta --features local-speech --all-targets` | 38 passed，0 failed，2 ignored | 原生依赖与所有 adapter targets；与默认测试重叠，不能相加 |
| native Clippy / example build | exit 0 / exit 0 | Clippy 使用 `--all-targets -- -D warnings`；真实原生 example 构建 |
| 实际二进制启动拒绝 | 5/5 | 缺失、零、非数字、溢出 connection，以及有效 connection 后缺少 config；全部在设备打开前完成 |
| 格式与脚本 | passed | workspace edition 2021 的 rustfmt；故障脚本 AST parse；首页 README 0 bytes |

两个 ignored case 分别要求真实中文 Whisper/Piper assets，以及受信配置和授权物理输出。没有把它们改成 mock 或标记成功。原生可执行文件 SHA-256 为 `2524fcbfb94f127753c89489733e5c9e5f29f4fee5b657ed2d2811879d08cbc4`；这个指纹只绑定本轮构建，不证明物理播放。

保留了两次重要失败：初次严格解析测试发现 serde 的单位 stop 变体会接受额外字段，改为空结构体后完整复跑；首次 native link 缺少任务 sysroot 的 ALSA symlink 目标，接到现有真实系统 ALSA runtime 后重建通过。原失败日志没有删除，也没有绑定成最终源码的失败。格式检查曾误用 edition 2024；按 manifest 的 edition 2021 重新检查通过，原检查没有修改源码。

这些检查不覆盖 62 个 crate 的完整 workspace gate。历史完整 gate 曾记录 **3,477 passed / 1 failed / 55 ignored**，失败为 WebRTC 重复 shutdown 后 UDP port 泄漏；现有导出资格仍将其保留为未关闭的 full-release blocker。本轮选定 Core 测试通过不能清除此项，也没有据历史失败推断本次最新源码运行必然失败。[原历史记录](../../docs/DRAFT_QUALIFICATION.md)

## 不能跨越的所有权边界

| 事项 | 唯一真实来源 | Rust HMI 的职责 |
| --- | --- | --- |
| thread / conversation / canonical delivery | 原 Agentd、App Server、AuthBus/Core queue | 使用原 ID，不建另一套权威会话数据库 |
| 权限、issuer、key epoch、device policy、撤销 | 原 owner 当前 authority | 在实际效果边界复验，标签不签发权限 |
| 用户观察、音频来源 receipt | 原签名观察入口和受控媒体 owner | 提交真实识别结果与受控 PCM 引用，保留不确定结果 |
| 取消、恢复、工具副作用 | 原 turn/operation owner | 先停止旧本地输出，再请求原操作取消/对账 |
| 原始音频 | 原 owner 的受控媒体目录、quota、capture generation | 引用精确采样区间与 digest，不复制成 neuron 或会话正文 |
| 只读 Organ | `TrustedReadOnlyOrganV1` | 保持无 I/O、无 capability、无模型/外部效果 |
| 神经自适应控制 | 未来明确的 shadow/评估 owner | 当前保持 inactive；权限、隐私和紧急停止始终确定性控制 |

Pipecat 通用 runner 每 session 建立 fresh context，不能原样替代 Hepta 的已有 thread 恢复。Pocket4 旧 Python voice 服务当前存活，其历史独立 SQLite voice ledger 只能按原 owner 的导入/保留规则处理，不能成为第二个写入权威。

## 按用户要求的顺序推进验收

| 顺序 | 已有可确认基础 | 剩余决定性验收 | 本轮判定 |
| --- | --- | --- | --- |
| P0：同 thread 文本 | 已有真实 Agentd/App Server 进程 fixture；签名 admission、增量显示、原 delivery reconcile、取消和去重 | 当前真实 owner/用户/模型的同次运行；重连后的完整显示恢复。当前 HMI 没有 token replay cursor | 软件实现已有；当前生产资格 HOLD |
| P1：麦克风至扬声器 | Rust CPAL/Silero/Whisper/Piper、来源保留、签名音频入口、泛型 media bridge | 原 owner 实际组合媒体 child、当前设备 grants、观察签名者；真实中文发言进入同一 thread 并输出到实际扬声器 | HOLD；Agentd 当前仍注册 TextPort + NoAudio |
| P2：多片段与媒体 | owner 多 AgentMessage 完成态聚合，16 KiB 总文本、4 KiB synthesis segment、250 ms PCM write | 多个真实输出片段/音视频双向与同步。native WebRTC 目前 audio/data，视频、摄像头、屏幕、PLC/FEC、adaptive jitter、ICE restart 等仍缺资格/能力 | HOLD |
| P3：设备、权限、恢复 | 既有中断、source 引用、进程故障和恢复 fixture；本轮接收端连接/请求 fence | 拔插、OS 撤权、驱动错误、owner/media 崩溃、结果 UNKNOWN、原 ID 对账、老 scope 拒绝 | 分项基础成立；生产组合 HOLD |
| P4：跨平台和用户感知 | 历史 Linux 部件及少量模型观测 | Linux/macOS/Windows/目标设备同负载、真实录入、播放、噪声/回声、长期资源及 p50/p95/p99 | NOT_RUN；不以编译/合成音频时间替代 |
| P5：neuron shadow | 确定性控制路径可作为未来对照 | 固定事件/预算/任务的影子建议、误中断/漏中断、延迟/质量/成本增量与负迁移 | INACTIVE；未授予写权限 |

需要恢复的实际入口是原专属云执行环境、原 Supervisor/Agentd 的已配置 identity 与 thread、独立签名 producer 与 replay witness、当前 device/organ/capture generation 和 OS grants。不存在这些真实输入时，创建一组 qualification 常量不能关闭生产门槛。

## 环境与未关闭的风险

本轮原专属环境没有 callable executor。可达主机的有界只读检查也没有定位原 Rust 1.99.1/ALC897 HMI checkout；无法把声卡型号推定为某个主机。当前 scratch 的精确 Rust 1.99.0 编译属于工程预验收，不能记为原环境部署。

13 个只读远端 probe 中，12 个读回 succeeded/exit 0；desktop 的一个 launch_unknown 无法由 process_status 对账，保留 UNKNOWN，未重复派发。设备枚举没有打开麦克风、扬声器或摄像头。[原始环境账目](ENVIRONMENT_AUDIT_20261106.json)

历史 ALC897 DMA 与 CPAL callback 提交确有记录，但同次记录没有 Agentd，音频插孔状态均 off，声学相关不足。Whisper base / small-q5_1 的真人文件试验仍有中文词错。不能把不同环境、来源和源码的这些成功片段拼成一次端到端语音通过。

当前 endpoint 的 source `spawn_blocking` cleanup 等待没有底层文件 I/O 总截止时间；异步 deadline 不能抢占 native C 或设备驱动。生产 supervisor 的强制收敛及清理终态必须单独验证。65,536 请求预算后的安全轮换也尚未有生产 launcher。原一参 CLI 调用者必须同时更新，否则明确拒绝启动。

本轮最终交付为可审查 Rust Core 增量、全范围差异清单、固定 owner 契约与真实证据分类。生产 Organ、全 Pipecat parity、跨平台 E2E 和 neuron 收益均保持未通过。
