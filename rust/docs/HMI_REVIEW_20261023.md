# Pipecat / Hepta HMI 第四轮复核

本轮在原保存的 Codex 云环境继续，实测工作目录为 `/workspace`，
Hepta checkout 使用 Cargo/Rustc 1.95.0，Pipecat 使用 1.99.0。
两个 checkout 的 `.agents/skills` 均不存在；遵守 Hepta 根 `AGENTS.md`。
`/dev/snd` 不存在。没有切换到用户电脑、dot 工作区或 GitHub Actions。

原候选仍是 Pipecat `3f616e743619277e64dff499d870c826b58a89c2`
和 Hepta `3741d927273a3acecda02be39c41998834c1e732`，分别对应已核验的
`work/hepta-hmi-20261023` / `work/hmi-jsonl-20261023` 原分支缓存。
继续的分支是 `audit/hepta-hmi-cloud-20261023` 和
`audit/hmi-jsonl-cloud-20261023`，前三轮提交、120 条执行记录和三个归档保留。
本轮没有抓取或更新远端分支；缓存引用不代表再次查询后的远端最新状态。

## 八项严格 Clippy 错误

下列路径均相对 Hepta 的 `codex-rs/hepta-agentd/src/`，行号为修复前位置。

| 文件 / 行 | lint | 与构建的关系及最小修复 |
| --- | --- | --- |
| `automation_effect_host.rs:58` | `large_enum_variant` | Agentd 库始终编译，`state_control` 消费效果对账结果。仅将 observed receipt 放入 Box，在原 snapshot 转换处解包。 |
| `cognitive_context.rs:105` | `too_many_arguments` | 原 cognitive read 控制路径。将原八个输入放入借用型 `CognitiveReadInput`，保留全部 gate。 |
| `cognitive_context.rs:507` | `too_many_arguments` | 原 final-use revalidation 路径。将九个输入放入 `CognitiveRevalidationInput`，更新真实调用和原测试。 |
| `cognitive_retrieval_learning.rs:52` | `too_many_arguments` | 原 learning sink 实现始终编译。按 delivery context/policy 组合原参数；移除了旧 policy 方法的 allow，没有增加 suppress。 |
| `intelligence_product.rs:584` | `large_enum_variant` | 原 canonical runner 的 Ready 返回值。仅 Box prepared payload。 |
| `intelligence_product.rs:593` | `large_enum_variant` | 原 Agentd admitted outcome。仅 Box prepared field，保留 run receipt。 |
| `intelligence_product.rs:722` | `large_enum_variant` | 原 ledger error；构造器位于 legacy qualification feature。仅 Box pending append，显式执行该 feature 回归。 |
| `plasticity_host.rs:704` | `too_many_arguments` | 原 `plasticity_runtime` 调用。将七个原 store/verifier/writer 引用组合为 `AgentdPlasticityProposalContextV1`，保留 admission 重算和写入顺序。 |

这些是未发布 crate 的 Rust 类型/参数形状调整，工作区调用已更新；没有修改
Agentd wire、SQLite 或签名 schema。原严格命令返回 0。扩展到全部 Agentd
测试目标的默认 profile 和 legacy qualification profile，仍以
`-D clippy::all` 执行，也分别返回 0。`dead_code` 等原有非 gate warning
仍保留，不能将本结果称为 `-D warnings` 通过。

扩展检查还定位并修复了三个旧测试辅助模块的 29 项 `expect_used`：
automation effect host、plasticity process、kernel evidence。
辅助函数改为传播带上下文的原错误，保留断言和原 fixture，不增加 allow。

legacy 回归另暴露了旧测试未提供现已必需的签名 evaluation、未传当前 clock，
以及旧 decision 转换遗漏隐式 abstain / slow-path 控制结果的问题。
测试复用既有合法签名 fixture，转换按仓库既有 shadow 契约保留控制结果。
没有伪造签名、向签名后的 legal-action 集合添加动作或降低账本完整性检查。
该 qualification 路径没有因此成为 HMI 部署依赖，也未启用 neuron 学习控制。

## 新一轮来源与边界复核

重新阅读原 owner / audio / recovery 实现，并用实际失败复现确认三处遗漏：

1. 即使 text/audio 使用相同身份字符串，也不能切换附着会话的授权 profile。
   现在每次提交绑定原 profile、key epoch 和精确 audio-policy digest，之后才允许 arm。
2. Core 尚未连接时不应消费音频的首次投递 attempt。现在先连接并刷新当前 owner/
   issuer，再 claim；已可能送出的请求仍按原 ID 只读对账，不重新执行。
3. 音频 observation 已提交后，checkpoint 或授权刷新仍可能失败。现在错误保留
   `committedAudioObservation`，并明确 `admissionReady:false`。这份回执没有被
   提升为 Core admission、模型完成、设备播放或工具效果。

两项进程复现与 lease 的 SQLite 复现，在旧实现上的失败日志均保留。
复核同时确认：签名覆盖独立 audio domain 的完整来源；当前 issuer/thread/device
policy 在异步读取、投递和呈现边界复验；capture sequence 不能跨 thread 重放；
历史 observation lookup 不新增 admission；文件范围、链接、所有者、样本区间和摘要
按原受控媒体边界校验；取消先退役本地 output epoch，再请求原 queue/turn 的取消。
工具的 uncertain effect 仍沿原 operation 对账；没有将停止播报写成撤销工具效果。
详细逐项来源与证据在云目录 `owner-v4/source-review-ledger.md`。

## 实际测试终态

所有记录位于 `/workspace/pipecat-hmi-audit-20261023/owner-v4/`。

| 验证 | 终态 / 日志 |
| --- | --- |
| 原严格 Clippy 命令 | 0；`strict-clippy-complete-review-source.log` |
| 默认全部 Agentd 测试目标严格 Clippy | 0；`final-strict-default-all-agentd-tests.log` |
| legacy feature 全部测试目标严格 Clippy | 0；`strict-all-agentd-tests-qualification-complete.log` |
| Agentd 单元测试，含 legacy 构造路径和 lease 复现 | 172/172；`complete-unit-regression-all-reviewed-fixes.log`，后续依赖组合运行再次通过全部 172 项 |
| 原 AuthBus、HMI、签名 audio、真实 owner 进程重启与压力 | 12/12、无跳过；`complete-original-owner-audio-lifecycle-regression.log` |
| 受影响调用链组合 | 原运行 185/186，4 个既有 profile/provider 测试跳过；`affected-owner-callers-unit-and-process-regression.log`，保留 exit 100 |
| 上项唯一失败的 exact-cut / writer-lease 回归 | 修复后 1/1；`cognitive-exact-cut-and-writer-lease-regression.log` |
| 无秘密模板 | JSON 语法、实际源码字段清单及无效占位状态检查通过；未运行 native load、签名或安装 |

组合运行中的 cognitive 恢复测试原来把取得 writer lease 前后的 cut 断言为相同。
writer lease 本身会写入账本。修正后的测试先拒绝错误 cut 并证明原数据库未被改写，
再核对正确恢复后的 owner、schema、lease ID、generation 和当前 authority。
原 semantic write、invalid-write rollback、撤权断言全部保留并执行通过。
生产 recovery 实现没有改动。已通过的其他 185 项没有为此重复运行。
依赖进程路径的实际覆盖为 cognitive 9、kernel evidence 3、plasticity 1、
exact-cut writer 1；不能将原失败组合命令改写为 exit 0。

原来 4 个跳过是两项仅适用于 `qualification-cognitive-write` 的产品测试和两项需真实
loopback/external provider 输入的 smoke。另一个仅在 cognitive-write feature 编译的
writer 测试不在本轮所选 profile 中。没有宣称全 workspace / 全 feature / 商业 provider parity。
Pipecat 与 Hepta adapter 的 Rust 源码本轮未变，保留第三轮 42 项 adapter 回归结果，
未重复测试或将编译结果升级为设备验收。

40 次签名临时 PCM observation、8 次预热后的 32 个软件样本：发送至文本终态
p50/p95/p99 为 415121/478714/620428 微秒。FD 94→95、线程 54→55、
RSS 255160320→265187328 字节。原始样本、持久记录/媒体大小及实际 owner PID
更换记录在 `software-metrics.json`。该有限负载没有证明长期资源稳定性；延迟不含
PCM 准备、UI 连接、实体采音、STT、TTS 或扬声器播放。

仓库要求的最终 `just fix` 和 `just fmt` 在测试后运行；最终 source identity、
格式等价证明、补丁、bundle、完整命令记录及 SHA-256 以本轮 handoff 为准。
按仓库要求不在最终 fix/fmt 后重复运行测试。

## 接入准备及剩余项

Pipecat 配置模板：[local-speech.template.json](../crates/pipecat-hepta/config-templates/local-speech.template.json)。
对应说明：[config-templates/README.md](../crates/pipecat-hepta/config-templates/README.md)。
Hepta 的 `codex-rs/hepta-agentd/config-templates/hmi/` 包含原 owner argv、公钥
注册表、device-authority 模板和完整运行材料清单。模板的占位摘要、零代际、空线程
allowlist、过期/撤销状态故意不能启用；没有私钥、checkpoint 假摘要或新设备授权。

下一步需要原有 Supervisor/owner 的实际运行标识、现有独立 producer 的签名接缝、
公钥与原 thread allowlist、独立保留的 checkpoint、实际 device/organ/capture 代际及
既有私有媒体 root；coordinator 已接入原 owner，不再泛称其未注册。
当前 Unix 文件所有权校验不能当成 Windows ACL 实现或验收。

可在本云环境继续的下一批验证是：获得可信、兼容的本地多语言 Whisper、选定 Piper
voice/model/config、ONNX runtime 和所需 phonemizer 清单后，运行真实模型的文件输入、
语言质量和进程崩溃测试。当前没有这些可信运行材料。本轮未生成生产凭证或猜测模型摘要。

Whisper 原目标
`https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.en.bin`
的 CONNECT 403 仍是原拒绝终态；未重试，也未通过镜像或其他环境获取同一被拒资源。
Piper 所选可信 voice/model/native 清单仍缺失。实体音频设备在本云环境不存在。
因此中文及目标语言实际识别/播报、噪声/回声、真实抢话、拔插、OS 权限撤销、真实
模型故障、完整麦克风至扬声器 p50/p95/p99、长期压力和真实工具效果对账仍未验收。
真实语音基线完成前，neuron 自适应收益/shadow 对照仍未运行。

本轮只保存本地审计提交及可复核材料；未推送、合并、部署、改变真实权限，
未触碰或发布 `work/native-parity-20261012` 的未推送提交。
