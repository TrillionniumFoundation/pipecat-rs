# 原生 HMI 环境恢复证据（2026-11-06）

本目录记录本轮重新找回的原生环境、原文件身份与限定范围的当前输入检查。它不把历史日志、fixture 身份或目录存在性换算成生产闭环资格。机器可读证据见 [recovery.json](recovery.json) 与 [product-inputs.json](product-inputs.json)。

## 源码与原文件绑定

报告基线为 Pipecat `e526aabaf98522b2294e744ac48d75d9ed89a254`，owner contract 引用 Hepta `7bbdd715957e72274bc13209c4a619913e880129`。原 10/29 项目在 desktop 的 `/home/qian-qi/workspace/hepta-hmi-20261029` 找回：Pipecat checkout 是 clean `ec40f982205cb2c87f61094d78d163c5c2febb39`，Hepta checkout 是 clean `7bbdd715957e72274bc13209c4a619913e880129`；tree IDs、branch 和 probe job 记录在 JSON。

3 份历史日志重新计算 SHA256，与 pinned 10/29 发布证据完全相同。7 个 Whisper、Silero、Piper、ONNX Runtime、eSpeak 库文件重新计算 SHA256，与恢复配置中的预期 digest 一致。这里的 7 个对象是普通文件；此环境探针没有重算 eSpeak 数据目录整体 hash。原模型是否被当前生产 owner 采纳，以及新源码运行结果，不能从这些文件身份推出。

## 设备与真实 owner 的边界

当前 codec 为 Realtek ALC897。所读 ALSA 控件中，Front/Rear Mic、Line、Line Out、Front Headphone jack 均为 off；SPDIF Phantom 的 on 不是麦克风/扬声器声学路径证明。没有开麦、录音、读取真实用户 PCM 或播放音频。

限定查询只检查可读 `/proc/*/comm` 中匹配 `agentd|codex-hepta|hepta-super|pipecat|local_speech|app-server` 的项，以及选定 systemd unit 名称；未命中不能推广为整机没有 owner。5 个 HEPTA 环境变量的 absent 结果只属于记录该检查的 SSH job 环境，没有读取其他进程的任意环境或凭据。

原 HMI 模板目录存在，但模板不是生产授权。恢复的 `qualification:owner`、`qualification:offline`、`qualification:alc897` 和 generations 1/2 是历史独立测试配置，本轮没有把它们接成生产身份。仍需同一次运行的真实 Supervisor agent/current spawn generation、原 thread、注册签名 producer 与独立 replay witness、当前 device/organ/capture generations 与 grants、owner 采纳的不可变模型和私有媒体目录，以及实际麦克风/扬声器。10/23 历史 cloud executor 的可调用入口仍未恢复；10/29 desktop 环境恢复不替代它。

## 11/04 工程证据的继续追查

对已观察目录 `/home/qian-qi/work/hepta-agentd-product-inputs-evidence-20261104` 的 `command-plan.json`、`environment.json`、`validation-results.json`、`source-after.json` 做了两轮限定公开字段提取，四份文件的大小与 SHA256 在两轮中一致。它们记录两个 cargo build、`cognitive_product_e2e`、`supervisord_product_e2e`、fix 和 fmt。两项 original test 阶段记录 exit 96 且 `fresh_junit=false`；这里没有读取原日志来推断失败原因，也没有重跑测试。

`environment.json` 只有 Rust/Cargo/V8 构建变量，`CARGO_TARGET_DIR` 指向历史构建输出。`source-after.json` 是两个 tests 源文件的记录摘要。这四个 JSON 没有提供当前 owner/thread、运行 unit、HMI socket、trust/checkpoint 或 audio-authority 关联。未读取的子目录与其他进程仍属未观测范围。

## 完整来源与中断恢复

| Host | 原始 job ID | 本目录使用的观测 |
| --- | --- | --- |
| desktop | `1a1b0a883db-fc05-71fd` | ALSA jack、限定 owner/process、工具与浏览器存在性 |
| desktop | `1a1b0a9ddc3-fc05-7205` | 原项目目录、源码 HEAD/tree/clean 状态 |
| desktop | `1a1b0ac828f-fc05-7218` | 3 日志与 7 模型文件 hash、配置公开身份、普通 sandbox 的 about:blank 启动 |
| desktop | `1a1b0b36328-fc05-7240` | 该 job 的 owner-input-presence 子阶段产物；5 变量和2指定目录存在性 |
| desktop | `1a1b0c06bde-fc05-7295` | 11/04 四 JSON 的公开结构与路径 |
| desktop | `1a1b0c3a2d8-fc05-72b1` | 同四文件的精确公开 build/test argv、阶段、记录源码摘要 |

除明确标为子阶段产物的一行外，上述来源均已按原 ID 取得 succeeded、exit 0、完整输出。普通 Chrome about:blank 探针只证明当时正常 sandbox 可启动；实际应用/IME/autoplay/断线资格由本轮其他 browser 证据记录。

本地 executor 曾暂时断连，恢复后保留文件仍在。按中断前 SHA256SUMS 逐项重算的 18 份环境文件全部 digest 与 bytes 相同；再次读取原 native job 的 14,812-byte stdout，日志、模型摘要、浏览器和 unit 字段与保留证据一致。该校验清单完整写入 recovery.json；没有把断连写成文件丢失，也没有重放 job。

更早的 desktop `1a1b01f09b3-fc05-6e3b` 与 ai395 `1a1b04f07ab-fc05-6fa9` 仍是 UNKNOWN：以原 ID 对账返回 `INVALID_ARGUMENT / FileNotFoundError`。这两项均没有自动重试或重新提交，后来的成功 probe 不清除它们。
