# 执行入口变化与证据可用性

本轮本地执行入口的前两次中断分别处理。第一次恢复后，18 份已写文件按先前记录逐一核对原大小／SHA；第二次恢复入口下，原 scratch 路径不再可见。从已发布 Git commit `7105367d7d607653c3ab758f0131c029c4373faf` 新克隆的树为 `19a5c08fc2cbfeabacd344668fc7b25d03a282c2`，恢复开始时工作树干净。这个事实只说明当前可见性，不能推定所有旧数据永久丢失。

第三次入口切换于 14:02 UTC 恢复检查时，当前 checkout 与文件仍在；已发布 `8279c5566c861fc047b7e36f4a44524e34c00767` 的 59 份证据／文档逐一对上原字节长度和 SHA，主报告工作稿也与独立 Git blob 一致。只对这些已检查路径作出结论，不推定未观测路径状态。[第三次恢复收据](third-entrance-recovery.json)

第四次短暂 transport／executor-key 中断后，原文件仍可读。以中断前保留的完整manifest重新核对71个browser候选证据／文档的size、SHA256与Git blob SHA，全部一致；另核主报告、AISHELL探针说明和原标注字节。它们当时是未提交候选文件，不误写成272671已包含这些68份新browser证据。没有为此重建71文件或重放desktop job，也不将此次可见性推及未观测路径。[第四次恢复收据](fourth-entrance-recovery.json)

远端 durable job 具有自己的原 host/job ID。当前本地目录消失没有取消或重新执行远端作业；对其权威状态、原文件和完整输出分别读取。若重新执行某项 gate，使用新目录、专属 target 和新运行身份；新结果不冒充旧 raw。

| 材料 | 当前可核对范围 | 不作出的替代 |
| --- | --- | --- |
| 浏览器最终 runtime 与历史失败 | 原远端 JSON/log 和完整工具回包的正文回收，落盘后核对原 size/SHA；原 helper 的完整正文恢复，input-r2 单行重建与原 SHA 精确一致 | 不用新 JSON 伪装原结果；远端 application_source_present=false 保留 |
| 原 WebRTC 60-file archive | 4 个原文件精确恢复；[availability.json](webrtc/availability.json) 逐项列其余 56 个 missing | 原 README/index 的结果摘要不是缺失的两次 SIGKILL、Clippy、40 次 inherited-binary raw log、manifest 或原 checksum |
| 原 workspace format 证据 | 7 个格式修补已发布在 `dc7fdbd6b6c70bdffa5aadd52671eaf0f8b3334a`，源码可逐字节审阅；原检查 exit 与摘要曾实际观察 | 旧 35 文件目录、完整 2,546,224-byte fmt-all log、后来 37 文件 archive 当前没有可访问副本，不声称已交付 |
| 原 browser native gate | 本地曾观察 54 tests、native/WASM strict Clippy 与构建结果；远端实际使用的 bundle/资产哈希和打包收据已恢复 | 不把缺失的旧 native raw logs 写成可下载；新的 desktop 精确 pin 补资格独立记账 |
| 新 browser Rust／WASM 补资格 | 新身份下的54 tests、native strict Clippy、首次WASM下载失败及独立r2 WASM strict Clippy，48原文本与完整export均已按原size/SHA交付 | r1整体失败不被两个native phase通过或r2成功覆盖；不重复计算54项，也不补写r1未落盘的tool binary前后摘要 |
| Provider Cargo 产物探针 | 原执行脚本和当时完整打印 stdout 已有 Git blob，并恢复在本目录；最终 13 文件重建有独立 static validation 与完整新 SHA | 原临时 Cargo JSONL/stderr 当前不可取，不由脚本推导并补写成原 raw；重建是新检查点 |
| 完整 workspace／SFU／模型原始输出 | 保存在 desktop 各原 evidence 目录，由同 job 身份对账并回收；主报告给出最终交付位置 | source manifest、runtime log、状态回执分开；构建 exit 0 不计为测试执行 |

原 fmt-all log 的已记录 SHA-256 为 `ef35829143bc20d4bf8bd7e0af896cd9562e060f762bdd8904c5da75e09079f0`；旧 37-file archive 的已记录 SHA 为 `e75fc1c2ecaa7defd8e922cb4fa933cb2eb95e77e75db9f600f120a786e8abf9`。这些仅是可供以后追回原文件时使用的记录，不证明当前文件可用。新的全 member 格式检查另记运行，不能修复原 `cargo fmt --all` 对 vendored path dependencies 的失败历史。

第一次与第二次入口事件的说明来自该次工具返回和恢复观察；它不等同于环境 JSON 对物理 desktop 的模型／codec 探针。两个旧 UNKNOWN job 仍保留原 ID，对账失败不自动重放。整个恢复过程中没有根据摘要生成“原始日志”、修改锁定依赖、凭空建立生产 owner 或把旧 fixture generation 升为当前 grants。
