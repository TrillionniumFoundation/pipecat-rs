# Native 历史运行与最终矩阵证据

本目录把本轮各阶段的原始输出分开保存。完整最终矩阵以 [主报告](../../HMI_FULL_SWEEP_20261106.md) 为准；下表是最终候选之前的阶段，不能相加成一轮资格。

## 原始早期运行包

[earlier-runs.tar.gz](earlier-runs.tar.gz) 是新制作的确定性 archive，包含从原 desktop 目录回收的 **38 个原始文件、1,149,376 bytes**。每个 member 的原 size／SHA 在打包前重新核对，打包后再次逐字节核对。Archive 本身是 178,363 bytes，SHA-256 为 `dfafcc212bf6543623b2743339064476ce02f5aeab43438c6fcf892cced1c6b4`。它不是原传输包；原文件字节不变。

[交付清单](earlier-runs-delivery.json) · [原回收清单](earlier-runs-original-retrieval.json) · [逐命令计数](earlier-run-counts.json)

| 包内目录 | 源码／工具链 | 原结果 |
| --- | --- | --- |
| `full-workspace-r1` | 478ab3b／1.99.1 | exit101，InvalidPayload 非穷尽 match；0 runtime tests |
| `full-workspace-r2` | ae56473／1.99.1 | exit101，96 completed suites，637 pass／2 fail／0 ignored；Nova普通example身份碰撞 |
| `pin-prebuild-r1` | c45da3b／1.99.0 | exit0，`--no-run`，0 runtime tests；总847.51秒 |
| `full-workspace-rust-1.99.0-r1` | 7105367／1.99.0 | exit101，317 completed suites，3630 pass／2 fail／3 ignored；signed assistant helper错算wire user |
| `local-sfu-patch-r1` | ae56473／1.99.1 | 3 pass／0 fail；libtest scenario总16.42秒，Cargo build＋command总185.33秒，二者不混作媒体时延 |
| `chinese-synthetic-patch-r1` | ae56473／1.99.1 | 0 pass／1 fail，在scope quota准入失败；没有native加载或8条转写 |
| `chinese-synthetic-patch-r2` | ae56473／1.99.1 | 1 pass／0 fail，8条实际Piper→Whisper转写；Cargo command总107.41秒 |

完整源码 manifest 的 `manifest_sha256` 是脚本定义的规范内容摘要，`source-before.json`／`source-after.json` 原文件的 SHA 是其实际 JSON bytes 摘要，二者不是同一个哈希概念。原回收清单另外列出14份完整 source manifest 的原远端位置与文件 SHA；它们的后续交付使用单独 archive，不把本包38个文件扩称为已包含这些14份原件。

## 中文诊断的原结果

[原 JSON 行的独立提取](chinese-patch-diagnostic-observations.json) 保留8条转写和脚本输出的metric行；原始二进制log在archive中。完整原log含native输出交错形成的非UTF-8字节，保存时没有转码或replacement写回。提取仅按完整JSON行解析。

| Piper 输入 | Clean 转写 | Seeded-noise 转写 |
| --- | --- | --- |
| 你好，欢迎使用本地语音助手。 | 你好,欢迎使用本地语音助手。 | 你好欢迎使用本地语音助手。 |
| 请停止播放，保留当前会话。 | 请停止播放保留当前挥话。 | 请停止播放保留当前,挥话。 |
| 今天我们测试中文语音识别。 | 今天我们测试中文语音识别。 | 今天我们测试中文语音识别。 |
| 设备重新连接以后继续工作。 | 设备重新连接以后,继续工作。 | 设备重新连接以后继续工作。 |

两条“会话→挥话”替换属于本次合成端到端观测，没有把原因单独归给TTS或STT。原test断言为非空、来源字节／摘要、retention和release，不设置CER质量阈值。四句、每路一份样本不构成真人质量门槛。

每段 `submit_with_source(...).result().await` 的观察为8.598471–9.081217秒；它覆盖离线STT＋来源保留，不含前置TTS合成、设备、原Hepta或用户感知。RSS采样784,356–794,800KiB不是连续峰值监控。脚本n=8的p50/p95/p99是排序后第4／第8／第8个值，不是稳定的生产尾延迟估计。最终1.99.0候选会独立保存全部8条结果，不能以本轮1.99.1代替。

## 解释计数

所有计数均来自某一个原命令的 completed libtest summary。失败时未执行的后续 target 不补零、不推定通过。共享 `#[path]` regression graph 可以在多个harness内重复执行；数字表示实际测试执行数，不是全仓去重源码函数数量。显式ignored opt-in的通过、默认ignored、feature变体、预编译和压力重复也分开。

格式、模型与SDK资产、普通example compiler-artifact、实际运行状态和source before/after分别属于不同证据。成功退出、队列提交、真实远端媒体、实际device播放和用户听感没有互相替代。
