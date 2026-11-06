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

完整源码 manifest 的 `manifest_sha256` 是脚本定义的规范内容摘要，`source-before.json`／`source-after.json` 原文件的 SHA 是其实际 JSON bytes 摘要，二者不是同一个哈希概念。原回收清单另外列出14份完整 source manifest 的原远端位置与文件 SHA；这些原件已与 pin r2 的一对 manifest 一起另包交付，不把本包38个文件扩称为已经包含它们。

## 完整 source manifests 与 pin 第二轮

[source-manifests-and-pin-r2.tar.xz](source-manifests-and-pin-r2.tar.xz) 包含 **22 个原文件、7,869,197 bytes**，其中包括16份完整 source-before／after、pin r2 的原 Cargo log／gate／preparation／environment、实际 `run_full_gate.py` 及原回收 manifest。Archive 为203,088 bytes，SHA-256 `07d7a94198f9bea16309c5c832fa6d75e3e7469afd2c0392bcbdb880cdbe70d0`。

对原 index 所列21个文件逐项核对 bytes／原文件 SHA，另外重新计算16份 source map 的 canonical digest；两种摘要均匹配。第22份原 manifest 自身由 archive SHA 及独立交付 receipt 绑定，没有递归伪造“自校验”。[原 index](source-manifests-original-index.json) · [交付核对](source-manifests-delivery.json)

精确 Rust 1.99.0 的 default r2 实际源码为8a7c065，原命令exit101、源前后不变，232.660532秒。原 Cargo log为 **602,626 bytes**，SHA-256 `b0c28d096e751ed6ab185c7c16c9349fc7690ac70542e15e28dd9b08d7c48ecf`；此前观察到的607,265 bytes是带末尾gate JSON的runtime stdout，不是该文件大小。独立从原bytes提取得到442 completed suites、5,210 passed／2 failed／16 ignored，失败位于RTVI first-answer-timing。它仍是中途停止的一次完整命令，不是全workspace通过。[本轮原计数](pin-full-r2-counts.json)

## 默认 r3／r4、完整补充与诊断原件

[native-through-default-r4-r1.tar.xz](native-through-default-r4-r1.tar.xz) 新回收83个索引原件，共10,265,256 bytes，另含18,344-byte原导出index，所以archive实际为**84个members、10,283,600 uncompressed bytes**。Archive本身407,932 bytes，SHA256 `72d5de97465c18df89f9ce261f609d25568b0de19c83d3abb141396b1350c890`。

内容包括最早e526 prebuild的原E0004日志、default r3／r4、原完整default no-fail-fast、Grok两轮诊断与完整syscall trace／binding、12份完整source before/after、原Rust1.99.0恢复收据、实际runner及旧target受控退役记录。活动中的expanded阶段不在此包，二进制target／models也没有复制入包。

Root逐项核对83个原件的size／SHA、archive内index与外部原index的字节一致性，并重新计算全部12份source canonical摘要。随后从原Cargo bytes按每个Running／Doc-tests边界独立计数，每块必须恰好有一个terminal summary，未发现缺项或嵌套summary；结果与另一份独立派生统计一致。[原index](native-through-default-r4-r1.original-manifest.json) · [原回收收据](native-through-default-r4-r1.retrieval-receipt.json) · [交付清单](native-through-default-r4-r1.delivery.json) · [Root核验与逐命令计数](native-through-default-r4-r1.root-verification.json)

| 原命令阶段 | Source／Rust | 实际秒数 | Completed harnesses | Pass／fail／ignored | 原结果 |
| --- | --- | ---: | ---: | ---: | --- |
| 最早prebuild `--no-run` | e526aab／1.99.1 | 833.000812 | 0 | 0／0／0 | exit101，编译E0004，未执行测试 |
| default r3 | ae134e9／1.99.0 | 154.894659 | 266 | 2871／1／0 | exit101，在Grok listener重绑停止 |
| default r4 | 272671b／1.99.0 | 329.030442 | 656 | 7329／2／17 | exit101，在SmartTurn pipeline停止 |
| default `--no-fail-fast` r1 | 272671b／1.99.0 | 324.642713 | 755 | 7748／4／33 | exit101，全部目标与doctests继续执行；3个SmartTurn targets共4fail |

上述四个命令的source-before与after均相同。r3／r4两个原Cargo logs分别342,751／839,226 bytes，与包含末尾gate JSON的runtime stdout区别保存。最后一轮完整执行仍是失败；不用7,748通过抹掉4个失败，不与旧轮相加为独立覆盖。Grok重复诊断与strace是不同运行，亦不计入这张表的命令总数。

## 首轮完整扩展矩阵与 SmartTurn C API 原件

[expanded-and-reference-pin-r1.tar.xz](expanded-and-reference-pin-r1.tar.xz) 包含 **112 个原件、19,978,439 bytes**，另有39,183-byte原manifest，所以archive为113个members、20,017,622 uncompressed bytes。Archive为 **1,071,708 bytes**，SHA256 `a3442d8ccaa03134c124c58893102635346a7541d64eb3a9c13395c30cc74982`。包内保存普通examples build、原all-features/all-targets、唯一同配置no-fail-fast、strict Clippy、原fmt-all与owned格式，以及C1的实际脚本／输出／原source maps。此前已交付的default补充不在此包重复保存；C2是后续独立运行。

Root核对112个原件的size／SHA、archive内manifest的原字节和14份完整source maps的canonical摘要。原两条测试命令分别219／731个harness，每块恰好有一个terminal summary，独立结果与另一个统计实现一致。实际command elapsed来自各自gate start/end，不用wrapper或控制RPC等待替代。[原manifest](expanded-and-reference-pin-r1.original-manifest.json) · [交付记录](expanded-and-reference-pin-r1.delivery.json) · [Root逐命令核验](expanded-and-reference-pin-r1.root-verification.json)

| 阶段（source272671／Rust1.99.0） | Exit | Command秒数 | Harnesses | Pass／fail／ignored |
| --- | ---: | ---: | ---: | ---: |
| all-features普通examples build | 0 | 193.519372 | 不适用 | 0 runtime tests |
| 原all-features/all-targets | 101 | 925.479220 | 219 | 2175／15／0 |
| 同配置no-fail-fast | 101 | 482.644572 | 731 | 8839／24／63 |
| strict all-features/all-targets Clippy | 101 | 123.712347 | 不适用 | 不计测试 |
| 原fmt-all | 1 | 16.610195 | 不适用 | 不计测试 |
| 精确62个owned members格式 | 0 | 14.473070 | 不适用 | 不计测试 |

原扩展命令在bot_audio_response的1pass／15fail停止。补充完整执行后还有bot_audio_smart_turn 8/1、user_speech 15/1、Grok app_cli 10/2、cartesia_turns_eager 33/1、SmartTurn pipeline 0/2、VAD lib 17/1、SmartTurn analyzer 2/1，共8个失败targets、24个失败case。原始失败、修补fixture和新source复验保持不同身份。

SmartTurn C1直接使用三份原golden feature输入同一ORT库；全部图优化时三份概率均未过原1e-5。C2分别关闭／仅启用基础图优化，每级三次Run，两级的三项输出分别bit-exact相等，但speech／tone仍失败，silence才与reference精确相同。各probe执行exit0，与comparison wrapper exit1区别记录；无Cargo或参数扫描。C2并未重跑C1的ALL。[原C1](smart-turn/ort-reference-pin-r1/reference.json) · [原C2](smart-turn/ort-levels-pin-r2/levels.json) · [Root对三份stdout的独立核对](smart-turn/c-api-root-verification.json)

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
