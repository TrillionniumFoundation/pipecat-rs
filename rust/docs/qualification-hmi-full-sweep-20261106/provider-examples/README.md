# Cargo example 产物身份修复

原完整 workspace（源码 `ae56473b029b81a8cb08ec2fd385aecd74287e9f`、Rust 1.99.1）在 AWS Nova Sonic 两项 compiled-example 检查中失败。85 个该库用例有 83 通过，另两项分别是帮助内容不符、等待记录结果超时；完整 Cargo 在该处结束，不能称完整 workspace 通过。

四个 package 原先都生成普通 executable `target/debug/examples/pcm_session`。对当时实际二进制的只读 `--help` 探针打印的是 **INWORLD_API_KEY / Uses InworldApp**，而调用方期待 Nova Sonic。探针前后同一文件 SHA-256 是 `00758511da2145137486b2817aa1a1a5be1ed8915b8934631e41d77b1d2bdefb`。这为覆盖问题提供了实际产物证据；不是仅依据文件同名推测网络超时。

修复在每个 package 的 Cargo.toml 明确 `autoexamples=false` 和唯一 `[[example]]` 名称，保留原源码路径：

| Package | 普通 example 名称 |
| --- | --- |
| pipecat-aws-nova-sonic | aws_nova_sonic_pcm_session |
| pipecat-azure-realtime | azure_realtime_pcm_session |
| pipecat-azure-voice-live | azure_voice_live_pcm_session |
| pipecat-inworld-realtime | inworld_realtime_pcm_session |

对应 README／帮助文字及 Nova 的 executable 查找同步更新，不保留同名 alias。Nova 原帮助标题、输入错误、精确输出、进程 reap 和 12 秒等断言没有放宽。13 文件变更的最终静态恢复记录见 [reconstructed-checkpoint.json](reconstructed-checkpoint.json)，发布在 `7105367d7d607653c3ab758f0131c029c4373faf`。恢复材料和 provider 协议／真实账号资格分开。

## 普通可执行文件与 test harness 必须分别核对

[真实 Cargo 探针脚本](cargo-probe.py) 与 [当时完整打印 stdout](cargo-probe-stdout.json) 观察到：

1. 默认 `cargo test --no-run` 会构建普通、未加 hash 后缀的 example。
2. `--all-features --all-targets --no-run` 构建的是带 hash 的 `profile.test=true` example harness。它不会保证替换此前普通 example；新 target 中甚至没有那个普通文件。
3. 因而仅用“all features/all targets 通过”不能证明另一个测试启动的普通 executable 属于同 feature profile。

最终扩展矩阵必须先执行 `cargo build --workspace --all-features --examples --locked --message-format=json-render-diagnostics`，记录 Cargo compiler-artifact 的 package/target/features/profile.test=false/executable 和实际 SHA，再跑 all-features/all-targets 测试，并复核普通文件哈希。这是产物身份准入，不是改动产品断言。

探针原脚本 blob 为 `381c17904fd9648a893a65566450fb58a9718551`，原打印 stdout blob 为 `9da1951a2e687b4e33da1798918466aa080ea9cc`。临时 Cargo JSONL/stderr 原文件当前不可访问，未重新拼装为原记录；最终真实构建会生成独立的新 compiler-artifact 记录。
