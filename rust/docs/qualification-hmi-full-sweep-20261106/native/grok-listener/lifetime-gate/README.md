# Grok listener fixture 生命周期门：冻结证据交付

本目录供复制到 `rust/docs/qualification-hmi-full-sweep-20261106/native/grok-listener/lifetime-gate/`。这是证据打包，不是新的执行轮次。

## 当前状态与历史记录

Root 已将最终候选应用到工作 checkout 的 `rust/crates/pipecat-grok-realtime/tests/app_cli.rs`：41,107 bytes，SHA256 `b7e2420e488e7c2a348bfa7084bba4e6dde1376a2099b1d530efb3956bdf1913`。截至本交付生成时，修改后的固定 Rust 1.99.0 native 编译和原 harness 复验尚未执行；后续发布与 gate 由 Root 统一管理。

原 `DESIGN.md`、`audit.json` 和 immutable manifest 中的“未应用／待发布”是文件生成时的 scope，原文保留。它们没有被重写为执行结果。原 full-workspace r3、diagnostic-r2 与 traced 首轮的三次 rebind 失败继续保留，各自没有被本修补或受控实验倒推为已定因。

最终方案让 fixture 创建的每个实际 child 持有共享生命周期许可，直到 collect 已观察实际 reap 或 Drop 的 wait 路径结束。单调 reap-failure flag 在读、写 admission 两侧都检查；发生 wait 错误后不能继续准入 child 或判定 listener 释放成功。writer 等待全部潜在继承者回收后，执行原 listener drop、原单次 bind，并在同一 writer 内关闭 probe listener。

这增加的是跨 case fixture 清理屏障。12 个 case 与 live child 仍可并行；原 60 ms no_connection、产品期限、协议和断言不变，没有重试 bind。现有 WAIT 仅限定新增 read/write admission；既有同步 Drop child.wait 没有因此变成全面有界的 cleanup。

## 完整冻结归档

- `frozen-evidence.tar.xz`：89,444 bytes，SHA256 `2ab1f9fd30d57085d688f5010e5048e50968e5debb49f8bfd8c5ae6b07f848d3`。
- 归档含原 manifest 的 28 个文件，总原文字节 463,209，保留每个 `files[].path` 的相对层级。
- `immutable-backup-manifest.json` 单独提供；它不被纳入自己的 28 文件列表，不做递归 self-hash。该原文件 SHA256 为 `5e14937ab49e1c759aab6f8214a8cdfd6b72e604a5cdb02f5cb97ce8f1968c0e`。
- `archive-verification.json` 记录解压后逐 member 的 bytes、SHA256、Git blob identity，以及每个易读副本的原路径和精确字节对应。28 项全部与原 manifest 一致。

打包仅将 tar header 的 uid、gid、mtime 规范化；文件内容没有变化。7 份 primary source 原文及其原 copyright/license 文本均完整保留。它们对应明确的参考版本，不冒称实际 desktop 的 Linux/glibc 构建。

原层级包含被否决的短 spawn-only v1、最终 v2、全部 primary 原文、唯一一次受控实验及旧/最终两阶段格式收据。v1 保留为被否决方案，不能当作已发布修复或 runtime 资格。

## 易读副本

| 路径 | 内容与边界 |
| --- | --- |
| `DESIGN.md`、`audit.json` | 最终 v2 设计与静态字节审计；生成时仍是未应用候选 |
| `primary-boundaries.json` | 固定 Rust 1.99.0 与明确 Linux/glibc/Tokio 版本的原始来源、调用顺序及限制 |
| `controlled-experiment/controlled_inheritance.py` | 只执行过一次的独立受控 fork/exec 脚本 |
| `controlled-experiment/execution.json` | 原 argv、退出与源码前后绑定 |
| `controlled-experiment/stdout.log`、`stderr.log`、`experiment.json` | 原完整输出；stderr 原为空文件 |
| `format-r2/` | 最终候选的 Rust 1.95.0 rustfmt check 收据和完整 stdout/stderr；不是项目固定 Rust 1.99.0 的编译、Clippy 或 runtime 资格 |

受控实验直接观察同一 owned child 持有父 listener 的原 inode，父 close 后阶段 bind 失败，exec 后原 FD 已关闭且 child 仍活着时阶段 bind 成功，最后实际 reap。两次 bind 属于预先定义的两个实验阶段，不是 Grok oracle 的重试。这个实验验证该受控机制，没有运行 Grok/Tokio，也没有补成历史失败的直接 child-inode witness。

## 核验

在此目录运行 `sha256sum -c SHA256SUMS` 可核对所有其他交付文件。`SHA256SUMS` 排除自身；原 immutable manifest、归档成员验证和易读副本保持独立。可用 `tar -tJf frozen-evidence.tar.xz` 查看完整原路径，或将归档解到独立目录后按原 immutable manifest 逐项核验。

本次打包没有修改候选、仓库源码或远端 source，没有另跑受控实验，也没有创建或更新 Git ref。
