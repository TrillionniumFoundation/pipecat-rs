# Grok fixture 子进程生命周期门：待发布候选 v2

本目录是独立候选，未由作者写入 repository checkout 或 desktop source。基线是 `272671b95297313d24db921eceda8e95f3a8d545` 的 `rust/crates/pipecat-grok-realtime/tests/app_cli.rs`，基线 SHA256 为 `5f5d2cba7774aa4644cd7dd6dc67d86daf256223c78e65761705f59b67bf953f`。parent `52243a392065d474ea0d0aead585891c6010bb3d` 中该文件相同。

最终候选：41,107 bytes，SHA256 `b7e2420e488e7c2a348bfa7084bba4e6dde1376a2099b1d530efb3956bdf1913`。最终 patch SHA256 为 `ebce905890aeb4d67b7a0ed4b13f1dd2313e9295e1e9150dba2d1cad06eee78a`。完整机器审计在 `audit.json`。

## 实际证据与归因边界

新 strace 首轮的失败是 `actual_binary_failure_after_accepted_pcm_persists_exact_partial_only_after_join`。原监听器 `127.0.0.1:34901`、fd 28、inode 1121549341 在父进程 close 成功之后，替代 socket 的单次 bind 返回 EADDRINUSE。并行 case 的线程 696857 此时处于 clone3→exec 窗口；child 696897 的首次 execve 在 bind 失败之后才开始。clone 没有 CLONE_FILES，原监听器已标记 CLOEXEC。

原 trace 没有直接读取 child 696897 的 fd 28/inode，clone 入口也不标记内核复制 FD 表的精确时刻。因此这些记录支持一个具体、强匹配的继承窗口解释，但没有直接证明该 child 在失败瞬间持有原 inode。原 full-workspace r3、未追踪 diagnostic-r2 和本次 traced 首轮是三个独立失败；没有把本次解释倒推成前三轮已证实的根因。原始失败和限额/无截断记录继续保留。

## 为什么否决只锁住 spawn 同步调用的 v1

Rust 1.99.0 官方源文件 `library/std/src/sys/process/unix/unix.rs` 优先使用可用的 posix_spawnp；当前命令没有 pre_exec，正常 GNU/Linux 分支符合这个选择。fallback 分支则在 fork 后读取专用 CLOEXEC/error channel。完整主来源、版本、Git blob 和调用位置在 `primary-boundaries.json`。

但“spawn 成功返回”不能当作“每个继承 socket 的关闭都已完成”的通用 fence。核查的 glibc 2.39 Linux 实现使用 CLONE_VM|CLONE_VFORK；Linux v6.8 和 v6.14 的 `begin_new_exec` 都先经过 `exec_mmap`→`exec_mm_release`，后执行 `do_close_on_exec`。v6.8 的 `mm_release` 会完成 vfork_done，唤醒父线程。也就是说，vfork 父线程恢复与全部 CLOEXEC 关闭之间仍存在内核阶段边界；Rust 的正常 posix_spawn 返回路径没有再添加全部 FD 关闭握手。

这几个版本是明确列出的 primary reference versions，不冒称本次 desktop 恰好就是这些内核/libc 构建。它们足以否定“锁到 spawn 返回必然闭合所有继承 FD”的一般性证明。单个 error channel 的 EOF 也不是对所有其他 FD 的独立关闭观察。

v1 可消除 trace 中较大的 pre-exec 重叠，但没有据此发布或宣称根治。v1 的原候选、patch 和静态审计保留在相邻的 `grok-spawn-gate-candidate-r1/`。

## v2 的边界

`ChildOwner` 的 `.0` 仍是原来的 `Option<Child>`；新增 `.1` 持有 Tokio `RwLockReadGuard`。成功创建的每个实际子进程持有共享读许可，直至 `collect` 确认 reap 或 Drop 的 wait 路径结束。现有 `collect` 正文逐字节不变：它先观察 `try_wait(Some)`，然后才取出 Child 并读取最终输出。

Drop 保持实际 kill/wait 行为。若 wait 失败，先设置单调的 `CHILD_REAP_FAILED`，再关闭 Child 持有的句柄并释放许可。这一路径明确是失败状态；没有把 kill 请求或 wait 错误写成已回收。读、写两侧取得许可后都检查该标志，因此失败后不能继续创建新 child，也不能继续判定监听器释放成功。

`release` 保留原来的 60 ms `no_connection` 检查。在这之后，它以现有 `WAIT=10s` 等待独占写许可。取得写许可意味着所有经本 fixture 唯一进程创建入口产生的潜在继承者已经经过上述回收路径；失败标志必须仍为 false。然后执行原监听器的 drop 和原来的一次 `TcpListener::bind(address).await`。成功生成的检查用 listener 也在写许可内 drop，避免它成为下一次 spawn 的继承对象。原错误诊断完整保留在写许可之外。

这是一个显式的跨 case fixture 清理屏障。共享读许可允许多个 child 和测试主体同时运行；等待清理的 writer 会暂时阻止后续 child admission。它不把完整 case 排成串行队列，也不把实际 child 的退出替换为 timer 或安静期。它将 fixture 监听器重新绑定检查的前提明确为“所有潜在继承者已退出”，而不是依赖 spawn/exec 的短暂中间状态。

read 和 write admission 都使用现有 `WAIT=10s` 作为新增同步操作的明确上限。原产品 `3000 ms`、`500 ms`，原 `WAIT` 常量、60 ms quiet check、协议消息、数据断言和单次 bind 均未放宽。没有 bind retry，没有新增产品握手，也没有更换 socket 配置或依赖 now_or_never 的立即 Ready 契约。

## 调用闭包与循环依赖检查

底层 OS 进程创建只有中央一处 `std::process::Command::spawn`。没有 `.output()`、`.status()`、其他 Command 构造入口、fork/vfork/exec 旁路；`tokio::spawn` 在本文件中是任务创建，不能混作 OS 进程创建。

15 处 helper 调用均在原有 12 个 async tests 中，只给 helper 调用添加 `.await`。移除这 15 个新增 `.await` 后，全部 scenario 正文与基线逐字节相同；`no_connection`、`collect` 和原 bind 失败诊断也逐字节相同。

| 调用组 | 当前顺序 | 对 writer 公平排队的影响 |
| --- | --- | --- |
| 6 处 help/config/preflight 调用点 | 每个 child 都立即 collect；循环下一次 spawn 在前一次 collect 后 | 不会持有旧 child 的读许可再等待新的读许可 |
| 9 处真实 peer 调用点 | 主任务 collect 在 peer.await 前；peer 在 release 前 closed(ws) 并 drop(ws) | 当前 child 的退出不等待 listener 写许可 |
| 3 个 oneshot 场景 | final_echo、accepted_partial、path_swap 都在 collect 前发送继续/失败信号 | 没有持读许可等待写许可再反向解锁 child 的环 |

没有跨 case channel。Root 与独立 reviewer 均检查了这些路径。未来新增“持有一个 child 再创建第二个 child”或“child 退出依赖 listener release”的路径时，必须重新检查公平 writer 排队下的循环依赖；当前候选不为这种尚不存在的调用形态提供一般性保证。

## 已完成的受控直接 inode 实验

脚本及所有原字节在相邻目录：

- `grok-spawn-gate-candidate-r1/controlled_inheritance.py`
- `grok-spawn-gate-candidate-r1/controlled-run-r1/execution.json`
- `grok-spawn-gate-candidate-r1/controlled-run-r1/stdout.log`
- `grok-spawn-gate-candidate-r1/controlled-run-r1/stderr.log`
- `grok-spawn-gate-candidate-r1/controlled-run-r1/experiment/experiment.json`

仅执行了一次本地 standalone Python fork/exec 实验。原 socket 是 `127.0.0.1:46473`、fd 3、inode 601085、FD_CLOEXEC。child PID 7 在 exec 前用 fstat 直接报告同 inode；父进程关闭自己的 listener 后，再经 `/proc/7/fd/3` 直接确认同一 socket 仍在该 owned child 中。该阶段的一次 bind 返回 errno 98。

随后同一 child 执行新解释器，并由新 user code 报告 fd 3 为 EBADF。此时 `waitpid(WNOHANG)=0`，明确该 child 仍存活；原 endpoint 的一次 bind 成功。最后让 child 正常退出并实际 reap。整个实验 exit 0，stderr 为空，无 kill/signal 请求。两个 bind 分别属于预先设计的“持有”与“exec 后仍活着”两个对照阶段，不是原 Grok 断言的重试。

这直接验证了被控制 child 的继承机制。它没有执行 Grok、Tokio 或 Cargo，不是 candidate 的 Rust runtime 资格，也不是 historical failures 的重现；其中的控制 pipe 只属于独立实验，没有新增产品协议。

## 当前验证与待完成项

本地静态审计确认基线身份、唯一进程创建入口、15 个 await 修改、12 个 case 和原断言内容。可用 Rust 1.95.0 的 rustfmt 仅用于候选的解析/格式检查；其 receipt 明确不是项目固定 Rust 1.99.0 的编译、Clippy 或 runtime 资格。候选代码仍待 Root 复制、发布，再由独占的精确工具链 gate 执行原 12-case harness 和预先限定的重复窗口。当前没有把任何候选执行结果写成通过。
