# HMI 全量推进证据索引（2026-11-06）

本轮继续原 Draft PR #2，保留原 Hepta 会话、权限和模型／工具 owner。源码检查点和运行结果逐轮记账；后来的通过不清除旧失败，也不把不同 feature、工具链、helper 或重复调用相加成独立覆盖。

- [浏览器实际运行](browser/README.md)：原始最终 JSON、历史 fixture 失败、原 helpers、独立 IME 控制和单 viewport 截图。
- [浏览器 Rust／WASM 新补资格](browser/requalification/README.md)：54 项原生测试与两种 target 的 strict Clippy；首次官方组件下载截断与单独恢复都保留，完整 raw 与运行身份已核对。
- [生产组合边界](PRODUCTION_BOUNDARY.md)：原 ALC897 环境已恢复定位；真实 owner/thread、签名 producer/replay witness、当前 generations/grants、采用的模型和实际设备仍需同次组合。
- [环境恢复](environment/README.md)：原配置主机、历史源 checkout、7 个文件与 3 份日志的摘要核对，以及查询范围限制。
- [追加 owner 入口探针](environment/owner-entry-followup-r1.json)：12 份选定工程文件的完整读取与摘要；没有得到真实 thread／unit／socket 组合，不外推为整机不存在。
- [WebRTC 当前证据可用性](webrtc/RECOVERY.md)：旧 60 文件中确切恢复 4 份、56 份未恢复；原 invocation-25 失败仍有完整原文。以该入口解释 preserved README/index，不能将其中旧相对链接当成现已交付。
- [Provider 示例产物身份修复](provider-examples/README.md)：四个同名 Cargo example 的覆盖、普通 executable 与 test harness 的区别，以及最终 gate 的显式 build 要求。
- [早期 native 完整原始记录](native/README.md)：38 份原文件的 byte-verified archive、实际 completed-suite 计数和8条原始合成中文输出。
- [Grok 重绑独立控制](native/grok-listener/control-delivery.json)：本地受控端口竞争，只证明错误的非唯一归因；不清除 desktop r3 的未归因失败。
- [证据入口变化与可用性](EVIDENCE_AVAILABILITY.md)：哪些是原字节回收、哪些是新观察，哪些旧 raw 当前取不到。

完整 workspace、精确 Rust 1.99.0、真实本地 SFU、合成中文模型、Piper／Whisper 生命周期和 WebRTC 固定次数连跑，由 [本轮主报告](../HMI_FULL_SWEEP_20261106.md) 逐项结算。此索引不为仍在运行或尚未执行的阶段预填通过。
