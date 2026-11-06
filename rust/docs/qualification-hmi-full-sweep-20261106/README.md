# HMI 全量推进证据索引（2026-11-06）

本轮继续原 Draft PR #2，保留原 Hepta 会话、权限和模型／工具 owner。源码检查点和运行结果逐轮记账；后来的通过不清除旧失败，也不把不同 feature、工具链、helper 或重复调用相加成独立覆盖。

- [浏览器实际运行](browser/README.md)：原始最终 JSON、历史 fixture 失败、原 helpers、独立 IME 控制和单 viewport 截图。
- [浏览器 Rust／WASM 新补资格](browser/requalification/README.md)：54 项原生测试与两种 target 的 strict Clippy；首次官方组件下载截断与单独恢复都保留，完整 raw 与运行身份已核对。
- [生产组合边界](PRODUCTION_BOUNDARY.md)：原 ALC897 环境已恢复定位；真实 owner/thread、签名 producer/replay witness、当前 generations/grants、采用的模型和实际设备仍需同次组合。
- [环境恢复](environment/README.md)：原配置主机、历史源 checkout、7 个文件与 3 份日志的摘要核对，以及查询范围限制。
- [追加 owner 入口探针](environment/owner-entry-followup-r1.json)：12 份选定工程文件的完整读取与摘要；没有得到真实 thread／unit／socket 组合，不外推为整机不存在。
- [WebRTC 当前证据可用性](webrtc/RECOVERY.md)：旧 60 文件中确切恢复 4 份、56 份未恢复；原 invocation-25 失败仍有完整原文。以该入口解释 preserved README/index，不能将其中旧相对链接当成现已交付。
- [Provider 示例产物身份修复](provider-examples/README.md)：四个同名 Cargo example 的覆盖、普通 executable 与 test harness 的区别，以及最终 gate 的显式 build 要求。
- [Native 完整原始记录](native/README.md)：早期38原件、source／pin r2另包、83原件、首轮扩展112原件与最终矩阵147原件分别保存；完整命令、失败诊断、工具链与source maps保留各自身份。
- [Grok 重绑独立控制](native/grok-listener/control-delivery.json)：本地受控端口竞争，只证明错误的非唯一归因；不清除 desktop r3 的未归因失败。
- [Grok 原系统调用记录](native/grok-listener/trace-r1/README.md)：原12-case首轮11/1、父close成功后bind失败与另一child pre-exec窗口重叠；完整310,585-byte trace及其观测限制。
- [Grok 生命周期隔离修补](native/grok-listener/lifetime-gate/README.md)：真实child回收前保留共享许可，listener原单次bind位于独占窗口；独立继承FD控制不冒充历史失败的当轮证据。
- [冻结源码的完整复验](native/final-matrix-pin-r1.root-verification.json)：147原件、20份source map、四条命令逐case／summary独立计数，新增MoQ与cache Lookup失败保留。
- [最终模型／SFU批次](native/model-optins-pin-r2.root-verification.json)：131原件、40 source maps、19命令／21项case执行；8条中文属于原1项case，CER诊断与执行门分开。
- [单条AISHELL真人录音](native/aishell-human-pin-r1/root-verification.json)：一次离线example，实际3／12字编辑差异、4.281秒输入与30,000ms segment末端的原metadata差异均保留。
- [另一官方ORT C API binary对照](native/smart-turn/wheel-reference-pin-r3/root-verification.json)：三项输出与原C1 ALL逐bit相同，原概率门仍全部失败，未重建历史Python oracle。
- [最后366份runtime原件](native/final-runtime-pin-r1.root-verification.json)：WebRTC100、MoQ20、AISHELL、C3、Q prepare与19套模型wrapper补件，10份source maps。
- [最后35份strict-Q原件](native/strict-clippy-final-q-r1.root-verification.json)：一次原strict命令及两文件格式全部0，2份Q source maps，保留旧strict／fmt失败。
- [最后严格命令的执行准备](native/strict-after-moq-preparation-r1/root-verification.json)：9份原件、原argv／环境与Q契约的独立核对；准备文件不冒充实际执行结果。
- [最后两份诊断／lint源码契约](native/final-diagnostic-source-freeze-r1/source-contract.json)：c1c3的完整2,422路径canonical身份；0cc16完整矩阵不冒称在此HEAD执行。
- [首轮完整扩展矩阵](native/expanded-and-reference-pin-r1.root-verification.json)：112份原件、14份完整source map，原219个harness与完整补充731个harness的独立计数；原24项失败及strict/fmt失败全部保留。
- [Bot-audio进程级观测隔离](native/bot-audio-response/original-raw-review-r1.json)：原15个失败的逐case worker身份／HTTP状态与同进程fixture互扰；原严格等式和期限不变。
- [Cartesia精确签名历史](native/cartesia-eager/README.md)：原request的signed blocks正确，旧helper拒绝既有wire user点号；完整消息数组和roles按实际provider精确核对。
- [静默HTTP阶段选择](native/user-speech-http/audit.json)：原Private-cache lookup timeout仍是失败；该HTTP case显式禁用cache后保留原180ms与实际request／close断言，不能称作cache回收资格。
- [SmartTurn 原失败切片](native/smart-turn/default-no-fail-fast-slices-r1/smart-turn-log-slices.json)：完整no-fail-fast log的精确偏移和原字节摘要；直接speech概率差异、analyzer与pipeline失败分开。
- [SmartTurn 静态路径核对](native/smart-turn/static-review-r1.json)与[QDQ数学诊断](native/smart-turn/qdq-static-analysis-r1.json)：不改原模型、参考值或容差，不将静态近边界值称作实际运行bins或CPU原因。
- [SmartTurn固定输入C API对照](native/smart-turn/c-api-root-verification.json)：同一模型／runtime／原golden features的ALL、DISABLE_ALL、BASIC；原始stdout逐项核对，后两级仍未恢复speech／tone oracle，不更改产品设置或参考值。
- [四项源码冻结与精确路径](fixture-source-freeze.json)：0cc16e8/tree aec037b，只有四份测试源码相对522改变；后续docs提交与实际native测试HEAD分开。
- [证据入口变化与可用性](EVIDENCE_AVAILABILITY.md)：哪些是原字节回收、哪些是新观察，哪些旧 raw 当前取不到。

完整 workspace、精确 Rust 1.99.0、真实本地 SFU、合成中文模型、Piper／Whisper 生命周期和 WebRTC 固定次数连跑，由 [本轮主报告](../HMI_FULL_SWEEP_20261106.md) 逐项结算。本轮授权的工程资格作业已全部终态；未执行的生产／设备能力继续列为未验收。
