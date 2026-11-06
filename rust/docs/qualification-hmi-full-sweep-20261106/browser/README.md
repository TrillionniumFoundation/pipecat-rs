# 浏览器实际运行资格，2026-11-06

## 已完成的范围

Chrome **156.0.8078.79** 在配置的 `desktop` 上保留正常 Chromium sandbox，执行了真实 Rust/WASM 页面。最终 durable job 是 `1a1b0db389b-fc05-7360`；[权威状态](final/runtime-status.json) 为成功、exit 0、输出完整。这里只结算两个独立 fixture：

| 执行 | 实际结果 | 时间与原始证据 |
| --- | --- | --- |
| `input-r3` | 4 个检查组通过；failure 为 null，page_errors 与 cleanup_errors 均为空 | fixture 4.2872 秒；外层 4.8000 秒；[完整 JSON](final/input-r3.json) |
| `protocol-r4` | 23 个检查组通过；failure 为 null，page_errors 与 cleanup_errors 均为空 | fixture 204.5476 秒；外层 205.0369 秒；[完整 JSON](final/protocol-r4.json) |

“检查组”是报告的组数，不是新增 27 个互不重叠产品能力，也不能与 native Rust 单元用例相加。实际 argv 和 exit code 见 [运行收据](final/results.json)。最终协议 fixture 的原有 114 个 Python assert 节点保持原意；新增诊断没有延长原来五处 50ms 观察窗口。历史结果没有被删除。

## 实际验证了什么

协议轮加载实际 WASM 和静态页面，使用 loopback HTTP/WebSocket、二进制 protobuf 和真实配对的 RTCPeerConnection。它观察 SDP offer/answer、SCTP data channel、RTVI 文本与图像发送、synthetic audio 下行、keepalive、mute、取消、迟到结果和关闭资源。两种 transport 都覆盖 transcript、toast、job-group DOM 的字面显示、过期与不完整提示；显式 PNG/JPEG send-file 保留原始字节，选择文件本身不触发上传。文字包含中文、emoji、换行、UTF-8 与编码预算；本地排队成功不伪造对话确认。

Text-input 模式使用会抛错的 getUserMedia guard，实际没有触发麦克风请求或创建 capture graph。WebRTC text mode 观察 recvonly transceiver、无本地音轨、正向下行 RTP 和零音频上行 RTP counters；ICE、DTLS、SCTP 和 RTCP 仍然可发送。取消 pending start、permission 和 SDP offer 后，迟到资源得到释放。重连使用新会话，没有重放之前的草稿。

延迟 FileReader 的辅助实现经过修正后，最终报告保留 **7 次真实读取**：观察到 load、DONE 和非空 ArrayBuffer，每次都是原始 1,028-byte JPEG，SHA-256 为 `af231fc8371206c675c33b379707b81de70f8ae072d440f7977995d903b4ae11`。等待有期限，异常被报告；没有用空结果或仅排入回调代替真实完成。远端 fixture 页也监听 pageerror，关闭时 await AudioContext close，并核对实际 closed 状态。

单独的输入轮**没有 autoplay override**。它通过可信 Connect click 观察 user activation、native AudioContext running，以及 16kHz、16,000 samples 的 synthetic PCM 从 start 到 ended。浏览器带 `--mute-audio`，所以这证明调度与生命周期，不能证明扬声器发声或用户听到了声音。

输入轮调用 Chromium 的 CDP IME：从“中”到“中文”，最终文本精确为 `中文 🦀\n第二行`；只有显式 Send 后才产生一次完全相同的 protobuf 文本。随后真实中断 loopback TCP：草稿和资源清空，在固定 300ms 观察内没有自动重试，用户点击 Connect 后建立新连接且不重放旧消息，新的显式 Send 成功。它没有实施 WAN 断网、TURN 故障、已有 peer 的 ICE restart 或 provider 自动恢复。

## 源码与实际 bundle 的关系

[原始打包收据](history/bundle.json) 把实际 bundle 绑定到 `e526aabaf98522b2294e744ac48d75d9ed89a254`，wasm-bindgen 为 **0.2.129**。打包后的 WASM 是 776,407 bytes，SHA-256 为 `6dc704bd6d05f09b53fbe4136f6a7ea119bd7bcbf8cd4b34e28818980d3a622d`。这与最终两份浏览器报告的资产哈希相同。

发布的 fixture 是 `478ab3b8e0deca51571c41c2890b519f0faeab94` / tree `52644150c51bd2c66052ed4d32f2ad7a844ac71f`；Rust browser 源码及锁定依赖未改变。其后 `7105367d7d607653c3ab758f0131c029c4373faf` / tree `19a5c08fc2cbfeabacd344668fc7b25d03a282c2` 与 478 的整个 `rust/client-templates`、vendored JPEG 目录逐路径 Git diff 为零。后续本目录及 browser README/VALIDATION 的文档变化须与实际编译输入分开核对，不能把新文档 commit 写成运行时重新编译过的代码。

最终 fixture SHA：

- protocol：`67e5a18fc8583504d957c991e85235e01be3be746e4e0dd4260d5b7a23bd1b3e`。
- input：`13ee11fb9d30e1fc39f783e102d155b0ff5dd59b9292465376ff4ba373388873`。

[原 source binding](final/source-binding.json) 保留完整归属。输入报告中的 `application_source_present=false` 表示远端仅部署 bundle 和 helper，没有 Rust src；这项原始值没有改成 true。源码绑定来自独立 Git/资产记录。原来本地 54 native tests、native/WASM strict Clippy 和 release build 的完成观察，与入口重置后取不回的旧 raw log 分开记账；当前新 native/WASM 补资格另有运行身份，结果以主报告为准。

## 历史失败与 fixture 修正

| 原轮次 | 原结果 | 修正与保留 |
| --- | --- | --- |
| Python 依赖准备 | 默认可见索引缺少所需 aiohttp；一次官方索引请求没有所需 Playwright | 使用原固定版本的官方 wheel，逐个 metadata/SHA 核对，再 require-hashes 安装；[wheel manifest](history/wheel-manifest.json)、安装失败及成功日志均保留，没有降级 |
| protocol-r1 | 3 组完成后，注入 send 的 evaluate 返回函数，被 Playwright 当作可调用结果执行 | 辅助安装改为显式无返回值的代码块；[原 JSON](history/protocol-r1.json)、[原 log](history/protocol-r1.log)、[原 helper](history/helpers/protocol-r1.py) |
| protocol-r2 | 5 组完成后，FileReader 安装遇到同类返回函数求值，Illegal invocation | 安装语句同样显式无返回；[原 JSON](history/protocol-r2.json)、[原 log](history/protocol-r2.log)、[原 helper](history/helpers/protocol-r2.py) |
| protocol-r3 | 23 组通过 | 随后加强真实 FileReader 完成与 peer cleanup 观察，最终重新跑 r4；[原 JSON](history/protocol-r3.json)、[原 helper](history/helpers/protocol-r3.py) 不与 r4 加成独立覆盖 |
| input-r1 | 0 组完成；Browser.getBrowserCommandLine 要求 enable-automation | 加入该诊断所需参数；[原 JSON](history/input-r1.json)、[原 helper](history/helpers/input-r1.py) |
| input-r2 | 音频组完成后，要求所有 composition 事件 isTrusted 的断言失败 | 独立空 textarea 控制复现同一 Chrome 只有 compositionend 为 false；保留 start/update 信任位、实际字面内容和精确 Send 的要求，记录 end 位；[原 JSON](history/input-r2.json)、[独立原控制](history/ime-cdp-native-control.json) |

独立 IME 控制 job 是 `1a1b0d24892-fc05-7332`，没有加载应用 WASM，也没有调用应用 dispatchEvent。核对的 [Chromium 原始实现](https://github.com/chromium/chromium/blob/86244e71dab2acdc71850441d4aee99dafe06a8d/third_party/blink/renderer/core/editing/ime/input_method_controller.cc) 区分 scoped event queue 与直接事件分派；该源码提交**没有被认定为安装的 Chrome 156 的精确 revision**。修正依据是该浏览器的独立 native 控制，加上源码契约核对；它不建立 OS 拼音候选窗口或真人输入的资格。

原来云执行入口的 AF_UNIX/socket EPERM 仍是历史失败。此轮在已配置、正常允许 Chrome 启动的 desktop 执行，没有禁用 sandbox、改变系统安全配置或把旧失败重写成通过。

## 证据可用性与边界

`final/retrieval-manifest.json`、`history/retrieval-manifest.json` 及后续回收索引记录原远端 path、字节长度、SHA-256 和完整读取。原 raw JSON/log 从远端或先前完整工具回包恢复，落盘后重新验证原哈希。[截图](final/browser-acceptance.png) 是单次 **960×480** viewport 的实际产物，不是全尺寸、多浏览器视觉矩阵。

本轮没有证明物理麦克风权限弹窗、OS 撤权/拔插、真实扬声器声学输出、真人中文识别质量、回声处理、OS IME 候选 UI、多浏览器、WAN/TURN、LiveKit browser adapter、公共云 provider、生产 Hepta owner/grants 或签名 media/thread 组合。协议轮有 synthetic microphone 和 autoplay override；输入轮正常 autoplay 的成功仅限当前 automation/headless Chrome 下可信点击后启动。两轮均不把模拟设备或静音播放算作用户验收。
