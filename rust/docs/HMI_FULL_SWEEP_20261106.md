# HMI 全量推进与资格结算（2026-11-06）

> 本轮工程与独立资格检查均已终态，继续原Draft PR。**完整workspace仍失败，生产Hepta媒体闭环尚未建立。** 冻结0cc16的完整expanded补充为8856pass／7fail／63ignore；模型／SFU19条命令21项执行、Grok20与WebRTC100窗口完成。最后c1c3源码的MoQ原81项suite完成20／20轮，原严格Clippy命令单次exit0，两文件格式与完整性检查均通过。AISHELL单条真人录音有3／12字编辑差异，另一官方ORT C API库仍未恢复SmartTurn原概率门；历史失败与本轮失败分别保留。

本轮继续原 Draft PR #2 的 owner、payload admission、Linux native supervision 与 DTLS shutdown 工作。所有变更保留原 Hepta 会话、权限、模型与工具执行权威，不引入第二套 thread、会话数据库或工具 owner。原根目录 README 保持空文件。以下把实际产品修补、测试辅助实现修补、运行结果、未覆盖能力和证据可用性分别说明。[前轮实现与失败](HMI_FOLLOWTHROUGH_20261106.md) · [上游全类别审计](AUDIT_UPSTREAM_20261106.md) · [owner 契约](OWNER_CONTRACT_20261106.md)

## 1. 源码与运行身份

本轮只使用配置中的主机入口。原 native 主机目录与 browser 部署有自己的 durable job；本地入口中断不会使远端 job 失去身份。两套 native 工具链和不同阶段不混算：

| 阶段 | Published commit / tree | 作用与边界 |
| --- | --- | --- |
| 前轮基线 | `e526aabaf98522b2294e744ac48d75d9ed89a254` | 保留原 repeat-25 failure 与前轮资格 |
| 首批本轮源码 | `478ab3b8e0deca51571c41c2890b519f0faeab94` / `52644150c51bd2c66052ed4d32f2ad7a844ac71f` | WebRTC 分层诊断、真实 SFU scenario、浏览器 fixture 与环境证据 |
| InvalidPayload consumers | `ae56473b029b81a8cb08ec2fd385aecd74287e9f` / `62b1f03d9004bb14523ea973227b10d04240d51c` | 4 个枚举消费点；Rust 1.99.1 的 default、SFU 与首次模型诊断绑定此源码 |
| 格式与未使用绑定 | `dc7fdbd6b6c70bdffa5aadd52671eaf0f8b3334a`；`c45da3bd794af3640c28f8160285dcad31960c40` / `12a56abe43c39621e2d1e2639c828c4d85145f58` | 7 个 owned 文件格式；2 文件中 3 个未使用绑定修正；精确 pin 预编译发生在 c45 |
| 示例产物身份与 SFU 清理 | `7105367d7d607653c3ab758f0131c029c4373faf` / `19a5c08fc2cbfeabacd344668fc7b25d03a282c2` | 13 文件 provider example 命名／调用，3 文件 SFU bounded reap |
| Signed assistant fixture | `8a7c0652c4be02321d1fac6346b19b9ce7931476` / `e8bbba1a5b1af45fdec8be9bcd74df2e63153f6b` | 只修独立测试 helper 的 role 观测，原 scenario 内容与期限不变 |
| 原始证据 checkpoint | `8279c5566c861fc047b7e36f4a44524e34c00767` / `b78833c213d17f7d5bb6455f9cefdc0d8567d459` | 59 个 docs/evidence 文件；与 8a7 编译源码相同 |
| Scalar tool-result fixture | `72cac7fdfc65585219ebf757992ffa5b12c63dcb` / `20fed35e5a3822a0f14d892b904e108776942649` | 3文件4行旧wire期待修正；该中间checkpoint没有执行完整runtime gate |
| Redacted-history trailing user | `ae134e942411c6b0ad64d7f5b6caa1c603919c57` / `b5115bcf53c8992d59117ffd8396f6eea57b9c9d` | 两处静态旧长度期待，增加唯一wireuser点号的精确断言；完整default r3候选 |
| 早期 raw 与 owner probe | `0d4da9a89473fdbc87ad528f3fe081e53226a3b5` / `9fc41d8346eed823ce75da7b1179a7d231bd9df7` | 15 个 docs/evidence 文件；与 ae134 编译源码相同 |
| Grok listener 失败诊断 | `272671b95297313d24db921eceda8e95f3a8d545` / `94424ffa2a8fbcda0f81321ee68e7461663e191c` | 只增加 bounded 观测；原 12 个 app_cli scenario、单次重绑 oracle 与期限不变 |
| Browser／native 原始证据 checkpoint | `52243a392065d474ea0d0aead585891c6010bb3d` / `276642de5386879c1f61030f77b7afd7384f5371` | 95 个 docs/evidence 文件；编译输入不变，native 后续运行继续绑定 272671，不伪称运行了 docs-only 新 HEAD |
| 四项 fixture 修补冻结 | `0cc16e8123cb05aaad480d5ab7860a267d998bac` / `aec037b9cf2be53fcf6f85fdc149382942277702` | Grok child 生命周期、bot-audio 进程级观测隔离、Cartesia 精确 wire history、静默 HTTP 用例阶段选择；包含 Grok 诊断的等价 Clippy 写法修正。新的原始 gate 绑定此源码，SmartTurn 产品配置与原参考值未改 |
| 完整原件分发checkpoint | `5d3f6e1a25d64655c2e6d66e7b8c649a1cc98d7f` / `4833e2db5009c57c42f16db6b88a4fa86b715c97` | 100个docs/evidence文件；native后续仍实际运行0cc16。完整canonical map包括文档，编译输入不变不等于整个checkout manifest相同 |
| 最后诊断与lint源码 | `c1c3addb07dde0a143462c10961ff2494eeba90a` / `4df0d197540551ed9b74d4e11c0936e5bedee5f5` | 只改MoQ原断言的失败消息与quality ledger边界测试的等价初始化；不重跑完整矩阵，独立执行原81项MoQ suite固定窗口、一次原strict命令和两文件格式检查 |

最终c1c3源码契约的2,422文件canonical摘要为`23a585582dba548ece61948fcb87ec32a0689ff46bef20ace1739edd3986e20d`。它由原0cc16完整map与0cc16→c1c3的全部102个真实Git路径差异推导，包括5d3的100项文档变更，不能只替换两个Rust文件摘要。native已用独立source-preparation job核对完整树与契约后才进入新检查。[发布与源码契约](qualification-hmi-full-sweep-20261106/native/final-diagnostic-source-freeze-r1/publication.json)

早期本地 commit 与发布 commit 的 metadata 不同，但曾按全树逐字节核对相同；它们是同一源码检查点的不同 commit 身份，不是两次产品修补。最终运行以报告中的 published SHA 和运行前后 manifest 为准。[发布桥接](qualification-hmi-full-sweep-20261106/publication-artifact-cleanup.json) · [signed helper 修补](qualification-hmi-full-sweep-20261106/signed-assistant-helper-fix.json)

### 工具链与 target 隔离

原 desktop 现有 Rust **1.99.1** 仅作独立诊断轮，没有被改写成项目 pin。精确 **Rust 1.99.0** 从官方 manifest 对应的 rustc、native std、cargo、clippy、rustfmt 五组件恢复；包摘要通过后安装在私有工具目录，记录实际 executable/version/hash。第一次 rustup 900 秒超时保留，没有通过改 pin 绕过。Clang 21、native LiveKit SDK、LiveKit server 和 ORT 也各有实际输入摘要，锁文件与依赖版本不变。

同一 checkout 使用独占 target：旧 `source/target`、精确 `source-pin/target-pin`、浏览器补资格 `source-browser/target-browser` 分开。前后 manifest 是运行边界的字节检查，不宣称执行期间物理不可修改。预编译 `--no-run` 的成功计为构建，不计 runtime test。主 native job 设置 CPU／内存／外层期限，原测试自身的期限与断言不变。[既有 target 隔离原因](RECOVERY.md)

## 2. 完整 gate 与独立 opt-in

### 冻结源码 0cc16：完整默认与扩展运行

下面各命令使用同一`0cc16e8123cb05aaad480d5ab7860a267d998bac`、Rust 1.99.0、独占target和原测试期限。原命令遇到运行失败后，各自仅增加一次同配置no-fail-fast补充；四条命令均已结束，原失败不被后面的通过覆盖。

| 命令 | Exit | Completed harnesses | Pass／fail／ignored | Gate记录秒数 |
| --- | ---: | ---: | ---: | ---: |
| 原完整default r5 | 101 | 325 | 3792／1／11 | 185.088316 |
| default `--no-fail-fast` r2 | 101 | 755 | 7748／4／33 | 392.459805 |
| 原all-features/all-targets r2 | 101 | 222 | 2221／1／0 | 351.790685 |
| 同配置 `--no-fail-fast` r2 | 101 | 731 | 8856／7／63 | 534.013946 |

时间取自各原`gate.json`的start/end区间，包含命令执行与末端source-after完整性检查；没有独立记录的纯Cargo child时长不补造。控制RPC等待、外层wrapper时间、libtest单target时间与模型推理时间分别保留。

原default停在MoQ absent-peer断言；独立default补充的四项失败属于三个SmartTurn targets，该轮MoQ通过。原expanded停在bot_audio_smart_turn概率断言；完整expanded补充的七项失败属于五个targets：evals lib中的两项cache Lookup期限、bot_audio_smart_turn的一项概率、turns pipeline的两项概率、VAD lib及model analyzer各一项概率。没有缺失或多重terminal summary，Root逐harness把每一条case的ok／FAILED／ignored状态数与summary核对，再独立复算总数。

四项fixture修补在同一次完整expanded补充中的实际结果如下；它们已经包含在8856／7／63内，不再相加：

| 原target | 实际pass／fail | 对应修补 |
| --- | ---: | --- |
| bot_audio_response | 16／0 | 进程级worker集合观测的fixture隔离 |
| user_speech | 16／0 | silent-HTTP用例明确进入原HTTP阶段 |
| cartesia_turns_eager | 34／0 | 原adapter完整signed wire history的精确匹配 |
| Grok app_cli | 12／0 | child实际回收前保留共享许可；原单次rebind不变 |

另一个独立Grok窗口完成20轮、每轮原12项全通过，20份原raw的摘要、case names与唯一summary均逐一核对；这是重复运行，不是240项新覆盖。

| 构建／静态检查，仍为0cc16 | Exit | Gate记录秒数 | 实际边界 |
| --- | ---: | ---: | --- |
| all-features普通examples build | 0 | 8.036619 | 四个唯一普通executable，profile.test=false；本轮compiler-artifact均fresh=true，摘要保持原已构建产物，没有记为重新编译或runtime tests |
| 原strict Clippy | 101 | 117.444348 | 唯一实际error为quality ledger测试的field_reassign_with_default；c1c3等价修补与复验另列 |
| 原fmt-all | 1 | 16.792953 | vendor格式与缺失module仍失败 |
| 62个owned members格式 | 0 | 15.090616 | 独立范围，不替换fmt-all结果 |

Root已核对新归档的147份原件、23,765,300 bytes与20份完整source map；其中C2的两份map保留原272671身份，其余18份绑定0cc16。187个archive members还包含原件index及39个派生统计／切片，派生文件不冒充新的执行。四条test命令、20轮Grok raw和normal example compiler-artifacts均从原bytes重新核实。[完整归档与Root核验](qualification-hmi-full-sweep-20261106/native/final-matrix-pin-r1.root-verification.json)

### 最后c1c3源码的独立检查

新source仅包含两项已审测试修改，完整2422路径契约与target owner在source-preparation完成后逐项核对。没有把0cc16完整矩阵改记为c1c3执行，也没有因只改两文件而重复整个运行矩阵。

| 原检查与新执行 | 次数／范围 | 实际退出 |
| --- | --- | --- |
| MoQ原whole-suite | 原81项、默认并行、固定20轮，首败停止策略；实际20／20成功 | command／containment／integrity均0 |
| 原严格Clippy | `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`，只启动一次 | 原command0，wrapper0 |
| 两个改动文件的pinned rustfmt | MoQ与quality.rs各一次独立检查 | 两项均0 |

Strict job `1a1b21a6b59-fc05-7c5f`权威终态succeeded0／complete／durable，无timeout；原506-byte stdout确认只启动一次Clippy、三条资格命令各一次、源码与被观测文件不变、containment与integrity均0。原fmt-all失败与0cc16的strict失败仍保留。[实际严格检查状态与原输出](qualification-hmi-full-sweep-20261106/native/strict-clippy-after-moq-pin-r1/root-runtime-read.json) · [已审执行准备与原命令绑定](qualification-hmi-full-sweep-20261106/native/strict-after-moq-preparation-r1/root-verification.json)

### 较早源码的原失败矩阵

| 原命令 | Source／Rust | Exit | Completed harnesses | Pass／fail／ignored | Gate记录秒数 |
| --- | --- | ---: | ---: | ---: | ---: |
| 完整default r3 | ae134e9／1.99.0 | 101 | 266 | 2871／1／0 | 154.894659 |
| 完整default r4 | 272671b／1.99.0 | 101 | 656 | 7329／2／17 | 329.030442 |
| default `--no-fail-fast` r1 | 272671b／1.99.0 | 101 | 755 | 7748／4／33 | 324.642713 |
| all-features普通examples构建 | 272671b／1.99.0 | 0 | 不适用 | 0 runtime tests | 193.519372 |
| 原all-features/all-targets r1 | 272671b／1.99.0 | 101 | 219 | 2175／15／0 | 925.479220 |
| 同配置 `--no-fail-fast` r1 | 272671b／1.99.0 | 101 | 731 | 8839／24／63 | 482.644572 |

早前default补充完成所有目标及doctests，四项失败属于三个SmartTurn targets；r3／r4在失败target后停止。首轮expanded原命令停在bot_audio_response的1pass／15fail，补充收齐8个失败targets。原strict Clippy101、fmt-all1、owned-format0及全部早前失败分别保留。[此前83份原件](qualification-hmi-full-sweep-20261106/native/README.md) · [此前112份原件与独立计数](qualification-hmi-full-sweep-20261106/native/expanded-and-reference-pin-r1.root-verification.json)


### 计数原则

同一用例在 default、all-features、重复压力轮和另一工具链上的执行不相加成新的独立覆盖。一个测试内部的 192 对连接、多个 PCM frame、两种视频图案或 8 个 transcript 也不转成额外测试用例。Cargo completed-suite 总数是该命令的测试执行次数；某些 target 通过 `#[path]` 引入共享 regression graph，同一 source function 可以在不同 harness 中执行，不能把总数称为去重后的源码用例数。Cargo test 因某个 target 失败而停止时，之前 completed suite 的通过只是该次部分执行，不能叫完整 workspace 通过。静态 inventory 中 63 个 literal `#[ignore]` 不等于某次 Cargo 的 runtime ignored 总数；helper、feature 选择和 target 选择必须逐项对应。

严格格式门也分开记：原 `cargo fmt --all -- --check` 递归进入 path dependencies，遇到 112 个 vendored 文件的格式差异和 vendored JPEG 的历史缺失测试模块；不能把 owned workspace members 的格式通过重写成该命令通过。7 个 owned 文件已修正；没有整体重写 vendor、补假测试模块或加忽略配置来制造绿色结果。旧格式 raw 的当前可用性见 [可用性记录](qualification-hmi-full-sweep-20261106/EVIDENCE_AVAILABILITY.md)。

## 3. 本轮修补、失败定位与诊断

### 3.1 新 payload 错误需要被消费方完整处理

前轮 core admission 引入 `PipelineError::InvalidPayload` 后，旧完整 build 首先在 Grok 非穷尽 match 报 E0004。修复在 Grok app 和 AWS Nova／Azure Realtime／Azure Voice Live 三个 PCM example 中补全 4 个消费点。错误转换使用静态、受控分类／文字，不回显原 payload，也不改 admission budget、厂商 wire 协议或锁定依赖。

基线 prebuild 与首轮 full default 均在编译阶段失败，执行测试数为零；这是本轮实际捕获的编译失败，不能被后来列出的 runtime 通过掩盖。

### 3.2 四个普通 Cargo example 覆盖同一路径

Rust 1.99.1 的下一次完整 default 已编译通过，但在 Nova 的 85 个 library tests 中出现 2 个失败：帮助信息不符，以及等待结果超时。当时 `target/debug/examples/pcm_session --help` 的实际 stdout 是 **INWORLD_API_KEY / Uses InworldApp**，前后 executable SHA 相同。这证明测试调用了其它 package 的产物，而非仅凭超时猜测云协议问题。

四个 package 现在各有显式、唯一 example 名称，并关闭自动同名发现。README、帮助文字和 Nova 调用方同步修改，原源码路径保留：

| Package | 唯一 example |
| --- | --- |
| AWS Nova Sonic | `aws_nova_sonic_pcm_session` |
| Azure Realtime | `azure_realtime_pcm_session` |
| Azure Voice Live | `azure_voice_live_pcm_session` |
| Inworld Realtime | `inworld_realtime_pcm_session` |

另一个真实 Cargo 探针表明，`cargo test --all-features --all-targets` 的 example test harness 不保证刷新普通 executable。最终扩展 gate 因此先显式 build 全 workspace 的 all-features normal examples，保存 compiler-artifact 的 package、target、features、`profile.test=false`、executable 与 SHA，再运行扩展测试并核对产物。这既修覆盖问题，也防止 default-feature 普通程序被误称为 all-features 产品。[原探针与最终重建](qualification-hmi-full-sweep-20261106/provider-examples/README.md)

### 3.3 Signed assistant 测试误把 user 兼容消息算作语音

精确 pin 首次完整 default 继续运行后，在 `spoken_readiness_independent` 的 11 项中出现 9 通过／2 失败。实际数组在 signed thinking／Heard 后多一个 `.`。

独立审查确认，已有 Anthropic adapter 对未知模型 `synthetic` 的尾部 assistant 会追加独立 `role=user` 的点号兼容消息。旧 helper flatten 所有 roles，把它误算作 assistant signed blocks。修补只改 helper：检查共享 snapshot 全为 assistant、wire 恰好两条、第一条 assistant、尾部恰为 user 点号，并确认共享 snapshot 在 request serialization 前后不变，然后只取 assistant 内容。两条原 signed 数组断言、所有 scenario body 和期限逐字节保留；producer、SpokenHistory reducer 和 adapter 产品逻辑没有改动。精确 1.99.0、edition 2021 的独立 rustfmt check exit 0。[修补收据](qualification-hmi-full-sweep-20261106/signed-assistant-helper-fix.json)

### 3.4 Anthropic 工具续轮 fixture 保留了旧的文本形状

精确 pin 第二轮完整 default 越过上述失败后，在 `pipecat-rtvi --test first_answer_timing` 的 155 项中出现 153 通过／2 失败。辅助 HTTP peer 从 `tool_result.content[0].text` 读取工具结果，但现有 adapter 对 `Content::Text` 明确保留 scalar string；该 fixture 输入正是 JSON `.to_string()`。已有 `anthropic_tool_result_text` 同时验证精确 string、空值／中文／JSON文本，以及 rich/missing/empty Parts 继续使用数组，产品契约已有独立对照。

修补只把该 selector 改成 `content.as_str()`，保留原 role、工具 ID／名称／参数、parsed JSON 与原 JSON 字节、3 messages、metrics／effect gating 和所有期限。全 workspace 同类检索还确认两个 turns fixture 的 echo 工具结果经 `.to_string()`／MessageContent::Text 进入相同分支；它们只调整原 expected tool-result 的 string 形状，签名／redacted block 顺序、工具结果字节和实际执行次数不变。这两处是已证实同一契约的静态修正，不被描述为此前完整 gate 已实际执行并失败。没有接受两种 wire 形状来掩盖差异，也没有修改 adapter、工具执行或 rich-parts 语义。

随后在 redacted runtime 同一未知模型 `fixture` 的完成／中断／取消路径中确认两个旧的 `messages.len()==2` 期待。原 context 是 user 加 assistant，现有适配器会追加唯一 wire-only user 点号；两处改为3并额外精确核对第三条消息。原 assistant/opaque/heard-prefix、共享 context、effects、cleanup 和期限不变，未闭合分支的单条user期待不变。这覆盖三个runtime case，属于静态契约修正，没有伪称旧baseline已经执行失败。[Scalar修补](qualification-hmi-full-sweep-20261106/anthropic-text-fixture-fix.json) · [Trailing-user修补](qualification-hmi-full-sweep-20261106/anthropic-trailing-fixture-fix.json)

### 3.5 WebRTC 增加可归因诊断，保持原失败开放

本轮没有因后续重复通过就宣称 SCTP race、lost wakeup 或资源压力已确定。诊断公开 wrapper／native channel、peer、ICE、DTLS、SCTP、`has_started`、published／cancelled 状态及首次 milestone 时间；channel 只用 Weak 引用，不延长拥有期。状态采样不是原子快照，native None 也不等于已经 join／reap。

原 stress 的32批次×6种schedule都保留；原本等待setup的schedule仍先A后B、各5秒，schedule5仍在构造／setup过程中进入关闭，UDP释放仍为2秒。新增有界状态转移采样记录 schedule／iteration／双方、monotonic elapsed、采样间隔与丢弃数量；失败立即保留，不在失败后重试并用后来的通过替换。受控 native UDP case 能观察 native channel Open 早于 wrapper publication 的阶段差异，但它不是对历史 timeout 的复现。[诊断契约](../crates/pipecat-webrtc/SHUTDOWN_REGRESSION.md)

最终0cc16／Rust1.99.0的固定100次新窗口全部通过，只选原`repeated_peer_shutdown_schedules_release_both_udp_ports`一个case，每轮实际selection、原log、退出及收尾分别保存。实际compiler-artifact绑定`review_lifecycle`，features为default／opus，profile.test=true、fresh=false；37,666,728-byte可执行文件前后SHA256均为`b833123a4df7cc24d1275b54eb35e418e70d78e489a56c1224d5796b4b0bbb86`，并非借用历史inherited binary。

Root从100份完整原log逐一重算每轮1pass／0fail，核对唯一case名及摘要；100个leader均实际reap，最终owned PGID观察均不存在，没有发送强制信号。whole runner177.159389秒含编译绑定、运行和收尾；100轮墙钟窗口139.723651秒，child execution区间合计133.972230秒，口径分别保留。100次重复不变成100项新覆盖，原invocation25与历史schedule5／iteration24 UDP失败均未因此解释或清除。[100轮Root原件核验](qualification-hmi-full-sweep-20261106/native/webrtc-stress-pin-r1/root-verification.json) · [完整终态原件包](qualification-hmi-full-sweep-20261106/native/final-runtime-pin-r1.root-verification.json)

该窗口使用前面已恢复核验的私有Rust1.99.0工具路径，但没有为本窗口另外保存compiler-tool executable的前后SHA；大测试executable本体也未导出，前后SHA来自原运行收据。source／runner／helper／compiler-artifact／harness的绑定和100份原log已闭合，不能将此扩大为本窗口对整个编译工具系统的独立快照。[115份相关原件的独立核验与限制](qualification-hmi-full-sweep-20261106/native/webrtc-stress-pin-r1/independent-review.json)

### 3.6 真实 SFU fixture 与有界 server reap

保留原 audio/data/leave/rejoin scenario，新增实际 remote video 解码和 participant mute/unmute。服务端使用隔离 loopback、固定 LiveKit server 1.13.9，实际 SDK 入房；合成 media 和 dev token 的范围明确，不混作托管服务／账号资格。

独立清理审查发现原 `Child::wait` 位于 scenario deadline 外，没有自己的等待上限。现在 kill 后以 `try_wait` 在 3 秒窗口内核对实际退出，每 10ms 观察一次；只有取得 exit status 才设置 reaped。cleanup JSONL 分开记录 kill、wait error、timeout 与 elapsed。正常 Drop 清理失败仍使测试失败；已经 unwind 时避免二次 panic。即便后续 Drop 的第二次尝试成功，第一次失败仍不被清除。外层 owned job 仍是最终内核／I/O 期限边界；不承诺任意 kernel-stuck 进程硬实时 reap。[SFU 规则与证据](../crates/pipecat-livekit/qualification/LOCAL_SFU.md)

### 3.7 Grok fixture listener 重绑失败仍未归因

精确 pin 第三轮完整 default 在 Grok 的 `app_cli` 中得到 11 passed／1 failed：`actual_binary_server_vad_rapid_burst_preserves_speech_and_joins` 的辅助 listener 在 drop 后立即单次重绑，返回 `AddrInUse`，随后 peer join 传播 panic。实际产品 child 的退出、PCM／report 与 join 断言先于主测试的这一 peer await，已执行通过；失败的监听器由并发测试 peer 创建。这一主任务语句顺序不证明并发 peer 的重绑发生在 child 退出之后。原日志没有地址、inode、持有者或 TCP 状态，不能由此判断是 retained owner、端口重新分配、TIME_WAIT 或资源压力。

新增诊断记录 drop 前的 fd、socket inode 与 descriptor target；原 drop 与单次 bind 之间没有插入操作。失败先输出原 errno，再读取有界的当前进程 fd 与精确／mapped endpoint、同端口 wildcard 的 TCP 行。fd 扫描上限 4,096 entries／64 matches，单张 proc TCP 表上限 1 MiB／8,192 lines／64 matches；截断、read error、race／unreadable 均显式输出，不扫描全机进程。快照不是原子的，失败后找不到原 inode 也不证明 bind 当时无人持有。drop 前少量 metadata 读取本身可能影响调度；后续通过不能倒推原 r3 未失败。

独立的本地 Linux socket 控制只证明一种可能性：原 inode 已释放后，受控竞争 listener 占用同一 endpoint，单次 bind 同样报 errno 98；竞争 listener 关闭后，下一次单次 bind 成功。它没有 TCP payload／连接，也不是 desktop 原 r3 的复现，没有给原失败指定原因。原 `no_connection` 60ms 与单次重绑断言继续保留。[独立控制与范围](qualification-hmi-full-sweep-20261106/native/grok-listener/control-delivery.json)

诊断复查限定为原完整 12-case harness、默认并行、最多 20 次、重复窗口不超过 300 秒且首败停；编译时间与该窗口分账。后续完整 workspace 仍运行原命令，单独诊断的通过不清除 r3，也不转成 240 个新覆盖项。

该诊断 r2 实际执行三轮：前两轮各 12/12，通过后的第三轮 11 passed／1 failed 即停止，没有执行余下 17 轮。重复窗口为 2.412084766 秒，失败改为 `actual_binary_server_vad_does_not_invent_explicit_run_or_playback`。原 listener 是 `127.0.0.1:46553`、fd31、inode1120236524；失败后 self 49 个 fd 未发现原 inode，TCP/tcp6 有界读取均为空、无错误或截断。这些后采样没有给出正向持有者或 TIME_WAIT 证据，也不能还原 bind 瞬间。原源码与两个实际 executable 的前后摘要均相同。

随后在同一 272671 源码上进行独立系统调用诊断。全 workspace build 改变了 feature-unified 产物，因此先用同配置 `--no-run --message-format=json` 重新绑定实际 harness 与普通 `grok_pcm`，没有复用旧二进制身份。现有 strace 6.8 的同用户预检通过；正式运行保持 12 cases、默认并行、原期限与单次 bind，限定最多 10 轮／90 秒、首败停。实际首轮 11 passed／1 failed，2.895 秒；窗口 2.901573 秒即停止。完整 syscall trace 为 310,585 bytes，无丢失／截断，leader 已回收且最终 PGID 不存在，未请求清理信号。strace 会影响调度，这不是未插桩复现的时间测量。

这次失败是第三个不同 case：`actual_binary_failure_after_accepted_pcm_persists_exact_partial_only_after_join`。原 fd28／inode1121549341／`127.0.0.1:34901` 明确带 CLOEXEC，父进程 close 返回0；随后原单次 bind 返回 errno98。另一并行 case 的 child696897 在 clone3 到 execve 之间处理 stdio，窗口覆盖了父 close 到 bind 失败，clone flags 没有 CLONE_FILES。该机制与 pre-exec 期间暂时继承 listener 高度吻合；但 trace 没有直接读取 child 的 fd28／inode，clone syscall 入口也不是内核复制 fd 表的精确时刻，因此仍是有具体 PID／时间线支持的候选解释。没有将它写成已直接证明持有原 inode，也没有倒推旧 r3 或诊断 r2 的原因。[原始 trace 与只读分析](qualification-hmi-full-sweep-20261106/native/grok-listener/trace-r1/README.md)

随后一次独立本地控制直接验证了这个机制：child7在exec前与父进程都观察到原fd3／inode601085；父close后，原地址的单次bind失败。该child进入exec后的新user code，fd3已是EBADF；此时child仍存活，原地址的单次bind成功，最后正常回收。该实验没有执行Grok／Tokio／Cargo，也不补写原失败当轮缺失的child inode证据。

据此准备并应用了测试fixture的生命周期隔离修补。每个中央spawn取得共享读许可，由实际ChildOwner一直持有到collect确认回收或Drop的wait路径结束；wait失败先设置单调失败标志，后续read admission与release qualification均拒绝。原60ms no_connection之后，release取得独占写许可，确认所有潜在fixture继承者都已退出，才执行原listener drop、原一次Tokio bind及检查listener的drop。原12case仍可并行；writer只暂时阻止新的child admission。新增两侧锁等待使用既有WAIT=10s，原产品3000／500ms、协议内容、数据断言、collect正文与60ms检查不变，不加入bind retry。既有Drop wait仍由原外层期限兜底，不声称被新的admission超时硬性限制。

只锁到spawn返回的短方案被否决：明确版本的Rust／glibc／Linux primary源码显示，spawn返回与所有继承CLOEXEC文件关闭完成不能视为通用的同一边界。最终方案依据真实child回收，不依赖该中间状态。Root独立核对15处新增helper await撤销后全部scenario及wire helpers与原字节相同。[修补应用审计](qualification-hmi-full-sweep-20261106/native/grok-listener/lifetime-gate/application.json)

冻结0cc16后，精确Rust1.99.0首先完成四份改动源码的格式检查及原workspace/default配置的`--no-run`绑定。新harness SHA为`89bbf1dad3c5a0b6fa435c22d358550bbc661874d30b4e52b1f8cb2fd5cc6e05`，普通grok_pcm SHA为`e998bc223a8b49a1871d8737c1861c9b3e267e98e41b7def7fb97fb8a4d4895f`，两者前后相同。预先限定的20轮全部执行，各为原12-case默认并行且完整通过；重复窗口25.122384秒，整个runner79.371363秒，后者包括fetch、格式、编译绑定与后验。原source canonical前后同为`1e07f773ca2827e8db5b06175f96137b5d85bb6dcb68229d29601efe29658535`。这是20次重复执行的有限证据，不计为240项新覆盖；原default／diagnostic／trace以及扩展补充的各次重绑失败均保留。[原20轮报告与二进制绑定](qualification-hmi-full-sweep-20261106/native/grok-listener/lifetime-qualification-pin-r1/repetitions.json)

### 3.8 SmartTurn 原概率门失败，继续分离特征与推理

272671／Rust 1.99.0 的完整 default r4 越过 Grok 和 RTVI 后，在 `pipecat-turns --test smart_turn_onnx_pipeline` 的两项原概率断言失败，exit101。该次gate记录区间为 **329.03 秒**（包含末端源码完整性检查）；早前按控制端仍显示 running 推算的约10–13分钟不是实际执行时长，不能计入性能结论。保留原 `1e-5` 阈值和所有接收／结束期限，没有因运行更远而标记完整 default 通过。

独立的原命令 `cargo test --workspace --locked --no-fail-fast` 的gate记录区间为 **324.642713 秒**（包括末端源码完整性检查）、exit101，继续跑完剩余 targets 与 doctests。原 Cargo log 为 893,156 bytes，SHA256 `60df6c85e3dfce912f720424a08467e92aeb9997b5b5aac71f3c05b967f0bf96`；896,180-byte runtime stdout 还包含最终 gate JSON，不是同一文件。最终失败目标只有 `pipecat-turns --test smart_turn_onnx_pipeline`、`pipecat-vad --lib` 与 `pipecat-vad --test smart_turn_model_analyzer`，不能因补充执行完成而改写其 nonzero。

VAD lib 的8项中7通过／1失败。三份 speech／tone／silence 特征比较都在原 `2e-6` 容差内通过；libtest 对通过项的 stdout 默认捕获，因此没有观测其实际最大误差，也不证明字节相同。直接模型 case 在第一份 speech 打印 `native=0.9685282, Python=0.9823202`。对应 f32 的可往返十进制值分别为 `0.9685282111167908`、`0.9823201894760132`，绝对差 `0.013791978359222412`，约为 `1e-5` 的1,379倍。不是略超阈值，也不能靠放宽容差结案。

原直接模型循环在 speech 概率断言处停止，因此 tone／silence 的直接概率及后续 sample-rate checks 尚未执行。Analyzer 的3项中2通过／1失败，失败在初次 tone 概率检查且未打印实际值，后续 speech／reanalysis 未到达；取消／复用及 silence timeout 两项通过。两个 pipeline case 均在概率检查失败，之后的 completion／context／End／join 断言不能计为通过。这排除了“仅 pipeline aggregation 导致差异”的解释，但不足以归因 CPU、特征前端、模型或 ORT。[原日志字节切片与偏移](qualification-hmi-full-sweep-20261106/native/smart-turn/default-no-fail-fast-slices-r1/smart-turn-log-slices.json)

静态核对确认原14个源码／模型／PCM／feature／reference文件与该次native输入相同；speech为80,999 samples，endpoint窗口前补47,001个零。原图是QDQ量化模型，input_features的唯一消费者为QuantizeLinear，scale为0.009614553302526474、uint8 zero point为28。原speech特征中17个值在数学上距离有效量化半步不超过2e-6，因此特征容差通过不足以证明量化后的输入相同；tone没有这个近边界集合。这里没有观测Rust实际feature bytes或运行中的quantized bins，不构成前端／CPU原因判断。[静态路径核对](qualification-hmi-full-sweep-20261106/native/smart-turn/static-review-r1.json) · [QDQ静态诊断及限制](qualification-hmi-full-sweep-20261106/native/smart-turn/qdq-static-analysis-r1.json)

随后在同一272671源树、同一模型和实际ORT共享库上完成两个独立C API对照，直接输入三份原冻结float32特征，不经过Rust特征前端或pipeline。r1保持顺序执行、inter/intra各1、CPU/Arena/CPUInput与全部图优化；native执行及release均exit0，wrapper因三项原概率门失败而exit1。r2只改变图优化枚举及相应日志标签，DISABLE_ALL与BASIC各执行三次Run；ALL直接引用r1，没有重复执行。两级的native均exit0，比较均exit1；全部观测输入与source前后摘要相同。

| 原fixture | 原reference f32 | C API ALL（C1／C3） | C API DISABLE_ALL／BASIC |
| --- | ---: | ---: | ---: |
| speech | 0.9823201894760132 | 0.9685282111167908 | 0.9729918241500854 |
| tone | 0.09577557444572449 | 0.28783851861953735 | 0.06667384505271912 |
| silence | 0.9870367050170898 | 0.9880069494247437 | 0.9870367050170898 |

Root与独立审查分别解析三份原stdout，逐条匹配保存的native_records并重新计算float32 bits及差值。DISABLE_ALL与BASIC三项各自bit-exact相等，但speech／tone仍未达到原1e-5，只有silence精确匹配reference。r1的speech bits与原Rust直接speech输出相同；原Rust循环未执行tone／silence，不能把C结果追记为原Rust观测。结果证明该固定环境下图优化级别会改变输出，也证明更换为这两个级别不足以恢复原oracle；没有分离具体fusion、CPU kernel或原Python环境因素，故不改产品优化设置／golden／容差。

实际加载库为ORT1.21.0、build commit e0b66cad28、SHA256 `3c8181b339025dd18899916357e74290029722e1d5a5778b4071a733f813420d`。原Python oracle元数据只有版本号，没有原库binary／build／CPU身份，版本字符串相同不足以认定运行环境相同。C1 wrapper实际1.988767秒，其中probe进程0.619064秒，包含session加载与释放；C2 runner实际4.128910秒，控制入口及结果回收的等待不计为推理时延。[C1原报告](qualification-hmi-full-sweep-20261106/native/smart-turn/ort-reference-pin-r1/reference.json) · [C2原报告](qualification-hmi-full-sweep-20261106/native/smart-turn/ort-levels-pin-r2/levels.json) · [Root独立对照核验](qualification-hmi-full-sweep-20261106/native/smart-turn/c-api-root-verification.json)

在最终0cc16上又完成一次独立C API库对照（C3，job `1a1b1fe3c1b-fc05-7bea`）：从固定官方ORT1.21.0 wheel提取另一份实际共享库，SHA256 `a0df84fae0f09d9eca3d6919f685ba435f1ae6cb628e128b69bcf2a2b965504c`，与原库`3c8181…`分开存放，未替换产品runtime、安装Python包或导入Python ORT。原C1可执行文件、三份冻结特征、顺序单session／inter与intra各1／ALL设置不变，只进行一个probe进程、三次Run。

三项新输出的f32 bits分别是`speech=3f77f177`、`tone=3e935f92`、`silence=3f7cee06`，与上表C1 ALL逐bit相同，三项仍全都超出原`1e-5`。native执行与release返回0，比较wrapper为1；两者分别记账。24个被观测文件与完整源码前后摘要一致，probe leader已实际回收，所观测PGID前后均不存在，没有发送强制信号。全runner13.457368秒包含10.060849秒archive下载，不当作三次纯推理耗时。

这次不同C API binary并没有恢复原Python oracle，也不能单凭结果归因为CPU或某个kernel。两份库记录的build_info字符串相同，均含官方v1.21.0的`e0b66cad28`与同一组flags；版本／构建字符串和二进制摘要的差异仍不能证明原库的发行来源。官方wheel中的pybind扩展与这里加载的C API仍是不同binary，原Python oracle缺失的binary／CPU身份没有因此补齐。保留产品优化、原golden与概率阈值，并停止没有新增分辨力的版本／flag重试。[C3完整原报告](qualification-hmi-full-sweep-20261106/native/smart-turn/wheel-reference-pin-r3/reference.json) · [Root逐bit与原stdout核验](qualification-hmi-full-sweep-20261106/native/smart-turn/wheel-reference-pin-r3/root-verification.json) · [独立复核](qualification-hmi-full-sweep-20261106/native/smart-turn/wheel-reference-pin-r3/independent-review.json)

### 3.9 扩展矩阵暴露的三个 fixture 契约问题

`bot_audio_response`的原16项中1通过、15失败。14个async失败记录均在进程级`eval-bot-vad`线程身份集合的原严格等式处停止；其中7个case的原基线为空、结束时看到并发case的worker，另7个case捕获了其它case的6–7个worker、后来集合又改变。最后一个case的34次结束观测全部为Some([])，仍因原基线含其它worker而失败，不能称为该case残留worker。另一个同步invalid-bounds case未打印两端集合，不补造其值。原helper先完成session／HTTP／peer join，再观察整个进程；旧注释假定`--test-threads=1`，实际完整gate使用默认并行。

修补在这个integration-test进程内加入一个共享async mutex：14个async入口先取得许可，同步invalid-bounds入口使用blocking_lock，纯constructor case保持原样。所有helper、场景、原严格集合等式、200ms观察与产品期限不变；等待许可发生在场景计时之前。它不是把整个workspace改成串行，也不把identity equality改成subset或空集宽容。原15个失败仍保留，复验按新source另列。[原日志逐case分析](qualification-hmi-full-sweep-20261106/native/bot-audio-response/original-raw-review-r1.json) · [应用核验](qualification-hmi-full-sweep-20261106/native/bot-audio-response/application.json)

`cartesia_turns_eager`原34项中33通过、1失败，实际next request的user及live-B thinking／signature／text都精确匹配，唯一旧条件是消息总数必须为2。现有Anthropic adapter对`loopback`模型按原规则追加唯一user点号。修补只替换`exact_signed_history`，比较完整消息数组：Anthropic精确三条，Gemini精确两条，同时核role／顺序及原signed内容；原helper之外所有字节相同。原失败发生于Anthropic循环，随后Gemini分支未执行，不补记通过。[原诊断](qualification-hmi-full-sweep-20261106/native/cartesia-eager/diagnosis.json) · [应用核验](qualification-hmi-full-sweep-20261106/native/cartesia-eager/application.json)

`user_speech`原16项中15通过、1失败。名为silent HTTP的case实际启用了Private cache，180ms生产期限先在Lookup阶段耗尽，原错误是`user_speech_cache_timeout`；原失败没有实际HTTP admission证据，不能写成HTTP超时被产品吞掉。该case现在显式选择Disabled cache进入HTTP阶段，仍保留180ms原预算、1500ms上界、两种chatty状态、精确`user_speech_timeout`、真实request=1／closed=1、无PCM与下一turn未执行等断言。其它Private cache case不变；其中原独立cache-lock期限case已实际通过，范围单独成立。原Private-cache失败仍是失败；新case对未使用空cache目录的检查不构成cache回收资格。[最小候选与边界](qualification-hmi-full-sweep-20261106/native/user-speech-http/audit.json) · [应用核验](qualification-hmi-full-sweep-20261106/native/user-speech-http/application.json)

272671首轮严格Clippy唯一owned错误是Grok地址诊断对固定4／16-byte IP地址使用`chunks_exact(4)`；现改成`as_chunks::<4>().0.iter()`，保留同样的native-endian u32转换和原输出格式。没有加allow或排除target。原strict命令exit101仍保留，新source的strict结果独立结算。

### 3.10 新 default 的 MoQ 返回值不符仍开放

0cc16上的原`cargo test --workspace --locked`第5轮gate记录区间185.088316秒（含末端源码检查）、exit101，source前后不变。失败位于`pipecat-moq --lib`的`tests::absent_peer_has_a_bounded_deadline`；该target为80pass／1fail、2.10秒，这不是整条命令的测试总数。原断言要求`Err(Error::Timeout("peer announcement"))`，但未打印实际返回值，因此当前只能确认结果不匹配，不能指定为协议错误、握手超时、清理失败或资源压力。[原gate](qualification-hmi-full-sweep-20261106/native/full-default-pin-r5-observed/gate.json) · [原返回末段](qualification-hmi-full-sweep-20261106/native/full-default-pin-r5-observed/runtime-tail.log)

该fixture的同一个150ms配置先作用于WebTransport握手，后来再作用于peer announcement；公告错误返回前的cancel清理也可能返回其它错误。已在c1c3提交的最小诊断仅在原断言失败时补实际Result、整个connect耗时、两项配置期限和accepting task的is_finished采样。原150ms、匹配条件、spawn／await／cleanup顺序不变，没有新增retry或接受其它错误结果。耗时包含connect内部清理，不被解释成公告等待时长；is_finished不证明特定握手阶段。原候选文档中的“未应用”保留为其创建时状态；实际应用以c1c3发布收据为准，新检查不改写已经完成的0cc16完整矩阵或原r5错误值缺失。[原失败切片绑定与候选](qualification-hmi-full-sweep-20261106/native/moq-absent-peer-diagnostic-candidate-r1/source-and-slice-binding.json) · [Root只读核验](qualification-hmi-full-sweep-20261106/native/moq-absent-peer-diagnostic-candidate-r1/root-review.json)

实际compiler-artifact所指的`moq-net 0.3.7` registry文件已取回；`client.rs`与官方`b00be0ffc3734addcdd5e6ad4b348ae055e70957`原文逐字节相同，`.cargo_vcs_info`也匹配。当前固定Lite06分支同步返回`start_lite`，所以没有发现可据以新增“中间异步协商deadline”的产品缺口；原r5缺失的实际错误值仍未恢复。[实际依赖与原target字节核验](qualification-hmi-full-sweep-20261106/native/moq-diagnostic-preparation-r1/root-verification.json)

同一0cc16的独立default no-fail-fast随后完成，gate记录区间392.459805秒、exit101；MoQ这个case通过，最终三个failed targets仍是原SmartTurn pipeline／VAD lib／analyzer。该独立通过没有恢复原r5遗漏的错误值，也不清除r5的失败。原all-features阶段及后续独立资格仍分别结算。

c1c3的新诊断窗口已完成20／20轮，使用原81项whole-suite、默认并行，不设置全局test-threads=1；原150ms与精确Timeout断言不变。实际job `1a1b20becfb-fc05-7c2f`权威终态succeeded0／complete／durable，原stdout报告20项attempted／started／completed／successful均为20，execution／containment／integrity均为0，source与被观测文件前后不变。它是原81项的重复检查，不增加1,620项独立覆盖，也没有补出原r5缺失的Result或证明原失败已解决。[Root权威状态](qualification-hmi-full-sweep-20261106/native/moq-diagnostic-pin-r1/root-runtime-status.json) · [原427-byte结果](qualification-hmi-full-sweep-20261106/native/moq-diagnostic-pin-r1/root-runtime-stdout.log)

完整MoQ原件到齐后，Root对20份7,561-byte原log逐一核81个原case名、各自ok状态及唯一81／0／0 summary，均一致；actual compiler-artifact为新编译的57,598,272-byte同一library harness，SHA256 `5c34c6efe9d16f8b0bc3d9ea52d81470a380be01c6f995097df3408bed3e46a6`，profile.test=true、fresh=false。whole runner104.430276秒与20轮窗口43.486857秒、child区间合计42.573646秒分开记录；原r5结果仍无法回填。[20轮完整原名／日志／清理复核](qualification-hmi-full-sweep-20261106/native/moq-diagnostic-pin-r1/root-verification.json)

### 3.11 扩展补充中新出现的缓存 Lookup 期限失败

0cc16的唯一all-features/all-targets no-fail-fast补充已实际结束，gate记录区间534.013946秒、exit101；同driver的后续lint／格式也已结束并分别记账。`pipecat-evals --lib`的222项中220通过、2失败，原harness用时4.72秒。`timely_command_between_empty_poll_and_timeout_is_not_discarded`与`late_commit_cannot_extend_the_original_production_deadline`都在`tx.lookup().await.unwrap()`得到`Timeout`，还未到达各自的command gate、commit、join及后续持久化断言。

源码中，两项原100ms production deadline均从native worker启动前起算，涵盖Namespace open／scan／lookup。现有日志未给出单case lookup耗时或具体哪一处native期限检查，因此4.72秒不能当作lookup耗时，也不能归因CPU／I/O／资源压力。这个失败没有观察到“及时命令被丢弃”或“迟到commit延长期限”所需的后续行为；原两项仍失败，未改变产品／测试期限或重试到通过。[源码阶段核验](qualification-hmi-full-sweep-20261106/native/evals-cache-deadline-review-r1/source-review.json) · [完整222-case原日志与字节核验](qualification-hmi-full-sweep-20261106/native/evals-cache-deadline-review-r1/raw-verification.json)

### 3.12 严格 Clippy 新揭示的测试初始化写法

0cc16的严格命令仍以101结束，唯一实际lint位于`pipecat-evals/src/simulation/quality.rs`的既有ledger边界测试：`QualityLedger::default()`后立即给`entries`赋值触发`field_reassign_with_default`。c1c3的最小修补将该两行改成带`entries: vec![json!({}); 2048]`与`..QualityLedger::default()`的结构体初始化。2048条原JSON值、其余所有字段默认值、原`user("x", 2).is_err()`断言及全部产品实现字节不变；没有加lint allow。Root与独立审查均核对撤销唯一替换即可逐字节恢复0cc16原文件。修补已在c1c3应用；候选审查保留创建时原文，0cc16严格命令的失败仍保留。c1c3上原Clippy命令已单次执行并exit0，两个改动文件各自格式检查也为0，与旧失败分开记账。[最小候选核验](qualification-hmi-full-sweep-20261106/native/quality-ledger-clippy-candidate-r1/review.json) · [独立审查](qualification-hmi-full-sweep-20261106/native/quality-ledger-clippy-candidate-r1/independent-review.json)

最后strict补包已核35份完整原件与两份Q源码maps。原Clippy argv与0cc16原命令逐参数相同，原14,726-byte完整log及实际exit0相符；两个文件的`--check`各自为0，28个被观测文件前后匹配。Clippy child阶段为172.473324秒，整个runner174.911892秒，两者不混作纯编译CPU时间。Root同时核对各有界阶段的实际leader reap及所观测PGID消失，不宣称逐一join所有descendants。[原严格log](qualification-hmi-full-sweep-20261106/native/strict-clippy-after-moq-pin-r1/strict-clippy.stdout.log) · [原argv与退出／完整性复核](qualification-hmi-full-sweep-20261106/native/strict-clippy-after-moq-pin-r1/root-verification.json)

## 4. 真实媒体与本地模型的资格范围

### SFU 三个实际场景

视频不是仅检查 API 接受 publish。fixture 从远端 `NativeVideoStream` 取得 I420，转换 RGB，核对 remote identity、Camera source、track SID、320×240、有效 payload 和 pipeline projection；先收到第一种图案 3 帧匹配后才切换第二种旋转 RGB 图案，再观察 3 帧。内部色块平均 channel error ≤30、dominance ≥95%。它证明合成图像的真实传输与解码，不证明实际 camera、帧率、同步播放时钟或视觉质量。

Participant control 用 publisher、受控 subscriber 和 witness subscriber。基线两订阅端均取得非零 PCM；mute 要求实际 unsubscribe，再用固定 500ms drain 后的 1 秒窗口观察目标零 PCM、witness 持续非零且后半段仍到达、publisher 继续接收新的输入 chunks。Unmute 要求同一 track SID 的重新订阅和新非零 PCM，witness subscription 不变。它不是只凭 command 返回成功判断控制收敛。

最终0cc16／Rust1.99.0的三个SFU case全部实际通过，包含在下面19命令／21项总数内。两种视频图案各decoded3／matching3；mute的1秒窗口受控端0帧、witness非零PCM100帧、publisher接受100份新输入，unmute后同SID完成第二次订阅并取得10份新PCM。三个server均在SIGKILL请求后由try_wait实际回收，收据记录10ms；这不是graceful server exit。视频／PCM场景的peer close_succeeded=true、state=Closed、failures=[]，原audio/data场景的Drop fallback也有实际server reap。

三个SFU case显式ignored、串行执行，避免SDK的进程全局WS client互扰，保存server／media／cleanup原记录。旧ae564／1.99.1的3passed仍只是那次独立源码结果，不与新三项相加成六项独立覆盖。[最终原件逐命令核验](qualification-hmi-full-sweep-20261106/native/model-optins-pin-r2.root-verification.json)

### 合成中文、Piper 与 Whisper

冻结0cc16／Rust1.99.0的最终独立批次已完成：**19条命令、21项Rust case执行，全部command／wrapper exit0**。这不是19个模型，也不包括下面单独执行的AISHELL example。32个输入文件和整棵eSpeak data前后摘要一致；外层native driver已实际reap，所观测PGID前后均不存在。Root核对131份完整原件、21,020,427 bytes、40份source maps与19条原summary；全部source canonical为`1e07f773ca2827e8db5b06175f96137b5d85bb6dcb68229d29601efe29658535`。该map含全部Git路径，不能用最终文档HEAD替代。[原批次](qualification-hmi-full-sweep-20261106/native/model-optins-pin-r2/batch.json) · [Root归档核验](qualification-hmi-full-sweep-20261106/native/model-optins-pin-r2.root-verification.json)

原131件模型包没有19份独立requests/runner-execution.json，这个已公开范围说明保持原样。最后366件runtime包补齐19份runner收据及95份伴随原文件；独立复核与旧batch／gate的实际argv、环境、source、脚本摘要及exit逐项相同，没有新增执行或追加21项覆盖。[补件范围与逐项对应](qualification-hmi-full-sweep-20261106/native/model-optins-pin-r2/runner-supplement-independent-review.json)

| 范围 | 命令数 | 实际case数 | 结果与边界 |
| --- | ---: | ---: | --- |
| 本地SFU | 1 | 3 | 真正server／远端解码／控制witness，合成媒体 |
| 合成中文TTS→STT | 1 | 1 | 原case包含8条transcript，不再算8项测试 |
| Piper CPU与生命周期 | 4 | 4 | 真实ORT／eSpeak／Piper，原English文本与原断言 |
| Whisper／runner owner约束 | 13 | 13 | 包含1项拒绝启动native的browser约束，不能全称为推理 |
| 合计 | 19 | 21 | 每项原command与wrapper均为0 |

新中文诊断使用固定small-q5_1模型、四句Piper合成中文，每句clean与seed17、范围[-300,300]的固定噪声各一路。实际原输出如下，clean与noisy两路在文字上各自相同：

| 原句 | 两路实际识别 |
| --- | --- |
| 你好，欢迎使用本地语音助手。 | 你好欢迎使用本地语音助手。 |
| 请停止播放，保留当前会话。 | 请停止播放保留当前,挥话。 |
| 今天我们测试中文语音识别。 | 今天我们测试中文语音识别。 |
| 设备重新连接以后继续工作。 | 设备重新连接以后继续工作。 |

仅去Unicode标点与空白、不作繁简转换或拼写修复，8条结果共94个reference codepoints、2次替换，诊断CER为2／94≈2.13%；两次均为“会”→“挥”。原case只要求非空、来源与生命周期等约束，没有CER通过阈值，故exit0不等于中文质量通过。整个TTS→resample→STT链的输出也不能独自分离发音或识别哪一方导致差异。

8次原STT计时为8.795723–9.285048秒，计时从submit_with_source之前到其result返回，不含前置Piper／resampling、后续显式verify／release或最终shutdown。n=8的p50为排序第4个9.084715秒，p95与p99均取第8个9.285048秒，不作为总体尾延迟；RSS采样781,164–794,960KiB不是峰值。原case未输出captured PCM duration，不能据此推RTF或mic-to-speaker延迟。中文gate区间190.293447秒包括构建与末端源码检查，libtest原summary为109.74秒，整个19命令外层829.720409秒，均与单次STT计时分开。[8条原JSON与独立检查](qualification-hmi-full-sweep-20261106/native/model-optins-pin-r2/chinese-independent-review.json)

旧ae564／1.99.1首次在加载模型前因max_segment_bytes超2MiB产品上限失败，修正private fixture后单独执行；这个历史失败不被最终批次清除。

中文测试不是录制真人：4 句固定中文由真实 Piper 合成，每句 clean 与固定 seeded-noise 两路进 Whisper，共 8 次 transcript，原断言为非空、PCM 来源摘要、retention／release 等路径要求。它没有 CER 质量阈值、真实噪声／回声、OS grant 或用户听感验收。private scope 与 generation 是该次测试 fixture，不能采用为生产权限。

Piper 四项使用真实 ORT/eSpeak/Piper CPU 路径，保留固定 English fixture 文本和原 PCM length／energy／cancellation／global owner／resampling／capacity 断言；这也不构成Chinese voice对英文的质量验收。long-pipeline原native PCM为1,197,036 bytes，在固定48kHz单声道PCM16下为12.469125秒；backend 1,198,080 bytes为12.48秒，585个source chunks进入312个backend chunks，补1,044零bytes，最终receipt=Discarded。27.07秒是该libtest执行时间，不是30秒音频；原超过128 queue capacity断言仍成立。SlowDiscard消费端不证明用户听到。没有为了凑数补跑六个合成 ONNX case。Whisper／runner 的 13 项使用已核对的 multilingual base 模型，分别检查末 owner drop/join、取消、溢出／过大帧、设置恢复、会话 admission／expiry 与独立 owner 释放；其中“不启动 native worker”的 case 仍计为生命周期约束，而不是模型推理。

英文专用模型语义、JFK fixture、Moonshine 其它资产、RNNoise 特定 ABI／C 参考输入，以及真实长中文输出的物理 case 均按实际输入选择保留，不由现有 Chinese Piper／multilingual Whisper 文件冒充。

### 单条公开真人中文录音的离线观察

在同一0cc16／Rust1.99.0上，原`stt_fixture`普通example对已恢复、已哈希的AISHELL WAV只执行一次：multilingual base、language=zh、beam5、temperature0、原no_speech=0.4与通用中文prompt保持固定，没有换录音、改reference或重试。普通example实际产物profile.test=false；16个被观测文件及source前后保持相同。执行与wrapper均为0，它不增加上面21项Rust case数；原runner的quality_passed=null，没有设置或通过质量阈值。

| 项目 | 实际记录 |
| --- | --- |
| 固定reference | 广州市房地产中介协会分析 |
| 实际识别 | 廣州市佛地产中间协会分析。 |
| 录音 | PCM16／mono／16kHz，68,496帧，4.281秒 |
| 仅去标点／空白后的编辑差异 | 广→廣、房→佛、介→间；3／12=25% CER |
| 模型load／submit-result | 0.206838秒／5.038608秒 |
| 原segment timestamp | 0–30,000ms，超过实际4,281ms录音 |

reference来自预先固定的WeNet annotation commit `be6ad97fdb512589df113042f7ff3a29d392fb38`；没有对繁简进行折叠，所以“广→廣”按一处差异记账。n=1不证明普遍中文质量，也不证明用户当前麦克风链路。原segment末端30,000ms按原始值保留，不能称录音30秒、偷偷截断或据此宣称时间戳对齐通过；现有资料不足以归因native timestamp原因。

原example在输出成功JSON前await shutdown，进程wait返回0；正常分支的stage.cleanup为null，没有另外采样正常PGID消失，所以不补造“所有descendants逐一join”的结论。native process阶段14.506751秒涵盖启动、输入读取、运行和shutdown，整个runner21.634362秒还包含构建与末端检查；它们都不是麦克风到扬声器时延。[原stdout](qualification-hmi-full-sweep-20261106/native/aishell-human-pin-r1/inference.stdout.log) · [固定输入与运行](qualification-hmi-full-sweep-20261106/native/aishell-human-pin-r1/result.json) · [Root独立复算](qualification-hmi-full-sweep-20261106/native/aishell-human-pin-r1/root-verification.json)

## 5. 浏览器已从 launch-blocked 推进到实际运行

最终 Chrome **156.0.8078.79** 保留正常 sandbox，`protocol-r4` 23 组与 `input-r3` 4 组全部通过；两个报告的 failure 均为 null，page／cleanup errors 均为空。前者实际执行 WASM、loopback WS/protobuf、配对 WebRTC、RTVI text/image/transcript/toast/job-group、取消、recvonly 和资源关闭；使用 synthetic microphone 与 autoplay override。后者没有 autoplay override，可信 Connect click 后原生 AudioContext 为 running，16kHz／16,000 samples synthetic PCM 有 start／ended，但音频输出静音。

输入轮的 Chromium CDP composition 保留 `中文 🦀\n第二行`，composition 期间没有提前发送，显式 Send 只产生一次准确 protobuf 文本。独立空 textarea 控制同样观察到 compositionend 的 trust bit 为 false；最终 fixture 记录这个实际值，仍要求 trusted start/update 和准确字面内容。它不证明 OS 拼音候选 UI。真实 loopback TCP abort 后清理并手工重新 Connect，不重放草稿；固定 300ms 无自动重试的观测不被扩展成无限期或 WAN 恢复承诺。

原 114 条 protocol Python assert 的有序 AST 不变；五处 50ms 窗口不变。早先 Playwright evaluate 返回函数误触发、FileReader 安装、诊断 command-line flag 和 IME trust 假设的失败均保留。最终 delayed reader 的 7 次真实读取都得到原始 1,028-byte JPEG 与相同 SHA，远端页错误和 AudioContext closed 状态也纳入清理。截图只有 **960×480** 一个 viewport。[完整 browser 说明与原始证据](qualification-hmi-full-sweep-20261106/browser/README.md)

实际 bundle 来自 e526 的 Rust browser／locked dependencies；478 发布了最终 helpers，直到710编译输入不变；后续媒体独立测试 helper 和文档也不改变 browser 编译输入。打包后 WASM SHA 为 `6dc704bd6d05f09b53fbe4136f6a7ea119bd7bcbf8cd4b34e28818980d3a622d`。远端 `application_source_present=false` 原值保留，源码绑定来自独立 Git 与资产核对。新的 native/WASM 补资格有自己的运行身份，不给原实际 browser 运行重复加分。

新的 Rust 1.99.0 补资格在独立 `source-browser/target-browser`、710 的实际源码上完成：原生 `cargo test --locked --offline` **54 passed／0 failed／0 ignored**，23.763411 秒；原生 `cargo clippy --locked --offline --all-targets -- -D warnings` exit 0，8.087912 秒。r1 随后的官方 WASM std 下载只有 8,338,068／23,152,600 bytes，XZ 截断且摘要不符，因此 **r1 整体仍为失败**。r2 只恢复同一官方组件，完整摘要匹配后安装到独立 WASM sysroot；仅运行 WASM target 的 strict Clippy，exit 0，42.282510 秒，没有重复计入原生测试。

10 份已记录的前后 source map 覆盖同一 72 个文件、摘要相同。r2 核对六个 native tool binary 前后摘要不变；r1 的 native-before 字典未在失败前落盘，不能把 r2 摘要追记成 r1 的前后证明。完整 raw export、48 份原始文本、官方 manifest／range 记录、脚本和实际 runtime returns 已按原字节核对。[新补资格与原始证据](qualification-hmi-full-sweep-20261106/browser/requalification/README.md)

## 6. 历史失败与本轮失败账

| 事件 | 原结果与当前解释 |
| --- | --- |
| 原 WebRTC invocation 25 | 前24次通过后，当前连接对 shutdown 前 data-channel setup timeout；原因仍开放，后来30次及本轮任何通过不能清除。[原 log](qualification-hmi-followthrough-20261106/webrtc-fixed-repeat.log) |
| 历史完整 workspace UDP leak | `schedule=5, iteration=24` 的历史记录保留；新的确定性 DTLS 修复和新的完整 gate 不能自动证明原确切随机失败已解释。历史文档不是原 raw execution log |
| 基线 prebuild／本轮 full r1 | InvalidPayload match E0004，0 tests；4处消费者修补后续复验 |
| Rust1.99.1 full r2 | Nova83 pass／2fail，误用Inworld普通example有实际二进制证据；完整运行在此停止 |
| 精确 pin default r1 | media-transport 独立readiness target 9pass／2fail；尾部 user compatibility 点号被helper错算，原signed内容／产品adapter不变 |
| 精确 pin default r2 | RTVI first-answer-timing target 153pass／2fail；辅助端按旧 nested-text 读取本应为 scalar string 的工具结果，三处同类fixture最小修正，产品adapter不变 |
| 精确 pin default r3 | Grok app_cli 11pass／1fail；fixture listener 单次重绑 AddrInUse。当前没有原 inode／TCP 状态证据，原因仍开放；新增诊断而未放宽 oracle |
| Grok 诊断启动 r1 | source diff 白名单遗漏已发布的主报告路径，fetch／diff 后、checkout／编译／selection 前退出127；0 rounds，source-pin仍为干净ae134。这是运行器前置失败，精确16路径白名单修正后用新job/evidence身份执行，不计产品test失败 |
| Grok 诊断 r2 | 两轮12/12后，第三轮11/1失败并停止；不同case重绑errno98，原inode／endpoint在事后有界快照为空，无截断；不归因为TIME_WAIT或资源压力 |
| Grok strace r1 | 首轮11/1即停止，第三个不同case；父close成功后单次bind errno98，与另一child pre-exec窗口重叠。未直接见到child原inode，原两轮原因仍开放 |
| 精确 pin default r4 | 329.03秒／exit101；SmartTurn pipeline 0pass／2fail，原1e-5概率门不变。控制端等待时长不计为native时长 |
| 精确 pin default no-fail-fast r1 | 324.642713秒／exit101；3个SmartTurn targets失败，直接speech概率0.9685282对0.9823202，剩余targets／doctests继续执行；不清除r4 |
| 首轮all-features/all-targets | source272671，原219个harness、2175pass／15fail，bot_audio_response停止；同配置补充731个harness、8839pass／24fail／63ignore，共8个失败targets，不清除原fail-fast结果 |
| Grok扩展补充 | 原12项10pass／2fail，两个listener单次重绑失败；与default／repeat／strace分别保存，不相加成独立覆盖 |
| Cartesia扩展补充 | 原34项33pass／1fail；signed blocks精确相同，旧helper拒绝原adapter的第三条user点号；原33／1保留；0cc完整expanded补充已为34／0，包含在其8856／7／63总数内 |
| 静默HTTP扩展补充 | 原user_speech 15pass／1fail，180ms先在Private-cache Lookup耗尽；无原HTTP到达证据。新fixture选择HTTP阶段，不把原cache失败改记为通过 |
| SmartTurn C1／C2／C3 | C1的3Run执行0而三项原概率门失败；C2的DISABLE／BASIC各3Run执行0、speech／tone仍失败；C3另一官方C API库的ALL三项与C1逐bit相同，仍全部失败；wrapper比较均非零，不改golden／容差／产品优化设置 |
| 首轮strict Clippy | 原命令exit101，实际报出Grok诊断chunks_exact_to_as_chunks；仅改等价固定数组分组写法，不加allow，后续strict单列 |
| 中文合成诊断 r1 | 私有scope的 max_segment_bytes=16MiB 超过2MiB产品上限；在LocalSpeech::load准入即失败，没有native model或8次STT执行；只修fixture配置 |
| Browser protocol r1/r2、input r1/r2 | 各自原partial groups、stdout/JSON/helper与修正均保留；最终r3/r4不覆盖旧失败 |
| Browser Rust 补资格 r1 | 原生54 tests 与 strict Clippy分别通过；随后WASM std下载截断，整体failed。r2仅WASM恢复与Clippy通过，不改写r1整体结果 |
| 原本地两次SIGKILL构建 | 0 tests；没有证明OOM或协议原因，当前raw不可访问与已知历史观察分开 |
| 原 fmt-all | vendored递归／缺module的失败保留；owned-member格式成功不是该命令成功 |
| 原两项远端 UNKNOWN | 原host/job ID对账未取得有效状态，不重放；后来的环境检查不清除UNKNOWN |
| 执行入口中断、依赖下载失败 | 原记录与缺失可用性分开；完整SHA不符的包不安装，锁定版本不替换 |

### 0cc完整复验中新增并保留的失败

| 事件 | 实际结果与边界 |
| --- | --- |
| default r5 MoQ absent-peer | 整条命令3792pass／1fail／11ignore，MoQ target 80／1；实际Result未打印，仍未知，后续同源通过不补写原失败原因 |
| expanded补充的evals cache Lookup | lib target 220／2，两项在配置100ms生产期限的Lookup阶段得到Timeout；command gate／commit／join及后续持久化断言未到达 |
| 0cc strict Clippy | 原命令101，唯一实际error为quality ledger边界fixture的field_reassign_with_default；原raw与c1c3等价初始化修补分别保存 |
| AISHELL单条真人录音 | 离线执行0，原输出3／12字编辑差异；segment end=30,000ms超过4,281ms输入，质量与时间戳资格未通过本次观察建立 |



## 7. 用户原能力表：本轮之后的完整账

| 范围 | 本轮可以确认的推进 | 仍需补齐／独立验收 |
| --- | --- | --- |
| Pipeline、流控、错误与取消 | 前轮实际 payload／queue reservation 和 Linux supervision 保留，本轮补消费方完整错误匹配及跨 workspace 运行 | RSS／native retention、任意callback抢占、非Linux containment、已提交效果回滚与生产对账 |
| 本地语音与设备 | 原ALC897环境与模型资产恢复；模型／SFU批次21项执行；新增单条公开真人中文离线观察，3／12字差异 | 用户本人中文、广泛真人质量、回声／噪声、OS授权、拔插／撤权、物理录放音和用户实际听到 |
| 云端 providers | 修复四个普通example产物冲突，保留专属协议与兼容preset区分，扩大locked工程矩阵 | 真实厂商auth／断网恢复／finality／限流逐厂商验收；preset不是完整API，也不由本地SFU替代 |
| Turn／工具／MCP／flows | 原Hepta继续唯一owner，本轮没有生成第二套会话或工具执行权威 | 同thread生产binding、签名证据进入原Core、恢复与已发生效果对账 |
| 共享媒体与评测 | cyclic image／sprite、音频、用户图片原有能力继续按源码记；浏览器真实FileReader和send-file已实测 | 模拟／合成评测不代表真人质量；不把“仅文本评测／无共享媒体”旧概述继续当现状 |
| 原生WebRTC | DTLS既有修复保留，增加可归因setup诊断与受控真实UDP观察；0cc16固定100次窗口完成，原失败保留 | 原timeout与历史随机UDP失败仍开放；native video/camera/screen、adaptive jitter、PLC、Opus FEC、统一播放时钟、已有peer ICE restart／track replacement仍缺能力或资格 |
| LiveKit | 实际本地SFU三scenario，包括remote video解码、participant mute/unmute的独立witness | 托管SFU、真实camera／screen、生产participant变更／网络恢复、多端时钟和用户感知 |
| 浏览器／RTVI | 正常sandbox中的真实WASM/WS/WebRTC、CJK composition、静音正常autoplay调度、loopback断线手动恢复、DOM与资源清理 | 真实麦克风权限／OS输入法、声学输出、多浏览器、多viewport、WAN/TURN、生产owner媒体对账 |
| 生产Hepta HMI | 原10/29部署资产可定位，固定Agentd的TextPort+NoAudio边界重新确认 | 同次真实owner/thread、签名producer/replay witness、当前generations/grants、采用模型与真实mic/speaker组合尚未建立 |
| 质量与发布 | 实际失败、source／toolchain／artifact／target身份和raw证据逐项记录，继续原Draft PR | 当前工程通过不能直接越过生产组合门槛；不以不同部署或重复通过清除旧失败 |

完整原实现范围和厂商目录仍见 [全类别审计](AUDIT_UPSTREAM_20261106.md)。本轮没有将 neuron shadow、只读 organ 或通用库提升为权限／效果 owner。

## 8. 已恢复的原设备环境与生产下一道门

原 `desktop` 上的 `hepta-hmi-20261029` 已恢复定位，codec是ALC897，原Pipecat／Hepta checkout分别为干净ec40f982／7bbdd715。核对3份历史日志与7个指定模型／runtime文件摘要；没有在环境探针中重新哈希整个eSpeak data tree。指定mic／lineout／headphone controls为off，SPDIF phantom on不证明声学输出。Pocket4仍是另一套旧Python部署，不能替代这次Rust验收。[原环境记录](qualification-hmi-full-sweep-20261106/environment/README.md)

限定的进程comm／unit查询未给出可用生产入口；5个HEPTA环境变量未设置只针对该次SSH job。11/04四个明确JSON是工程build/test记录，不能推出未读取目录或整机无owner。只读历史检索也未提供可验证的原thread/runroot/unit/socket组合。不能将这些窄范围absence写成全局不存在。

追加的单次定向 probe（desktop `1a1b12e0587-fc05-76a1`）完整读取12个选定工程文件、117,406 bytes，保留每文件hash，权威终态exit0且输出完整。候选只有4条字面量 `ThreadId`，没有实际unit/socket/run-root/thread。10/29人声／声学材料与既有AISHELL／独立stt_fixture历史对应，原报告明确没有live owner/model turn/tool effect；其余11/04诊断JSON仅有文件名信息，内容仍未观测。没有新的具体入口值支持进一步定向恢复，未扩大为无目标的全目录扫描。[追加探针与范围](qualification-hmi-full-sweep-20261106/environment/owner-entry-followup-r1.json) · [原native闭环历史](HMI_NATIVE_CLOSEOUT_20261029.md)

生产下一步需要以下材料位于**同一次运行时间线**：真实Supervisor/Agentd identity、spawn generation与原AppServer binding；既有thread和canonical session；已注册签名producer、公钥信任epoch及独立replay witness；当前device/organ/capture generations与有效grants；原owner采用的不可变Whisper/Silero/Piper/ORT/eSpeak配置和private media scope；实际麦克风、扬声器及独立播放观察。先验证同thread文本／恢复，再验证真人中文到来源receipt、Core admission、当前epoch输出和实际设备，之后才是拔插／撤权／崩溃／UNKNOWN对账。

缺少上述组合时，不启动替代owner、创建替代thread、借用历史qualification grants，也不把合成PCM或buffer提交视为用户听到。[逐项材料与边界](qualification-hmi-full-sweep-20261106/PRODUCTION_BOUNDARY.md)

## 9. 证据的可访问性也计入结论

第二次和第五次本地入口恢复后，原 scratch 路径在新入口不可见；原 desktop jobs 与 browser artifacts 仍按原身份读取，没有因入口变化重放。第五次恢复从已发布52243a重建checkout，95个checkpoint文件逐一匹配中断前保留的size／SHA256／Git blob，root README仍为0字节，Grok诊断源码仍为原摘要。未提交的分析段落重新依据事实撰写，未将其描述成追回原raw；原始材料只从远端原文件、完整保存内容或核实的Git blob恢复。缺失raw仍明确标记：原WebRTC60文件仅精确恢复4个，其余56个missing；原format archive和早期browser native raw不被摘要重建。新的验证使用新identity和专属target，不能冒充旧记录。[第五次恢复收据](qualification-hmi-full-sweep-20261106/fifth-entrance-recovery.json)

最后交付另含两个终态原件包：final-runtime为366份原件、10,683,065 bytes，覆盖WebRTC100、MoQ20、Q source preparation、AISHELL、C3与19套模型wrapper补件；strict-Q为35份原件、1,896,241 bytes。Root逐份核archive／解包bytes与原manifest，并复算12份source maps；5份Q map还与独立从实际Git树推导的2,422路径map逐项相同。Archive含manifest与派生文件后的members数分别369和37，不把这些派生文件计为新执行。C3 wheel／独立共享库两份大binary的明确排除清单、原size／SHA／来源保留；其它模型和编译产物也仍以所观测身份记账，不声称归档了完整运行系统。[终态runtime完整核验](qualification-hmi-full-sweep-20261106/native/final-runtime-pin-r1.root-verification.json) · [strict补包核验](qualification-hmi-full-sweep-20261106/native/strict-clippy-final-q-r1.root-verification.json)

完整探针输出、原始失败JSON/log、版本／SHA／source-before-after、普通executable compiler-artifact和实际exit状态是不同证据。文件存在、已构建、test listing、运行返回0、效果真实发生和用户感知也保持区别。[总证据索引](qualification-hmi-full-sweep-20261106/README.md) · [可用性账](qualification-hmi-full-sweep-20261106/EVIDENCE_AVAILABILITY.md)
