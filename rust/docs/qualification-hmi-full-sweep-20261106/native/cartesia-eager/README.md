# Cartesia eager expanded 原失败与最小候选

原 source `272671b95297313d24db921eceda8e95f3a8d545`、tree `94424ffa2a8fbcda0f81321ee68e7461663e191c`，固定 Rust 1.99.0 执行 `cargo test --workspace --all-features --all-targets --locked --no-fail-fast`。完整原始日志仍保留在 expanded-and-reference 包中；本目录只从其取出一段完整 target 原字节，没有重跑。

`cartesia_turns_eager` 原结果为 34 cases 中 33 passed、1 failed，耗时 0.51s。失败是 `eager::review_crosssocket_effects::signed_concurrent_owner_restart_keeps_foreign_receipts_out_of_both_next_requests`，provider 为 Anthropic。catch_unwind 后双方 cleanup 均为空错误列表，最终原断言报告 signed history 不匹配。

## 原失败的可验证原因

原 next-request 直接包含完整的 `live-B` thinking、signature、text，且与 signed_response + owner_signed 生成的预期逐项相等。第一个 user transcript 也相等。原 helper 的三个合取条件中，仅 `messages.len() == 2` 为 false：实际第三条是精确的 `{"role":"user","content":[{"type":"text","text":"."}]}`。

这条消息来自既有 provider 适配器规则：`pipecat-services/src/adapters.rs` 的 anthropic 分支在 converted wire history 以 assistant 结束且模型不符合固定 prefill 前缀时追加它。Harness 对 Anthropic 使用 `loopback`，不属于该固定前缀集合。既有 `anthropic_trailing_user.rs` 明确覆盖这种规范化并检查原 Context 不变。这里不是用户 committed history 多出话语，也不是 signed parts 被更改。

Gemini Harness 使用 `gemini-3-pro`，符合该适配器固定的 prefill 前缀，仍是两条 wire history。本失败 case 先运行 Anthropic，在末尾断言失败后停止，因此不能从本段原 raw 声称该 case 的 Gemini iteration 已执行。

## 独立候选与限界

`crosssocket_effects.rs.candidate` 只替换 `exact_signed_history`。现在直接精确比较完整数组：Gemini 是 user/model 两条；Anthropic 是 user/assistant 加一条精确 user dot。原 user 和 signed blocks/parts 内容不变，同时明确核验角色与数组顺序。没有放宽成任意 extra message 或 `len >= 2`。

候选之外的所有 helper、case、输入、协议、期限、并行与 cleanup 正文字节完全不变。候选文件 23,727 B，SHA256 `96ad703190d83041feea27fe1570d158d503491a958fd80b614615c668c77cf0`；patch SHA256 `df3d82963a338e8ed8c604094ef2ec192f08db7852adcd2cf04afdc7fd5806c3`。此目录生成时作者没有将候选应用到仓库；是否发布与实际 native 复验由 Root 统一管理。

`diagnosis.json` 是对原 request 的只读解析，不是新 Rust runtime pass。`actual-anthropic-next-request.json` 是原 panic 内 JSON 的便于阅读版本；原 escaping 和完整 panic 仍在 `target-original.log`。`slice.json` 给出完整原 log SHA、精确 byte range 与 target slice SHA，`source-bindings.json` 记录所有相关源码与失败 commit 的字节一致性。

原始失败保持失败记录；修正候选之后的完整原 harness 仍需在新的 source freeze 上执行。没有改变产品适配器或以另一种模型绕开本断言。
