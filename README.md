# 双轨结算核台

本地合成交易事件与日终账单的双轨封账核对。运行期仅 Python 标准库，无网络、真实支付接口或真实资金。`python -m dualsettle demo` 一条命令生成事件、费率、账单，重建实时轨并输出 JSON/Markdown；`python -m pytest -q` 测试。

事件按交易身份去重，状态授权→确认→撤销/退款；业务时间与到达时间均保留。窗口为含起点不含终点的带时区时间段；金额使用 Decimal，费率逐交易舍入到分。日终更正行必须引用旧行，调整独立于原交易。结论为可封账、证据不足、差异；只有全体可封账时 close 才能归档。报告从结论追到来源哈希、事件、费率与批次行。错误码包括 identity_conflict、bad_state、window_ambiguous、rate_ambiguous、orphan_correction、batch_conflict、not_closable、source_changed。演示预期 M1 实时净额 99.00，日终 99.00，结论可封账。

每次核对冻结本次输入摘要与问题清单。费率、交易、账单或调整改变后，旧报告以 `effective_status=已失效` 显示，不能再把历史“可封账”当成当前结论；下一次核对会在台账中把旧归档运行置为已失效，同时保留它当时的差异证据。`pytest` 固定从本仓 `src` 导入源码。

授权和确认金额均需为正且有限，确认不得超过授权；费率必须是有限的 `[0,1)` 比例。非法金额或费率进入本次运行的问题证据，结论为证据不足，不能因为账单金额恰好相等就封账。历史运行的问题清单与新运行隔离，旧报告仍可说明当时为何被拒。

争议保留金通过 `reserve-open` 建立案件，再以 `reserve-release` 按序部分释放。每个案件、释放流水及剩余敞口保存在 SQLite；同一序号同一金额可安全重试，冲突或超额释放会拒绝。保留金影响可分配净额，不改写已封账的原核对证据。

受益人分配通过 `agreements` 导入有生效区间和版本的份额协议，`payout-plan` 按指定商户和已封账运行计算可分配额与各受益人份额，`payout-approve` 绑定核对、保留金和协议摘要批准，`payout-status` 可发现批准后输入变化导致的失效。计划和分配行持久保存；份额不完整或冲突将阻断审批。`demo-decisions` 演示保留金与多受益人分配的联合决策。

交易级差异调查（`investigate`）在金额裁定之外提供可复核的归因结论，不替代、不改写 verdict，也不会把差异强行变成可封账。供应方账单合同必须先以来源文件冻结（`ingest-contract`），合同明示两种均合法的引用语义：`tx`（账单行引用原交易身份）或 `group`（账单行引用清算组身份，组可含多笔交易，需按组汇总后再归因）。账单 CSV 通过 `ref_kind`/`ref_value` 列声明引用，事件可带 `clearing_group`；更正行沿更正链继承原行引用。案件分类为 `missing_line`（交易缺行）、`duplicate_line`（重复账单行）、`rate_basis_diff`（账单按扣费前 gross 开单的费率口径差）、`window_pending`（跨窗或迟到待转）、`amount_unknown`（引用无法解析、同额多候选身份不能确定或证据不成立）。任何结论都只按合同引用键归因，禁止凭相同金额猜测交易身份；多笔同额或一笔多行保留多对多候选证据边（`candidate`、`line_covers_tx`、`line_covers_group`、`tx_in_group`、`duplicate_line`、`missing_event`、`unresolved_line`、`correction`、`adjustment` 等）。

调查绑定原 run 的 `source_digest`：输入变化后旧调查在报告中标为历史解释，不能称为当前调查；相同输入重跑得到稳定案件编号（由 run、商户、分类与归因锚点哈希生成）和分类，且同 run 重跑幂等。同一组内 T1=60、T2=40、账单一行 100 带组号时，组引用合同认定该行覆盖两笔交易；交易引用合同下该行只对应 T1，则 T2 缺行、T1 多记 40——两种结论均来自冻结的合同键而非总金额猜测。合同未冻结（`contract_not_frozen`）或同商户合同语义冲突（`contract_conflict`）将拒绝调查或导入。

