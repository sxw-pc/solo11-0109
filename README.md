# 双轨结算核台

本地合成交易事件与日终账单的双轨封账核对。运行期仅 Python 标准库，无网络、真实支付接口或真实资金。`python -m dualsettle demo` 一条命令生成事件、费率、账单，重建实时轨并输出 JSON/Markdown；`python -m pytest -q` 测试。

事件按交易身份去重，状态授权→确认→撤销/退款；业务时间与到达时间均保留。窗口为含起点不含终点的带时区时间段；金额使用 Decimal，费率逐交易舍入到分。日终更正行必须引用旧行，调整独立于原交易。结论为可封账、证据不足、差异；只有全体可封账时 close 才能归档。报告从结论追到来源哈希、事件、费率与批次行。错误码包括 identity_conflict、bad_state、window_ambiguous、rate_ambiguous、orphan_correction、batch_conflict、not_closable、source_changed。演示预期 M1 实时净额 99.00，日终 99.00，结论可封账。

每次核对冻结本次输入摘要与问题清单。费率、交易、账单或调整改变后，旧报告以 `effective_status=已失效` 显示，不能再把历史“可封账”当成当前结论；下一次核对会在台账中把旧归档运行置为已失效，同时保留它当时的差异证据。`pytest` 固定从本仓 `src` 导入源码。

授权和确认金额均需为正且有限，确认不得超过授权；费率必须是有限的 `[0,1)` 比例。非法金额或费率进入本次运行的问题证据，结论为证据不足，不能因为账单金额恰好相等就封账。历史运行的问题清单与新运行隔离，旧报告仍可说明当时为何被拒。

争议保留金通过 `reserve-open` 建立案件，再以 `reserve-release` 按序部分释放。每个案件、释放流水及剩余敞口保存在 SQLite；同一序号同一金额可安全重试，冲突或超额释放会拒绝。保留金影响可分配净额，不改写已封账的原核对证据。

受益人分配通过 `agreements` 导入有生效区间和版本的份额协议，`payout-plan` 按指定商户和已封账运行计算可分配额与各受益人份额，`payout-approve` 绑定核对、保留金和协议摘要批准，`payout-status` 可发现批准后输入变化导致的失效。计划和分配行持久保存；份额不完整或冲突将阻断审批。`demo-decisions` 演示保留金与多受益人分配的联合决策。

大文件 JSONL 通过 `ingest-resumable` 做可恢复分段导入：在固定字节预算（`--budget`）内只按完整记录边界切段，单条超大记录除外，内存占用不随整文件增长。每个段在同一事务内提交段原始字节、已验证字节范围、起止行号、段 SHA 以及该段交易与问题的身份（`import_run`/`source_segment` 表）；崩溃重启后重新哈希全部已提交字节范围，从最后验证边界继续，不漏行、不重记、金额不重复计入。恢复前外部字节若变化（含截断）或同一路径尝试中途切换验收策略，旧进度被隔离为 `quarantined` 并返回确定错误码（`source_bytes_changed`、`import_quarantined`、`accept_policy_conflict`、`source_missing`），旧事件对任何核对都不可见。

> 离线沙箱备注：本仓运行期仅依赖标准库。若环境没有 `python` 别名或没有安装 pytest/pip，可把仓库自带的 `.local-bin` 加入 `PATH`：`PATH=$PWD/.local-bin:$PATH python -m pytest -q`。该目录的包装器指向 `python3`，并让 `python -m pytest` 使用 `tools/pytest_shim` 中仅用标准库实现的最小 pytest 兼容层（支持 `pytest.raises` 与 `tmp_path`）；装有真实 pytest 的环境无需任何包装，直接运行即可。


验收语义由导入委托书在开始时声明且不可中途更换：`--accept whole` 仅在全文件末尾 SHA 完整吻合后才让该源事件对核对可见；`--accept segment` 允许把已验证完整段提供给 `reconcile --preview` 预览核对，预览运行标注非封账且 `close` 拒绝（`not_sealable_preview`），文件末尾完成并转正后才能正式封账。任何未完成导入都会让正式核对不可封账（`source_incomplete`/`source_quarantined`）。报告（JSON 与 Markdown）对预览与正式两种形态分别列出来源摘要、已验证字节范围、缺段、全文件 SHA 与问题行。小文件 `ingest-stream` 调用与按整文件 SHA 的重复文件幂等语义保持不变；新库与旧库均通过启动时的 SQLite 追加迁移获得新表与新列。`demo-resumable` 离线演示两段文件、段后故障注入、预览拒绝封账与恢复后 A+B 各计一次。

