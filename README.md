# 双轨结算核台

本地合成交易事件与日终账单的双轨封账核对。运行期仅 Python 标准库，无网络、真实支付接口或真实资金。`python -m dualsettle demo` 一条命令生成事件、费率、账单，重建实时轨并输出 JSON/Markdown；`python -m pytest -q` 测试。

事件按交易身份去重，状态授权→确认→撤销/退款；业务时间与到达时间均保留。窗口为含起点不含终点的带时区时间段；金额使用 Decimal，费率逐交易舍入到分。日终更正行必须引用旧行，调整独立于原交易。结论为可封账、证据不足、差异；只有全体可封账时 close 才能归档。报告从结论追到来源哈希、事件、费率与批次行。错误码包括 identity_conflict、bad_state、window_ambiguous、rate_ambiguous、orphan_correction、batch_conflict、not_closable、source_changed。演示预期 M1 实时净额 99.00，日终 99.00，结论可封账。
