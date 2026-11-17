"""交易级差异调查（不替代金额裁定）。

调查器只读取指定 run 冻结的证据（evidence / verdict / reconcile_issue），
按供应方账单合同冻结的引用语义（tx 或 group）把账单行归因到交易或清算组，
形成可复核的差异案件与多对多证据边。调查不改动事件、账单或 verdict，
也不会把差异重新裁定为可封账。
"""
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP

from .store import connect
from .timeline import at, transactions, window_for
from .contract import validate_rate
from .rules import input_digest

CENT = Decimal('0.01')

MISSING_LINE = 'missing_line'        # 交易缺行
DUPLICATE_LINE = 'duplicate_line'    # 重复账单行
RATE_BASIS_DIFF = 'rate_basis_diff'  # 费率口径差
WINDOW_PENDING = 'window_pending'    # 时间窗待转
AMOUNT_UNKNOWN = 'amount_unknown'    # 金额未知/身份无法确定

DIFF_KINDS = (MISSING_LINE, DUPLICATE_LINE, RATE_BASIS_DIFF, WINDOW_PENDING, AMOUNT_UNKNOWN)


def money(value):
    return str(Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP))


def case_id(run_id, merchant, kind, anchor_type, anchor_id):
    """稳定案件编号：只依赖 run、商户、分类与归因锚点，同输入重跑不变。"""
    payload = json.dumps([run_id, merchant, kind, anchor_type, str(anchor_id)],
                         ensure_ascii=False, sort_keys=True)
    return 'C-' + hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def _chain_view(db, run_id, merchant, events):
    """把事件链还原为交易视图：金额、费率、窗口归属与冻结状态。"""
    view = {}
    for (m, tx), items, confirmed, refunded, valid in transactions(events):
        if m != merchant:
            continue
        biz = at(items[0]['business_at'])
        issue_codes = {
            r['code'] for r in db.execute(
                'SELECT code FROM reconcile_issue WHERE run_id=? AND merchant=? AND tx=?',
                (run_id, merchant, tx))
        }
        rates = [r for r in db.execute('SELECT * FROM rate_version WHERE merchant=?', (merchant,))
                 if at(r['start_at']) <= biz < at(r['end_at'])]
        rate_ok = False
        fee = None
        try:
            for rate in rates:
                validate_rate(rate)
            rate_ok = len(rates) == 1
        except ValueError:
            rate_ok = False
        if valid and rate_ok:
            fee = (confirmed * Decimal(rates[0]['rate'])).quantize(CENT, rounding=ROUND_HALF_UP)
        groups = {e['clearing_group'] for e in items if e['clearing_group']}
        view[tx] = {
            'tx': tx,
            'items': items,
            'event_ids': [e['id'] for e in items],
            'valid': valid,
            'confirmed': confirmed,
            'refunded': refunded,
            'gross': confirmed - refunded,
            'fee': fee,
            'expected': (confirmed - fee - refunded) if fee is not None else None,
            'rate_id': rates[0]['id'] if rate_ok else None,
            'group': sorted(groups)[0] if len(groups) == 1 else (next(iter(groups)) if groups else None),
            'group_conflict': len(groups) > 1,
            'issues': issue_codes,
        }
    return view


def investigate(path, run_id):
    """对指定 run 的冻结证据做交易级调查，返回 (调查行 id 列表, 是否当前)。"""
    db = connect(path)
    try:
        with db:
            run = db.execute('SELECT * FROM reconcile_run WHERE id=?', (run_id,)).fetchone()
            if run is None:
                raise ValueError('unknown_run')
            current = 1 if input_digest(db) == run['source_digest'] else 0
            window = dict(db.execute('SELECT * FROM window WHERE id=?', (run['window_id'],)).fetchone())
            # 幂等重跑：同一 run 的旧调查整体替换，但案件编号由稳定输入决定。
            for old in db.execute('SELECT id FROM investigation WHERE run_id=?', (run_id,)).fetchall():
                db.execute('DELETE FROM investigation_edge WHERE investigation_id=?', (old['id'],))
                db.execute('DELETE FROM investigation_case WHERE investigation_id=?', (old['id'],))
                db.execute('DELETE FROM investigation WHERE id=?', (old['id'],))
            events = [dict(x) for x in db.execute('SELECT * FROM transaction_event')]
            merchants = [r['merchant'] for r in db.execute(
                'SELECT merchant FROM verdict WHERE run_id=? ORDER BY merchant', (run_id,))]
            inv_ids = []
            for merchant in merchants:
                contract = db.execute(
                    'SELECT * FROM billing_contract WHERE merchant=?', (merchant,)).fetchone()
                if contract is None:
                    # 合同未冻结在来源中时不存在合法归因键，禁止凭金额猜测。
                    raise ValueError('contract_not_frozen')
                inv_id = db.execute(
                    'INSERT INTO investigation(run_id,merchant,source_digest,current) VALUES (?,?,?,?)',
                    (run_id, merchant, run['source_digest'], current)).lastrowid
                inv_ids.append(inv_id)
                worker = _Worker(db, inv_id, run_id, merchant, window, dict(contract), current)
                worker.run(events)
            return inv_ids, bool(current)
    finally:
        db.close()


class _Worker:
    def __init__(self, db, inv_id, run_id, merchant, window, contract, current):
        self.db = db
        self.inv_id = inv_id
        self.run_id = run_id
        self.merchant = merchant
        self.window = window
        self.contract = contract
        self.semantics = contract['reference_semantics']
        self.current = current
        self.edges_seen = set()

    def edge(self, relation, case_id=None, batch_id=None, event_id=None, rate_id=None,
             adjustment_id=None, group_id=None, note=None):
        key = (relation, case_id, batch_id, event_id, rate_id, adjustment_id, group_id, note)
        if key in self.edges_seen:
            return
        self.edges_seen.add(key)
        self.db.execute(
            'INSERT INTO investigation_edge(investigation_id,run_id,merchant,case_id,relation,'
            'batch_id,event_id,rate_id,adjustment_id,group_id,note) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (self.inv_id, self.run_id, self.merchant, case_id, relation, batch_id, event_id,
             rate_id, adjustment_id, group_id, note))

    def case(self, kind, anchor_type, anchor_id, detail, amount=None, rate_id=None):
        cid = case_id(self.run_id, self.merchant, kind, anchor_type, anchor_id)
        self.db.execute(
            'INSERT INTO investigation_case(id,investigation_id,run_id,merchant,kind,anchor_type,anchor_id,detail,amount) '
            'VALUES (?,?,?,?,?,?,?,?,?)',
            (cid, self.inv_id, self.run_id, self.merchant, kind, anchor_type, str(anchor_id), detail,
             money(amount) if amount is not None else None))
        if rate_id is not None:
            self.edge('rate_basis', case_id=cid, rate_id=rate_id)
        return cid

    def run(self, events):
        db = self.db
        frozen_events = {r['event_id'] for r in db.execute(
            'SELECT event_id FROM evidence WHERE run_id=? AND merchant=? AND event_id IS NOT NULL',
            (self.run_id, self.merchant))}
        frozen_batches = {r['batch_id'] for r in db.execute(
            'SELECT batch_id FROM evidence WHERE run_id=? AND merchant=? AND batch_id IS NOT NULL',
            (self.run_id, self.merchant))}
        chains = _chain_view(db, self.run_id, self.merchant, events)
        lines = [dict(x) for x in db.execute(
            'SELECT * FROM batch_line WHERE window_id=? AND merchant=? ORDER BY id',
            (self.window['id'], self.merchant)) if x['id'] in frozen_batches]
        self.line_by_id = {l['id']: l for l in lines}
        for line in lines:
            root = self._root_of(line)
            if root is None:
                cid = self.case(AMOUNT_UNKNOWN, 'line', line['id'],
                                '更正链无法追溯到被更正账单行，归因键断裂')
                self.edge('unresolved_line', case_id=cid, batch_id=line['id'])
        for adj in db.execute('SELECT * FROM adjustment WHERE window_id=? AND merchant=? ORDER BY id',
                              (self.window['id'], self.merchant)):
            self.edge('adjustment', adjustment_id=adj['id'],
                      note=f"{adj['reason']} {adj['amount']}")
        self.chains = chains
        self.frozen_events = frozen_events
        if self.semantics == 'tx':
            self._run_tx_semantics()
        else:
            self._run_group_semantics()

    def _root_of(self, line):
        seen = set()
        cur = line
        while cur['corrects']:
            parent = cur['corrects']
            if parent in seen or parent not in self.line_by_id:
                return None
            seen.add(parent)
            cur = self.line_by_id[parent]
        return cur

    def _effective_target(self, line):
        """账单行的有效引用：更正行沿更正链继承原行的合同引用。"""
        root = self._root_of(line)
        if root is None:
            return None, None, None
        return root, root['ref_kind'], root['ref_value']

    def _line_total(self, root_id):
        total = Decimal(0)
        for line in self.line_by_id.values():
            root = self._root_of(line)
            if root is not None and root['id'] == root_id:
                total += Decimal(line['amount'])
        return total

    def _root_lines(self):
        return [l for l in self.line_by_id.values() if not l['corrects']]

    def _status_of(self, chain):
        """返回冻结证据中该交易链的状态：adjudicated / pending / broken / other_window。"""
        if all(eid in self.frozen_events for eid in chain['event_ids']):
            return 'adjudicated'
        codes = chain['issues']
        if codes & {'late_arrival', 'window_ambiguous'}:
            return 'pending'
        if codes & {'bad_state', 'rate_invalid', 'rate_ambiguous'}:
            return 'broken'
        # 证据链本身有效但未进入本窗：业务时间落在其他窗口，按时间窗待转处理，
        # 而不是当作与本 run 无关而静默丢弃。
        if chain['valid'] and chain['expected'] is not None:
            biz = at(chain['items'][0]['business_at'])
            try:
                wid = window_for(biz, [dict(r) for r in self.db.execute('SELECT * FROM window')])
            except ValueError:
                return 'pending'
            if wid != self.window['id']:
                return 'pending'
        return 'other_window'

    def _candidate_edges(self, cid):
        # 同额或多候选时只列候选，绝不凭金额选定身份。
        for tx, chain in sorted(self.chains.items()):
            if self._status_of(chain) == 'adjudicated':
                self.edge('candidate', case_id=cid, event_id=chain['event_ids'][0],
                          note=f'tx={tx} expected={chain["expected"]}')

    def _compare_amount(self, anchor_type, anchor_id, billed, chain_or_group, rate_id, covered_edges):
        """金额比对只在引用键已锁定身份后进行，返回案件 id 或 None（已覆盖）。"""
        expected = chain_or_group['expected']
        gross = chain_or_group['gross']
        if expected is None:
            cid = self.case(AMOUNT_UNKNOWN, anchor_type, anchor_id,
                            '引用身份已锁定，但交易链或费率证据不足以计算应付金额')
            covered_edges(cid)
            return cid
        if billed == expected:
            covered_edges(None)
            return None
        residual = billed - expected
        # 客观可证的费率口径：账单按未扣费的 gross 开单，而合同费率产生非零费用。
        if chain_or_group.get('fee_total', Decimal(0)) != Decimal(0) and billed == gross:
            cid = self.case(RATE_BASIS_DIFF, anchor_type, anchor_id,
                            f'账单按扣费前金额 {money(gross)} 开单，合同费率口径净额应为 {money(expected)}',
                            amount=residual, rate_id=rate_id)
        else:
            cid = self.case(AMOUNT_UNKNOWN, anchor_type, anchor_id,
                            f'引用身份已锁定但金额不等：账单 {money(billed)}，应付 {money(expected)}，差额 {money(residual)}',
                            amount=residual, rate_id=rate_id)
        covered_edges(cid)
        return cid

    def _run_tx_semantics(self):
        refs = {}
        for line in self._root_lines():
            kind, value = line['ref_kind'], line['ref_value']
            if kind != 'tx' or not value:
                cid = self.case(AMOUNT_UNKNOWN, 'line', line['id'],
                                '交易引用合同下账单行缺少合法 tx 引用，无法归因到交易')
                self.edge('unresolved_line', case_id=cid, batch_id=line['id'])
                self._candidate_edges(cid)
                continue
            refs.setdefault(value, []).append(line)
        for line in self.line_by_id.values():
            if line['corrects']:
                root = self._root_of(line)
                self.edge('correction', batch_id=line['id'],
                          note=root['id'] if root else 'unresolved')
        for tx, chain in sorted(self.chains.items()):
            status = self._status_of(chain)
            if status == 'other_window':
                continue
            if status == 'pending':
                cid = self.case(WINDOW_PENDING, 'tx', tx,
                                '交易业务时间或到达时间超出本窗，待转入对应窗口后再归因')
                for eid in chain['event_ids']:
                    self.edge('event_pending', case_id=cid, event_id=eid)
                continue
            if status == 'broken':
                cid = self.case(AMOUNT_UNKNOWN, 'tx', tx,
                                '交易链状态或费率证据不成立，应付金额未知')
                for eid in chain['event_ids']:
                    self.edge('broken_event', case_id=cid, event_id=eid)
                continue
            matched = refs.get(tx, [])
            if not matched:
                cid = self.case(MISSING_LINE, 'tx', tx,
                                f'交易已入实时轨净额 {money(chain["expected"])}，账单无对应引用行')
                for eid in chain['event_ids']:
                    self.edge('missing_event', case_id=cid, event_id=eid, rate_id=chain['rate_id'])
                continue
            if len(matched) > 1:
                cid = self.case(DUPLICATE_LINE, 'tx', tx,
                                '同一交易引用出现多条账单根行，存在重复账单')
                for root in matched:
                    self.edge('duplicate_line', case_id=cid, batch_id=root['id'],
                              event_id=chain['event_ids'][0])
                continue
            root = matched[0]
            billed = self._line_total(root['id'])

            def covered(cid, root=root, chain=chain):
                for line in self.line_by_id.values():
                    r = self._root_of(line)
                    if r is not None and r['id'] == root['id']:
                        self.edge('line_covers_tx', case_id=cid, batch_id=line['id'],
                                  event_id=chain['event_ids'][0], rate_id=chain['rate_id'])

            self._compare_amount('tx', tx, billed,
                                 {'expected': chain['expected'], 'gross': chain['gross'],
                                  'fee_total': chain['fee'] or Decimal(0)},
                                 chain['rate_id'], covered)
        # 引用了不存在交易的账单行：不得凭金额猜测身份。
        known = set(self.chains)
        for value, roots in sorted(refs.items()):
            if value not in known:
                for root in roots:
                    cid = self.case(AMOUNT_UNKNOWN, 'line', root['id'],
                                    f'账单引用交易 {value} 在事件证据中不存在，身份无法确定')
                    self.edge('unresolved_line', case_id=cid, batch_id=root['id'])
                    self._candidate_edges(cid)

    def _run_group_semantics(self):
        groups = {}
        for tx, chain in sorted(self.chains.items()):
            status = self._status_of(chain)
            if status == 'other_window':
                continue
            if not chain['group'] or chain['group_conflict']:
                cid = self.case(AMOUNT_UNKNOWN, 'tx', tx,
                                '组引用合同要求清算组键，但事件缺少一致的 clearing_group')
                for eid in chain['event_ids']:
                    self.edge('broken_event', case_id=cid, event_id=eid)
                continue
            groups.setdefault(chain['group'], []).append((tx, chain))
        refs = {}
        for line in self._root_lines():
            kind, value = line['ref_kind'], line['ref_value']
            if kind != 'group' or not value:
                cid = self.case(AMOUNT_UNKNOWN, 'line', line['id'],
                                '清算组引用合同下账单行缺少合法 group 引用，无法归因')
                self.edge('unresolved_line', case_id=cid, batch_id=line['id'])
                continue
            refs.setdefault(value, []).append(line)
        for line in self.line_by_id.values():
            if line['corrects']:
                root = self._root_of(line)
                self.edge('correction', batch_id=line['id'],
                          note=root['id'] if root else 'unresolved')
        for gid, members in sorted(groups.items()):
            pending = [tx for tx, c in members if self._status_of(c) == 'pending']
            broken = [tx for tx, c in members if self._status_of(c) == 'broken']
            if pending:
                cid = self.case(WINDOW_PENDING, 'group', gid,
                                f'组内交易 {",".join(pending)} 跨窗或迟到，整组待转后归因')
                for tx, c in members:
                    for eid in c['event_ids']:
                        self.edge('event_pending', case_id=cid, event_id=eid, group_id=gid)
                continue
            if broken:
                cid = self.case(AMOUNT_UNKNOWN, 'group', gid,
                                f'组内交易 {",".join(broken)} 证据不成立，组金额未知')
                for tx, c in members:
                    for eid in c['event_ids']:
                        self.edge('broken_event', case_id=cid, event_id=eid, group_id=gid)
                continue
            roots = refs.get(gid, [])
            if not roots:
                cid = self.case(MISSING_LINE, 'group', gid,
                                '清算组已形成实时轨金额，账单无对应组引用行')
                for tx, c in members:
                    self.edge('missing_event', case_id=cid, event_id=c['event_ids'][0],
                              rate_id=c['rate_id'], group_id=gid)
                continue
            if len(roots) > 1:
                cid = self.case(DUPLICATE_LINE, 'group', gid,
                                '同一清算组引用出现多条账单根行，存在重复账单')
                for root in roots:
                    self.edge('duplicate_line', case_id=cid, batch_id=root['id'], group_id=gid)
                continue
            root = roots[0]
            billed = self._line_total(root['id'])
            expected = sum((c['expected'] for _, c in members), Decimal(0))
            gross = sum((c['gross'] for _, c in members), Decimal(0))
            fee_total = sum((c['fee'] or Decimal(0) for _, c in members), Decimal(0))
            rate_ids = {c['rate_id'] for _, c in members if c['rate_id']}

            def covered(cid, root=root, members=members):
                for line in self.line_by_id.values():
                    r = self._root_of(line)
                    if r is not None and r['id'] == root['id']:
                        self.edge('line_covers_group', case_id=cid, batch_id=line['id'], group_id=gid)
                for tx, c in members:
                    self.edge('tx_in_group', case_id=cid, event_id=c['event_ids'][0],
                              rate_id=c['rate_id'], group_id=gid,
                              note=f'tx={tx} expected={money(c["expected"])}')

            self._compare_amount('group', gid, billed,
                                 {'expected': expected, 'gross': gross, 'fee_total': fee_total},
                                 next(iter(rate_ids)), covered)
        for value, roots in sorted(refs.items()):
            if value not in groups:
                for root in roots:
                    cid = self.case(AMOUNT_UNKNOWN, 'line', root['id'],
                                    f'账单引用清算组 {value} 在事件证据中不存在，组身份无法确定')
                    self.edge('unresolved_line', case_id=cid, batch_id=root['id'])


def load_investigations(db, run_id, fresh):
    """供报告读取：案件与多对多证据边，附带当前/历史有效性。"""
    out = []
    for inv in db.execute('SELECT * FROM investigation WHERE run_id=? ORDER BY id', (run_id,)):
        item = dict(inv)
        item['effective'] = bool(inv['current']) and fresh
        item['cases'] = [dict(x) for x in db.execute(
            'SELECT * FROM investigation_case WHERE investigation_id=? ORDER BY id', (inv['id'],))]
        item['edges'] = [dict(x) for x in db.execute(
            'SELECT * FROM investigation_edge WHERE investigation_id=? ORDER BY id', (inv['id'],))]
        out.append(item)
    return out
