import csv, json
import pytest
from dualsettle.store import connect
from dualsettle.ingest import stream, batch, contract
from dualsettle.rules import reconcile, close, input_digest
from dualsettle.report import write
from dualsettle.investigation import investigate, case_id


def setup(tmp, window='D1'):
    db = tmp / 'x.db'
    c = connect(db)
    with c:
        c.execute("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        c.execute("INSERT INTO window VALUES ('D2','2026-01-02T00:00:00+00:00','2026-01-03T00:00:00+00:00','2026-01-03T01:00:00+00:00',1)")
        c.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES ('M','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")
    c.close()
    return db


def ev(tx, version, kind, amount='100.00', hour=None, group=None):
    e = {'merchant': 'M', 'tx': tx, 'version': version, 'kind': kind, 'amount': amount,
         'business_at': f'2026-01-01T{hour or version:02d}:00:00+00:00'}
    if group:
        e['clearing_group'] = group
    return e


def chain(tx, amount='100.00', hour=None, group=None):
    return [ev(tx, 1, 'authorize', amount, hour, group),
            ev(tx, 2, 'confirm', amount, hour, group)]


def load_events(tmp, db, events):
    p = tmp / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(x) for x in events), encoding='utf-8')
    stream(p, db)


def write_contract(tmp, db, semantics, merchant='M'):
    p = tmp / f'contract-{merchant}.json'
    p.write_text(json.dumps({'merchant': merchant, 'reference_semantics': semantics}, ensure_ascii=False),
                 encoding='utf-8')
    contract(p, db)


def load_lines(tmp, db, rows):
    p = tmp / 'batch.csv'
    with p.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'merchant', 'amount', 'corrects', 'ref_kind', 'ref_value'])
        for r in rows:
            w.writerow(r)
    batch(p, db, 'D1')


def run_investigate(tmp, db, events, rows, semantics='tx', close_run=True):
    load_events(tmp, db, events)
    write_contract(tmp, db, semantics)
    load_lines(tmp, db, rows)
    run, verdict = reconcile(db, 'D1')
    if close_run and verdict.get('M') == '可封账':
        close(db, run)
    ids, current = investigate(db, run)
    return run, ids, current


def cases(db, inv_id):
    return [dict(x) for x in connect(db).execute(
        'SELECT * FROM investigation_case WHERE investigation_id=? ORDER BY id', (inv_id,)).fetchall()]


def edges(db, inv_id):
    return [dict(x) for x in connect(db).execute(
        'SELECT * FROM investigation_edge WHERE investigation_id=? ORDER BY id', (inv_id,)).fetchall()]


def kinds(db, inv_id):
    return {c['kind'] for c in cases(db, inv_id)}


def two_tx_events(hour_base=0, group=None):
    return [ev('T1', 1, 'authorize', '60.00', 1, group), ev('T1', 2, 'confirm', '60.00', None, group),
            ev('T2', 3, 'authorize', '40.00', 3, group), ev('T2', 4, 'confirm', '40.00', None, group)]


def test_tx_contract_group_line_leaves_missing_and_overbilling(tmp_path):
    db = setup(tmp_path)
    # 交易引用合同：账单一行 100 只引用 T1，结论不能靠总金额猜出。
    run, ids, current = run_investigate(
        tmp_path, db, two_tx_events(),
        [('B1', 'M', '100.00', '', 'tx', 'T1')], semantics='tx')
    assert current is True
    cs = cases(db, ids[0])
    by_anchor = {(c['anchor_type'], c['anchor_id']): c['kind'] for c in cs}
    assert by_anchor[('tx', 'T2')] == 'missing_line'
    t1 = next(c for c in cs if c['anchor_id'] == 'T1' and c['anchor_type'] == 'tx')
    assert t1['kind'] == 'amount_unknown' and t1['amount'] == '40.60'
    es = edges(db, ids[0])
    assert {'line_covers_tx', 'missing_event'} <= {e['relation'] for e in es}
    # 原 verdict 仍是金额裁定结果，调查不触碰。
    verdict = dict(connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (run,)).fetchone())
    assert verdict['status'] == '差异' and verdict['reason'] == 'amount_mismatch'


def test_group_contract_same_line_covers_both_transactions(tmp_path):
    db = setup(tmp_path)
    run, ids, current = run_investigate(
        tmp_path, db, two_tx_events(group='G1'),
        [('B1', 'M', '99.00', '', 'group', 'G1')], semantics='group')
    cs = cases(db, ids[0])
    assert cs == []  # 组汇总后净额 99.00，一行已覆盖两笔交易
    es = edges(db, ids[0])
    rels = {e['relation'] for e in es}
    assert {'line_covers_group', 'tx_in_group'} <= rels
    covered = {e['event_id'] for e in es if e['relation'] == 'tx_in_group'}
    assert covered == {1, 3}
    verdict = dict(connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (run,)).fetchone())
    assert verdict['status'] == '可封账'


def test_attribution_key_change_flips_cases_but_verdict_inputs_keep_history(tmp_path):
    # 同一批输入先按 tx 合同调查，再换成 group 合同重跑：案件结论翻转，
    # 旧 run 的 verdict 作为历史保持不变。
    db = setup(tmp_path)
    load_events(tmp_path, db, two_tx_events(group='G1'))
    load_lines(tmp_path, db, [('B1', 'M', '99.00', '', 'group', 'G1')])
    write_contract(tmp_path, db, 'tx')
    first, _ = reconcile(db, 'D1')
    investigate(db, first)
    inv1 = connect(db).execute('SELECT id FROM investigation WHERE run_id=?', (first,)).fetchone()[0]
    before = kinds(db, inv1)
    assert 'amount_unknown' in before  # tx 合同下 group 引用键无法归因
    # 同商户合同语义变更属于来源冲突，必须显式改写来源（合同身份不可静默漂移）。
    changed = tmp_path / 'contract-v2.json'
    changed.write_text(
        json.dumps({'merchant': 'M', 'reference_semantics': 'group'}, ensure_ascii=False), encoding='utf-8')
    with pytest.raises(ValueError, match='contract_conflict'):
        contract(changed, db)
    c = connect(db)
    with c:
        c.execute("UPDATE billing_contract SET reference_semantics='group' WHERE merchant='M'")
    second, verdict = reconcile(db, 'D1')
    assert first != second
    investigate(db, second)
    inv2 = connect(db).execute('SELECT id FROM investigation WHERE run_id=? ORDER BY id DESC', (second,)).fetchone()[0]
    assert cases(db, inv2) == []
    rels = {e['relation'] for e in edges(db, inv2)}
    assert 'line_covers_group' in rels
    old_verdict = dict(connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (first,)).fetchone())
    # 金额裁定只看净额（99=99），合同语义变化不改变历史 verdict；
    # 变化的是调查案件：无法归因 -> 组内全覆盖。
    assert old_verdict['stream_net'] == old_verdict['batch_net'] == '99.00'
    new_verdict = dict(connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (second,)).fetchone())
    assert new_verdict['reason'] == old_verdict['reason']


def test_equal_amount_ambiguity_keeps_many_to_many_candidates(tmp_path):
    db = setup(tmp_path)
    events = [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm'),
              ev('T2', 3, 'authorize'), ev('T2', 4, 'confirm')]
    # 一笔多行：同一 tx 引用出现两条根行；T2 完全没有账单行。
    rows = [('B1', 'M', '99.00', '', 'tx', 'T1'), ('B2', 'M', '99.00', '', 'tx', 'T1')]
    run, ids, _ = run_investigate(tmp_path, db, events, rows, semantics='tx', close_run=False)
    cs = cases(db, ids[0])
    dup = next(c for c in cs if c['kind'] == 'duplicate_line')
    assert dup['anchor_id'] == 'T1'
    missing = next(c for c in cs if c['kind'] == 'missing_line')
    assert missing['anchor_id'] == 'T2'
    # 不存在的引用身份：同额也禁止猜成 T1/T2，候选必须多对多保留。
    extra = tmp_path / 'ghost.csv'
    with extra.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'merchant', 'amount', 'corrects', 'ref_kind', 'ref_value'])
        w.writerow(['B3', 'M', '99.00', '', 'tx', 'GHOST'])
    batch(extra, db, 'D1')
    run2, _ = reconcile(db, 'D1')
    ids2, _ = investigate(db, run2)
    cs2 = cases(db, ids2[0])
    ghost = next(c for c in cs2 if c['anchor_type'] == 'line' and c['anchor_id'] == 'B3')
    assert ghost['kind'] == 'amount_unknown' and 'GHOST' in ghost['detail']
    candidate_edges = [e for e in edges(db, ids2[0]) if e['relation'] == 'candidate']
    assert {e['event_id'] for e in candidate_edges} == {1, 3}
    notes = ' '.join(e['note'] for e in candidate_edges)
    assert 'tx=T1' in notes and 'tx=T2' in notes


def test_cross_window_transaction_is_window_pending_not_missing(tmp_path):
    db = setup(tmp_path)
    events = [ev('T1', 1, 'authorize', hour=1), ev('T1', 2, 'confirm', hour=2),
              ev('T2', 3, 'authorize', hour=5), ev('T2', 4, 'confirm', hour=6)]
    # T2 业务时间落在 D2 窗口。
    for e in events:
        if e['tx'] == 'T2':
            e['business_at'] = '2026-01-02T03:00:00+00:00'
    run, ids, _ = run_investigate(tmp_path, db, events,
                                  [('B1', 'M', '99.00', '', 'tx', 'T1')], semantics='tx')
    cs = cases(db, ids[0])
    pending = next(c for c in cs if c['anchor_id'] == 'T2')
    assert pending['kind'] == 'window_pending'
    assert all(c['kind'] != 'missing_line' for c in cs)
    assert any(e['relation'] == 'event_pending' for e in edges(db, ids[0]))


def test_batch_correction_inherits_reference_and_covers_transaction(tmp_path):
    db = setup(tmp_path)
    events = [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm')]
    run, ids, _ = run_investigate(
        tmp_path, db, events,
        [('B1', 'M', '98.00', '', 'tx', 'T1'), ('B2', 'M', '1.00', 'B1', 'tx', 'T1')],
        semantics='tx')
    assert cases(db, ids[0]) == []  # 98+1=99，更正行继承 T1 引用后完成覆盖
    es = edges(db, ids[0])
    assert any(e['relation'] == 'correction' and e['batch_id'] == 'B2' for e in es)
    assert any(e['relation'] == 'line_covers_tx' and e['batch_id'] == 'B2' for e in es)


def test_rate_basis_difference_is_classified_objectively(tmp_path):
    db = setup(tmp_path)
    events = [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm')]
    run, ids, _ = run_investigate(
        tmp_path, db, events, [('B1', 'M', '100.00', '', 'tx', 'T1')],
        semantics='tx', close_run=False)
    cs = cases(db, ids[0])
    assert len(cs) == 1 and cs[0]['kind'] == 'rate_basis_diff'
    assert cs[0]['amount'] == '1.00'
    assert any(e['relation'] == 'rate_basis' and e['rate_id'] for e in edges(db, ids[0]))


def test_input_invalidation_marks_old_investigation_historical(tmp_path):
    db = setup(tmp_path)
    events = [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm')]
    run, ids, _ = run_investigate(tmp_path, db, events,
                                  [('B1', 'M', '98.00', '', 'tx', 'T1')], semantics='tx', close_run=False)
    cid_before = [c['id'] for c in cases(db, ids[0])]
    c = connect(db)
    with c:
        c.execute("UPDATE rate_version SET rate='0.02' WHERE merchant='M'")
    data = json.loads(write(db, run, tmp_path / 'report').read_text(encoding='utf-8'))
    inv = data['investigations'][0]
    assert inv['effective'] is False
    assert inv['source_digest'] != input_digest(connect(db))
    md = (tmp_path / 'report.md').read_text(encoding='utf-8')
    banner = next(line for line in md.splitlines() if line.startswith('### '))
    assert '历史解释' in banner and banner.startswith('### 商户 M — 历史解释')
    # 旧案件仍可作为历史解释读取。
    assert [c['id'] for c in inv['cases']] == cid_before
    # 新输入下重新核对与调查，得到当前调查。
    new_run, _ = reconcile(db, 'D1')
    new_ids, current = investigate(db, new_run)
    assert current is True
    new_data = json.loads(write(db, new_run, tmp_path / 'report2').read_text(encoding='utf-8'))
    assert new_data['investigations'][0]['effective'] is True


def test_repeated_investigation_is_stable_and_idempotent(tmp_path):
    db = setup(tmp_path)
    events = two_tx_events()
    run, ids1, _ = run_investigate(tmp_path, db, events,
                                   [('B1', 'M', '100.00', '', 'tx', 'T1')], semantics='tx', close_run=False)
    first_cases = sorted((c['id'], c['kind']) for c in cases(db, ids1[0]))
    expected = case_id(run, 'M', 'missing_line', 'tx', 'T2')
    assert any(cid == expected for cid, _ in first_cases)
    ids2, current = investigate(db, run)
    assert current is True
    second_cases = sorted((c['id'], c['kind']) for c in cases(db, ids2[0]))
    assert first_cases == second_cases
    con = connect(db)
    assert con.execute('SELECT COUNT(*) FROM investigation WHERE run_id=?', (run,)).fetchone()[0] == 1
    assert con.execute('SELECT COUNT(*) FROM investigation_case').fetchone()[0] == len(first_cases)


def test_investigation_never_mutates_frozen_inputs_or_verdict(tmp_path):
    db = setup(tmp_path)
    events = two_tx_events()
    run, ids, _ = run_investigate(tmp_path, db, events,
                                  [('B1', 'M', '100.00', '', 'tx', 'T1')], semantics='tx', close_run=False)
    con = connect(db)
    before_events = [tuple(r) for r in con.execute('SELECT * FROM transaction_event ORDER BY id')]
    before_lines = [tuple(r) for r in con.execute('SELECT * FROM batch_line ORDER BY id')]
    before_verdict = [tuple(r) for r in con.execute('SELECT * FROM verdict ORDER BY merchant')]
    investigate(db, run)
    assert [tuple(r) for r in con.execute('SELECT * FROM transaction_event ORDER BY id')] == before_events
    assert [tuple(r) for r in con.execute('SELECT * FROM batch_line ORDER BY id')] == before_lines
    assert [tuple(r) for r in con.execute('SELECT * FROM verdict ORDER BY merchant')] == before_verdict
    # 差异案件不会把商户变成可封账。
    v = dict(con.execute('SELECT * FROM verdict WHERE run_id=? AND merchant=?', (run, 'M')).fetchone())
    assert v['status'] == '差异'


def test_missing_contract_blocks_guessing(tmp_path):
    db = setup(tmp_path)
    load_events(tmp_path, db, [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm')])
    load_lines(tmp_path, db, [('B1', 'M', '99.00', '', '', '')])
    run, _ = reconcile(db, 'D1')
    with pytest.raises(ValueError, match='contract_not_frozen'):
        investigate(db, run)


def test_group_contract_without_group_key_is_amount_unknown(tmp_path):
    db = setup(tmp_path)
    run, ids, _ = run_investigate(
        tmp_path, db, two_tx_events(),  # 事件未带 clearing_group
        [('B1', 'M', '99.00', '', 'group', 'G1')], semantics='group')
    cs = cases(db, ids[0])
    tx_anchors = {c['anchor_id'] for c in cs if c['anchor_type'] == 'tx'}
    assert tx_anchors == {'T1', 'T2'}
    assert all(c['kind'] == 'amount_unknown' for c in cs)
    assert any(e['relation'] == 'broken_event' for e in edges(db, ids[0]))
    # 引用了不存在组的账单行同样无法归因。
    assert any(c['anchor_type'] == 'line' and c['anchor_id'] == 'B1' for c in cs)


def test_group_duplicate_lines_and_missing_group(tmp_path):
    db = setup(tmp_path)
    run, ids, _ = run_investigate(
        tmp_path, db, two_tx_events(group='G1'),
        [('B1', 'M', '50.00', '', 'group', 'G1'), ('B2', 'M', '49.00', '', 'group', 'G1')],
        semantics='group')
    cs = cases(db, ids[0])
    assert len(cs) == 1 and cs[0]['kind'] == 'duplicate_line'
    assert cs[0]['anchor_type'] == 'group' and cs[0]['anchor_id'] == 'G1'
    # 缺组行场景
    db2 = tmp_path / 'x2.db'
    c = connect(db2)
    with c:
        c.execute("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        c.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES ('M','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")
    c.close()
    run2, ids2, _ = run_investigate(
        tmp_path, db2, two_tx_events(group='G9'),
        [('B1', 'M', '1.00', '', 'group', 'OTHER')], semantics='group', close_run=False)
    cs2 = cases(db2, ids2[0])
    anchors = {(c['anchor_type'], c['anchor_id']): c['kind'] for c in cs2}
    assert anchors[('group', 'G9')] == 'missing_line'
    assert anchors[('line', 'B1')] == 'amount_unknown'


def test_contract_ingest_conflict_and_idempotency(tmp_path):
    db = setup(tmp_path)
    write_contract(tmp_path, db, 'tx')
    assert contract(tmp_path / 'contract-M.json', db) == 0
    other = tmp_path / 'other.json'
    other.write_text(json.dumps({'merchant': 'M', 'reference_semantics': 'group'}), encoding='utf-8')
    with pytest.raises(ValueError, match='contract_conflict'):
        contract(other, db)
    with pytest.raises(ValueError, match='contract_semantics'):
        bad = tmp_path / 'bad.json'
        bad.write_text(json.dumps({'merchant': 'M', 'reference_semantics': 'guess'}), encoding='utf-8')
        contract(bad, db)


def test_batch_with_unknown_ref_kind_is_issue_not_silent(tmp_path):
    db = setup(tmp_path)
    load_events(tmp_path, db, [ev('T1', 1, 'authorize'), ev('T1', 2, 'confirm')])
    p = tmp_path / 'b.csv'
    with p.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'merchant', 'amount', 'corrects', 'ref_kind', 'ref_value'])
        w.writerow(['B1', 'M', '99.00', '', 'wild', 'T1'])
    assert batch(p, db, 'D1') == 0
    assert connect(db).execute("SELECT COUNT(*) FROM issue WHERE code='batch_ref_kind'").fetchone()[0] == 1
