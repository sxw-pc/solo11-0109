import json
from pathlib import Path

from .store import connect
from .rules import input_digest


def _fx_sources(db, run_id):
    """Read frozen FX identities from the run's own evidence rows.

    Joining ``fx_rate_version`` live would let an in-place restatement rewrite
    what an older run appears to have used; the evidence snapshot is
    authoritative here so old and new sources can be shown side by side.
    """
    seen = set()
    out = []
    for row in db.execute('''SELECT fx_rate_id,fx_role,fx_detail FROM evidence
                             WHERE run_id=? AND fx_role IS NOT NULL AND fx_detail IS NOT NULL
                             ORDER BY fx_role,fx_rate_id''', (run_id,)):
        detail = json.loads(row['fx_detail'])
        if detail.get('fx_rate_id') is None:
            continue
        key = (row['fx_role'], detail.get('fx_rate_id'), detail.get('fx_digest'))
        if key in seen:
            continue
        seen.add(key)
        out.append({
            'fx_role': row['fx_role'],
            'fx_rate_id': detail.get('fx_rate_id'),
            'base': detail.get('base'),
            'quote': detail.get('quote'),
            'start_at': detail.get('fx_start_at'),
            'end_at': detail.get('fx_end_at'),
            'quote_rate': detail.get('quote_rate'),
            'digest': detail.get('fx_digest'),
            'source': detail.get('fx_source'),
        })
    return out


def _successor_fx_sources(db, run_id, window_id):
    successor = db.execute('SELECT id FROM reconcile_run WHERE window_id=? AND id>? ORDER BY id DESC LIMIT 1',
                           (window_id, run_id)).fetchone()
    if not successor:
        return None, []
    return successor['id'], _fx_sources(db, successor['id'])


def _transactions(db, run_id):
    """Frozen per-run transaction evidence (never the latest aggregate).

    A newer FX version or contract therefore changes the new run's sources
    without retroactively rewriting what an older report recorded.
    """
    out = []
    for row in db.execute('SELECT * FROM run_transaction WHERE run_id=? ORDER BY merchant,tx', (run_id,)):
        tx = json.loads(row['detail'])
        tx['window_id'] = row['window_id']
        tx['merchant'] = row['merchant']
        out.append(tx)
    return out


def write(path, run, out):
    db = connect(path)
    record = db.execute('SELECT * FROM reconcile_run WHERE id=?', (run,)).fetchone()
    if record is None:
        raise ValueError('unknown_run')
    run_record = dict(record)
    fresh = run_record['source_digest'] == input_digest(db)
    run_record['effective_status'] = run_record['status'] if fresh else '已失效'
    successor = db.execute('SELECT id,status,started_at FROM reconcile_run WHERE window_id=? AND id>? ORDER BY id LIMIT 1',
                           (run_record['window_id'], run)).fetchone()
    # A successor only supersedes this run when the inputs actually changed;
    # a repeat run over identical inputs leaves this one current.
    run_record['superseded_by'] = successor['id'] if successor and not fresh else None
    _, successor_fx = _successor_fx_sources(db, run, run_record['window_id'])
    data = {
        'run': run_record,
        'verdicts': [dict(x) for x in db.execute('SELECT * FROM verdict WHERE run_id=?', (run,))],
        'aggregates': [dict(x) for x in db.execute('SELECT * FROM run_aggregate WHERE run_id=? ORDER BY merchant',
                                                   (run,))],
        'transactions': _transactions(db, run),
        'fx_sources': _fx_sources(db, run),
        'successor_fx_sources': successor_fx,
        'evidence': [dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=?', (run,))],
        'issues': [dict(x) for x in db.execute('SELECT * FROM reconcile_issue WHERE run_id=?', (run,))],
        'sources': [dict(x) for x in db.execute('SELECT id,path,sha FROM source_file')],
    }
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix('.json').write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = ['# 双轨结算结论', '', f"运行当前状态：{run_record['effective_status']}"]
    if run_record['superseded_by']:
        lines.append(f"已被运行 {run_record['superseded_by']} 取代；本报告保留当时的金额与来源证据。")
    lines += ['', '## 商户结论', '']
    lines += [f"- {x['merchant']}: {x['status'] if fresh else '证据已失效'}，实时 {x['stream_net']}，批次 {x['batch_net']}"
              for x in data['verdicts']]
    lines += ['', '## 交易级汇率来源', '']
    if not data['transactions']:
        lines.append('- 无带币种折算证据')
    for tx in data['transactions']:
        confirm = tx['confirm']
        if confirm is None:
            lines.append(f"- {tx['merchant']}/{tx['tx']} 路线 {tx['route']}：无确认，不计净额")
            continue
        lines.append(f"- {tx['merchant']}/{tx['tx']} 路线 {tx['route']}：确认 {confirm['original_amount']} "
                     f"{tx['tx_currency'] or '(无币种1:1)'} -> {confirm['converted']} {tx['settlement_currency'] or ''}，"
                     f"报价来源 {confirm['fx_source'] or '同币种/旧兼容'} (rate_id={confirm['fx_rate_id']})，"
                     f"舍入残差 {confirm['rounding_residual']}")
        for refund in tx['refunds']:
            lines.append(f"  - 退款 {refund['original_amount']} {tx['tx_currency'] or '(无币种1:1)'} "
                         f"按 {refund['role']} 折算 {refund['converted']} {tx['settlement_currency'] or ''}，"
                         f"报价来源 {refund['fx_source'] or '同币种/旧兼容'} (rate_id={refund['fx_rate_id']})，"
                         f"舍入残差 {refund['rounding_residual']}")
    lines += ['']
    if data['fx_sources']:
        lines += ['## 本次冻结的汇率版本', '']
        for fx in data['fx_sources']:
            lines.append(f"- {fx['fx_role']}: {fx['base']}/{fx['quote']} {fx['quote_rate']} "
                         f"[{fx['start_at']}, {fx['end_at']}) id={fx['fx_rate_id']} digest={fx['digest']} "
                         f"source={fx['source']}")
        if data['successor_fx_sources']:
            lines += ['', '## 新运行使用的汇率来源（仅供对照，不改变本报告）', '']
            for fx in data['successor_fx_sources']:
                lines.append(f"- {fx['fx_role']}: {fx['base']}/{fx['quote']} {fx['quote_rate']} "
                             f"id={fx['fx_rate_id']} digest={fx['digest']} source={fx['source']}")
    out.with_suffix('.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    db.close()
    return out.with_suffix('.json')
