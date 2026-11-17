import json
from pathlib import Path
from .store import connect
from .rules import input_digest
from .investigation import load_investigations
def write(path,run,out):
    db=connect(path)
    record=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run,)).fetchone()
    if record is None:raise ValueError('unknown_run')
    run_record=dict(record)
    fresh=run_record['source_digest']==input_digest(db)
    run_record['effective_status']=run_record['status'] if fresh else '已失效'
    data={'run':run_record,
      'verdicts':[dict(x) for x in db.execute('SELECT * FROM verdict WHERE run_id=?',(run,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=?',(run,))],
      'issues':[dict(x) for x in db.execute('SELECT * FROM reconcile_issue WHERE run_id=?',(run,))],
      'investigations':load_investigations(db,run,fresh),
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file')]}
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 双轨结算结论','',f"运行当前状态：{run_record['effective_status']}",'']
    lines += [f"- {x['merchant']}: {x['status'] if fresh else '证据已失效'}，实时 {x['stream_net']}，批次 {x['batch_net']}" for x in data['verdicts']]
    lines += ['', '## 交易级差异调查']
    if not data['investigations']:
        lines += ['- 本运行尚无调查结论（调查不改变金额裁定与封账状态）。']
    for inv in data['investigations']:
        banner='当前调查' if inv['effective'] else '历史解释（输入已变化，不能称为当前调查）'
        lines += [f"### 商户 {inv['merchant']} — {banner}",
                  f"- 绑定 source_digest: {inv['source_digest']}"]
        if not inv['cases']:
            lines += ['- 账单行按合同引用键全部覆盖，未形成差异案件。']
        for case in inv['cases']:
            lines += [f"- 案件 {case['id']} [{case['kind']}] 锚点 {case['anchor_type']}:{case['anchor_id']}：{case['detail']}"]
        for edge in inv['edges']:
            bits=[f"relation={edge['relation']}"]
            if edge['batch_id']:bits.append(f"line={edge['batch_id']}")
            if edge['event_id']:bits.append(f"event={edge['event_id']}")
            if edge['group_id']:bits.append(f"group={edge['group_id']}")
            if edge['note']:bits.append(f"note={edge['note']}")
            lines += ['  - 证据边 '+'，'.join(bits)]
    out.with_suffix('.md').write_text('\n'.join(lines),encoding='utf-8')
    db.close();return out.with_suffix('.json')
