import json
from pathlib import Path
from .store import connect
from .rules import input_digest
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
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file')]}
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    out.with_suffix('.md').write_text('# 双轨结算结论\n\n'+f"运行当前状态：{run_record['effective_status']}\n\n"+'\n'.join(f"- {x['merchant']}: {x['status'] if fresh else '证据已失效'}，实时 {x['stream_net']}，批次 {x['batch_net']}" for x in data['verdicts']),encoding='utf-8')
    db.close();return out.with_suffix('.json')
