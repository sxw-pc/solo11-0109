import json
from pathlib import Path
from .store import connect
from .rules import input_digest
from .review import authorization_for_run
def write(path,run,out):
    db=connect(path)
    try:
        record=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run,)).fetchone()
        if record is None:raise ValueError('unknown_run')
        run_record=dict(record)
        fresh=run_record['source_digest']==input_digest(db)
        run_record['effective_status']=run_record['status'] if fresh else '已失效'
        authorization=authorization_for_run(db,run)
        data={'run':run_record,
          'verdicts':[dict(x) for x in db.execute('SELECT * FROM verdict WHERE run_id=?',(run,))],
          'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=?',(run,))],
          'issues':[dict(x) for x in db.execute('SELECT * FROM reconcile_issue WHERE run_id=?',(run,))],
          'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file')],
          'authorization':authorization}
        out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
        out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
        lines=['# 双轨结算结论','',f"运行当前状态：{run_record['effective_status']}",'']
        lines.extend(f"- {x['merchant']}: {x['status'] if fresh else '证据已失效'}，实时 {x['stream_net']}，批次 {x['batch_net']}"
                     for x in data['verdicts'])
        lines.extend(['','## 封账授权说明',''])
        lines.append(f"- 授权路线：{authorization.get('route_label') or '未冻结审核合同'}")
        lines.append(f"- 提交核对：{authorization.get('submitter') or '未记录'}")
        if authorization.get('contract_digest'):
            lines.append(f"- 冻结合同摘要：{authorization['contract_digest']}")
        lines.append(f"- 当前授权结论：{'可归档' if authorization['authorized'] else '不可归档'}"
                     f"（{authorization['reason']}）")
        requirements=authorization.get('requirements') or {}
        if authorization.get('route')=='standard':
            lines.append(f"- 路线要求：核对提交人之外至少两名不同角色同意；"
                         f"已满足角色 {', '.join(requirements.get('present_roles') or []) or '无'}")
        elif authorization.get('route')=='risk':
            lines.append(f"- 路线要求：财务与风险审核人同意，有效异议先经独立仲裁；"
                         f"已满足角色 {', '.join(requirements.get('present_roles') or []) or '无'}")
        if authorization.get('blocking_reasons'):
            lines.append(f"- 阻断原因：{'；'.join(authorization['blocking_reasons'])}")
        if not fresh:
            lines.append('- 金额证据已改变：既有签署不能迁移到新证据，以下签署显示为失效并保留历史原文。')
        lines.extend(['','### 审核签署台账',''])
        if authorization.get('signatures'):
            for s in authorization['signatures']:
                lines.append(f"- #{s['signature_id']} {s['reviewer_name']}（{s['role_label']}）"
                             f"{s['decision_label']}：{s['opinion']} — {s['status_label']}，"
                             f"签署于 {s['signed_at']}，证据 {s['evidence_digest']}")
        else:
            lines.append('- 尚无签署。')
        if authorization.get('objections'):
            lines.extend(['','### 异议与撤销历史',''])
            for o in authorization['objections']:
                lines.append(f"- 异议 #{o['signature_id']} {o['reviewer_name']}：{o['opinion']} — {o['resolution_label']}")
                if o.get('withdrawal'):
                    w=o['withdrawal']
                    lines.append(f"  - 撤销签署 #{w['signature_id']} {w['reviewer_name']}：{w['opinion']} — {w['signed_at']}")
                if o.get('ruling'):
                    r=o['ruling']
                    lines.append(f"  - 仲裁签署 #{r['signature_id']} {r['reviewer_name']}：{r['decision_label']} — {r['opinion']}")
        out.with_suffix('.md').write_text('\n'.join(lines),encoding='utf-8')
        return out.with_suffix('.json')
    finally:db.close()
