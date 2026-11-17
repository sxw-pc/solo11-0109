import json
from pathlib import Path
from .store import connect
from .rules import input_digest
from .ingest import import_progress
def write(path,run,out):
    db=connect(path)
    record=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run,)).fetchone()
    if record is None:raise ValueError('unknown_run')
    run_record=dict(record)
    fresh=run_record['source_digest']==input_digest(db)
    run_record['effective_status']=run_record['status'] if fresh else '已失效'
    is_preview=bool(run_record.get('preview'))
    progress=import_progress(db)
    imports=[]
    for item in progress:
        r=item['run']
        imports.append({
          'import_run':r['id'],'source_id':r['source_id'],'path':r['path'],
          'accept_policy':r['accept_policy'],'status':r['status'],
          'total_bytes':r['total_bytes'],'next_line_no':r['next_line_no'],
          'whole_sha':r['whole_sha'],'error_code':r['error_code'],'error_detail':r['error_detail'],
          'verified_ranges':item['verified_ranges'],'missing_ranges':item['missing_ranges'],
          'segment_count':len(item['segments']),
          'segments':item['segments'],
          'problem_lines':[{'line_no':x['line_no'],'code':x['code'],'merchant':x['merchant'],
                            'tx':x['tx'],'segment_id':x['segment_id'],'detail':x['detail']} for x in item['issues']],
        })
    data={'run':run_record,
      'report_kind':'preview' if is_preview else 'formal',
      'sealable':bool(run_record.get('sealable')) and fresh and not is_preview,
      'imports':imports,
      'verdicts':[dict(x) for x in db.execute('SELECT * FROM verdict WHERE run_id=?',(run,))],
      'evidence':[dict(x) for x in db.execute('SELECT * FROM evidence WHERE run_id=?',(run,))],
      'issues':[dict(x) for x in db.execute('SELECT * FROM reconcile_issue WHERE run_id=?',(run,))],
      'sources':[dict(x) for x in db.execute('SELECT id,path,sha FROM source_file')]}
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    kind_label='预览（不可封账）' if is_preview else '正式'
    lines=['# 双轨结算结论','',f"报告性质：{kind_label}",f"运行当前状态：{run_record['effective_status']}",'','## 来源导入进度']
    if imports:
        for imp in imports:
            lines.append(f"- 导入运行 {imp['import_run']}：{imp['path']}（验收={imp['accept_policy']}，状态={imp['status']}，"
                         f"共 {imp['total_bytes']} 字节，下一行号 {imp['next_line_no']}）")
            lines.append(f"  - 已验证字节范围：{imp['verified_ranges'] or '无'}")
            lines.append(f"  - 缺段字节范围：{imp['missing_ranges'] or '无'}")
            if imp['whole_sha']:lines.append(f"  - 全文件 SHA：{imp['whole_sha']}")
            if imp['error_code']:lines.append(f"  - 隔离错误：{imp['error_code']} {imp['error_detail'] or ''}")
            for prob in imp['problem_lines']:
                lines.append(f"  - 问题行 {prob['line_no']}：{prob['code']} {prob['detail'] or ''}")
    else:lines.append('- （无分段导入来源，均为小文件流式导入）')
    lines+=['','## 商户结论']
    lines+= (f"- {x['merchant']}: {x['status'] if fresh else '证据已失效'}，实时 {x['stream_net']}，批次 {x['batch_net']}" for x in data['verdicts'])
    out.with_suffix('.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    out.with_suffix('.md').write_text('\n'.join(lines),encoding='utf-8')
    db.close();return out.with_suffix('.json')
