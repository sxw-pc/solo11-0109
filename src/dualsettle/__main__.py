import argparse,csv,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import stream,batch,stream_resumable
from .rules import reconcile,close
from .report import write
from .payout import open_reserve,release_reserve,set_agreements,plan_payout,approve_payout,payout_status
def demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-'));root.mkdir(parents=True,exist_ok=True)
    db=root/'data.db';con=connect(db)
    with con:
        con.execute("INSERT OR IGNORE INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        con.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES ('M1','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")
    con.close()
    events=root/'events.jsonl';events.write_text('\n'.join(json.dumps({'merchant':'M1','tx':'T1','version':n,'kind':kind,'amount':'100.00','business_at':f'2026-01-01T0{n}:00:00+00:00'}) for n,kind in [(1,'authorize'),(2,'confirm')]),encoding='utf-8')
    lines=root/'batch.csv'
    with lines.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B1','M1','99.00',''])
    stream(events,db);batch(lines,db,'D1');run,_=reconcile(db,'D1');close(db,run)
    return write(db,run,root/'report')
def decision_demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-decision-'));root.mkdir(parents=True,exist_ok=True)
    demo(root);db=root/'data.db';con=connect(db);run=con.execute('SELECT MAX(id) FROM reconcile_run').fetchone()[0]
    open_reserve(db,'dispute-1','M1','D1','10.00','synthetic dispute')
    set_agreements(db,[
        {'id':'owner-1','merchant':'M1','beneficiary':'owner','share':'0.60','start_at':'2025-01-01T00:00:00+00:00','end_at':'2027-01-01T00:00:00+00:00','version':1},
        {'id':'operator-1','merchant':'M1','beneficiary':'operator','share':'0.40','start_at':'2025-01-01T00:00:00+00:00','end_at':'2027-01-01T00:00:00+00:00','version':1}])
    plan,status,_=plan_payout(db,run,'M1')
    if status=='ready':approve_payout(db,plan)
    return {'payout_plan':plan,'payout_status':payout_status(db,plan),'reserve_case':'dispute-1','db':str(db)}
def _resumable_seed(root):
    db=root/'resumable.db';con=connect(db)
    with con:
        con.execute("INSERT OR IGNORE INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        con.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES ('M1','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")
    con.close()
    events=root/'large_events.jsonl'
    rows=[]
    for i,(tx,amount) in enumerate([('A','100.00'),('B','20.00')],1):
        rows.append(json.dumps({'merchant':'M1','tx':tx,'version':1,'kind':'authorize','amount':amount,
                                'business_at':f'2026-01-01T0{i}:00:00+00:00'}))
        rows.append(json.dumps({'merchant':'M1','tx':tx,'version':2,'kind':'confirm','amount':amount,
                                'business_at':f'2026-01-01T0{i}:30:00+00:00'}))
    events.write_text('\n'.join(rows)+'\n',encoding='utf-8')
    # Budget aligned to a complete transaction pair so the injected crash
    # leaves exactly one verified segment (A=100) and one missing segment (B=20).
    pair_budget=len((rows[0]+'\n'+rows[1]+'\n').encode('utf-8'))
    lines=root/'large_batch.csv'
    with lines.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects'])
        w.writerow(['BA','M1','99.00','']);w.writerow(['BB','M1','19.80',''])
    batch(lines,db,'D1')
    return db,events,pair_budget
def demo_resumable(workdir=None,accept='segment',budget=None):
    """Offline walkthrough of resumable chunked ingest, crash and recovery."""
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-resumable-'));root.mkdir(parents=True,exist_ok=True)
    db,events,pair_budget=_resumable_seed(root)
    if budget is None:budget=pair_budget
    crashed=[]
    def crash_after_first(run_id,seq):
        if seq==0 and not crashed:
            crashed.append(run_id);raise RuntimeError('injected_crash_after_segment_0')
    try:
        stream_resumable(events,db,budget=budget,accept=accept,hooks={'after_commit':crash_after_first})
    except RuntimeError as exc:
        pass
    preview_run=None
    if accept=='segment':
        preview_run,_=reconcile(db,'D1',preview=True)
        try:close(db,preview_run)
        except ValueError:pass
    else:
        formal_before,_=reconcile(db,'D1')
        try:close(db,formal_before)
        except ValueError:pass
    run_id,count=stream_resumable(events,db,budget=budget,accept=accept)
    final_run,verdicts=reconcile(db,'D1');close(db,final_run)
    return {'db':str(db),'accept':accept,'import_run':run_id,'events':count,
            'preview_run':preview_run,'final_run':final_run,'verdicts':verdicts,
            'report':str(write(db,final_run,root/'resumable_report'))}
def main():
    p=argparse.ArgumentParser(description='离线实时轨与日终轨封账核对');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('demo-decisions');dd.add_argument('--workdir')
    dr=s.add_parser('demo-resumable');dr.add_argument('--workdir');dr.add_argument('--accept',choices=('whole','segment'),default='segment');dr.add_argument('--budget',type=int,default=None)
    i=s.add_parser('ingest-stream');i.add_argument('file');i.add_argument('--db',required=True)
    ir=s.add_parser('ingest-resumable');ir.add_argument('file');ir.add_argument('--db',required=True)
    ir.add_argument('--budget',type=int,default=1<<20);ir.add_argument('--accept',choices=('whole','segment'),required=True)
    b=s.add_parser('ingest-batch');b.add_argument('file');b.add_argument('--db',required=True);b.add_argument('--window',required=True)
    r=s.add_parser('reconcile');r.add_argument('--db',required=True);r.add_argument('--window',required=True);r.add_argument('--preview',action='store_true')
    c=s.add_parser('close');c.add_argument('--db',required=True);c.add_argument('--run',type=int,required=True)
    q=s.add_parser('report');q.add_argument('--db',required=True);q.add_argument('--run',type=int,required=True);q.add_argument('--out',required=True)
    h=s.add_parser('reserve-open');h.add_argument('--db',required=True);h.add_argument('--case',required=True);h.add_argument('--merchant',required=True);h.add_argument('--window',required=True);h.add_argument('--amount',required=True);h.add_argument('--reason',required=True)
    rr=s.add_parser('reserve-release');rr.add_argument('--db',required=True);rr.add_argument('--case',required=True);rr.add_argument('--sequence',type=int,required=True);rr.add_argument('--amount',required=True)
    ag=s.add_parser('agreements');ag.add_argument('file');ag.add_argument('--db',required=True)
    pp=s.add_parser('payout-plan');pp.add_argument('--db',required=True);pp.add_argument('--run',type=int,required=True);pp.add_argument('--merchant',required=True)
    ap=s.add_parser('payout-approve');ap.add_argument('--db',required=True);ap.add_argument('--plan',type=int,required=True)
    ps=s.add_parser('payout-status');ps.add_argument('--db',required=True);ps.add_argument('--plan',type=int,required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='demo-decisions':print(decision_demo(a.workdir))
    elif a.cmd=='demo-resumable':print(demo_resumable(a.workdir,a.accept,a.budget))
    elif a.cmd=='ingest-stream':print(stream(a.file,a.db))
    elif a.cmd=='ingest-resumable':print(stream_resumable(a.file,a.db,budget=a.budget,accept=a.accept))
    elif a.cmd=='ingest-batch':print(batch(a.file,a.db,a.window))
    elif a.cmd=='reconcile':print(reconcile(a.db,a.window,preview=a.preview))
    elif a.cmd=='close':close(a.db,a.run)
    elif a.cmd=='reserve-open':open_reserve(a.db,a.case,a.merchant,a.window,a.amount,a.reason)
    elif a.cmd=='reserve-release':release_reserve(a.db,a.case,a.sequence,a.amount)
    elif a.cmd=='agreements':set_agreements(a.db,json.loads(Path(a.file).read_text(encoding='utf-8')))
    elif a.cmd=='payout-plan':print(plan_payout(a.db,a.run,a.merchant))
    elif a.cmd=='payout-approve':approve_payout(a.db,a.plan)
    elif a.cmd=='payout-status':print(payout_status(a.db,a.plan))
    else:print(write(a.db,a.run,a.out))
if __name__=='__main__':main()
