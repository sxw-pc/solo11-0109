import argparse,csv,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import stream,batch
from .rules import reconcile,close
from .report import write
from .review import register_reviewer,list_reviewers,submit_reconciliation,freeze_contract,sign_review,review_status
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
    stream(events,db);batch(lines,db,'D1');run,_=reconcile(db,'D1')
    # Fictional reviewers and local key digests only; no identity service.
    register_reviewer(db,'sub-1','核对员甲',['finance'])
    register_reviewer(db,'fin-1','财务审核乙',['finance'])
    register_reviewer(db,'ops-1','运营审核丙',['operations'])
    submit_reconciliation(db,run,'sub-1')
    freeze_contract(db,run,'standard','sub-1','常规路线：核对提交人之外两名不同角色签署同意后封账。')
    sign_review(db,run,'fin-1','finance','agree','金额与冻结证据一致',None)
    sign_review(db,run,'ops-1','operations','agree','批次与实时轨一致',None)
    close(db,run)
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
def review_demo(workdir=None):
    """Show both compliant routes and an objection that splits them."""
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-review-'));root.mkdir(parents=True,exist_ok=True)
    demo(root);db=root/'data.db'
    for reviewer,name,roles in [('sub-1','核对员甲',['finance']),('fin-1','财务审核乙',['finance']),
                                ('ops-1','运营审核丙',['operations']),('risk-1','风险审核丁',['risk']),
                                ('risk-2','风险审核戊',['risk']),('arb-1','独立仲裁己',['arbitrator'])]:
        register_reviewer(db,reviewer,name,roles)
    con=connect(db)
    try:
        # A fresh judged run over unchanged evidence, leaving the demo archive intact.
        run,_=reconcile(db,'D1')
        submit_reconciliation(db,run,'sub-1')
        freeze_contract(db,run,'risk','sub-1','风险复核：风险、财务审核人同意；有效异议须先由独立仲裁人裁定。')
        sign_review(db,run,'fin-1','finance','agree','金额一致',None)
        sign_review(db,run,'ops-1','operations','agree','运营复核一致',None)
        objection=sign_review(db,run,'risk-2','risk','object','风险保留意见',None)
        sign_review(db,run,'risk-1','risk','agree','风险审核同意',None)
        sign_review(db,run,'arb-1','arbitrator','dismiss','仲裁驳回异议',objection)
        before=review_status(db,run)
        close(db,run)
        archived=review_status(db,run)
        return {'route':before['route'],'authorized_before_close':before['authorized'],
                'reason':before['reason'],'signatures':len(before['signatures']),
                'archived_status':archived['run_status'],'db':str(db)}
    finally:con.close()
def main():
    p=argparse.ArgumentParser(description='离线实时轨与日终轨封账核对');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('demo-decisions');dd.add_argument('--workdir')
    rd=s.add_parser('demo-review');rd.add_argument('--workdir')
    i=s.add_parser('ingest-stream');i.add_argument('file');i.add_argument('--db',required=True)
    b=s.add_parser('ingest-batch');b.add_argument('file');b.add_argument('--db',required=True);b.add_argument('--window',required=True)
    r=s.add_parser('reconcile');r.add_argument('--db',required=True);r.add_argument('--window',required=True)
    c=s.add_parser('close');c.add_argument('--db',required=True);c.add_argument('--run',type=int,required=True)
    q=s.add_parser('report');q.add_argument('--db',required=True);q.add_argument('--run',type=int,required=True);q.add_argument('--out',required=True)
    h=s.add_parser('reserve-open');h.add_argument('--db',required=True);h.add_argument('--case',required=True);h.add_argument('--merchant',required=True);h.add_argument('--window',required=True);h.add_argument('--amount',required=True);h.add_argument('--reason',required=True)
    rr=s.add_parser('reserve-release');rr.add_argument('--db',required=True);rr.add_argument('--case',required=True);rr.add_argument('--sequence',type=int,required=True);rr.add_argument('--amount',required=True)
    ag=s.add_parser('agreements');ag.add_argument('file');ag.add_argument('--db',required=True)
    pp=s.add_parser('payout-plan');pp.add_argument('--db',required=True);pp.add_argument('--run',type=int,required=True);pp.add_argument('--merchant',required=True)
    ap=s.add_parser('payout-approve');ap.add_argument('--db',required=True);ap.add_argument('--plan',type=int,required=True)
    ps=s.add_parser('payout-status');ps.add_argument('--db',required=True);ps.add_argument('--plan',type=int,required=True)
    rv=s.add_parser('review-register');rv.add_argument('--db',required=True);rv.add_argument('--reviewer',required=True);rv.add_argument('--name',required=True);rv.add_argument('--roles',required=True)
    rl=s.add_parser('review-list');rl.add_argument('--db',required=True)
    rs=s.add_parser('review-submit');rs.add_argument('--db',required=True);rs.add_argument('--run',type=int,required=True);rs.add_argument('--reviewer',required=True)
    rc=s.add_parser('review-contract');rc.add_argument('--db',required=True);rc.add_argument('--run',type=int,required=True);rc.add_argument('--route',required=True);rc.add_argument('--submitter',required=True);rc.add_argument('--terms',required=True)
    rg=s.add_parser('review-sign');rg.add_argument('--db',required=True);rg.add_argument('--run',type=int,required=True);rg.add_argument('--reviewer',required=True);rg.add_argument('--role',required=True);rg.add_argument('--decision',required=True);rg.add_argument('--opinion',required=True);rg.add_argument('--ref',type=int);rg.add_argument('--source-digest')
    rt=s.add_parser('review-status');rt.add_argument('--db',required=True);rt.add_argument('--run',type=int,required=True)
    rw=s.add_parser('review-withdraw');rw.add_argument('--db',required=True);rw.add_argument('--run',type=int,required=True);rw.add_argument('--reviewer',required=True);rw.add_argument('--role');rw.add_argument('--opinion',required=True);rw.add_argument('--ref',type=int,required=True);rw.add_argument('--source-digest')
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='demo-decisions':print(decision_demo(a.workdir))
    elif a.cmd=='demo-review':print(review_demo(a.workdir))
    elif a.cmd=='ingest-stream':print(stream(a.file,a.db))
    elif a.cmd=='ingest-batch':print(batch(a.file,a.db,a.window))
    elif a.cmd=='reconcile':print(reconcile(a.db,a.window))
    elif a.cmd=='close':close(a.db,a.run)
    elif a.cmd=='reserve-open':open_reserve(a.db,a.case,a.merchant,a.window,a.amount,a.reason)
    elif a.cmd=='reserve-release':release_reserve(a.db,a.case,a.sequence,a.amount)
    elif a.cmd=='agreements':set_agreements(a.db,json.loads(Path(a.file).read_text(encoding='utf-8')))
    elif a.cmd=='payout-plan':print(plan_payout(a.db,a.run,a.merchant))
    elif a.cmd=='payout-approve':approve_payout(a.db,a.plan)
    elif a.cmd=='payout-status':print(payout_status(a.db,a.plan))
    elif a.cmd=='review-register':print(register_reviewer(a.db,a.reviewer,a.name,a.roles))
    elif a.cmd=='review-list':print(json.dumps(list_reviewers(a.db),ensure_ascii=False,indent=2))
    elif a.cmd=='review-submit':print(submit_reconciliation(a.db,a.run,a.reviewer))
    elif a.cmd=='review-contract':print(freeze_contract(a.db,a.run,a.route,a.submitter,a.terms))
    elif a.cmd=='review-sign':print(sign_review(a.db,a.run,a.reviewer,a.role,a.decision,a.opinion,a.ref,a.source_digest))
    elif a.cmd=='review-withdraw':print(sign_review(a.db,a.run,a.reviewer,a.role or _reviewer_role(a.db,a.reviewer,a.ref,a.run),'withdraw',a.opinion,a.ref,a.source_digest))
    elif a.cmd=='review-status':print(json.dumps(review_status(a.db,a.run),ensure_ascii=False,indent=2))
    else:print(write(a.db,a.run,a.out))
def _reviewer_role(db_path,reviewer_id,ref,run):
    con=connect(db_path)
    try:
        row=con.execute('SELECT role FROM review_signature WHERE id=? AND run_id=?',(ref,run)).fetchone()
        if not row:raise ValueError('unknown_objection')
        return row['role']
    finally:con.close()
if __name__=='__main__':main()
