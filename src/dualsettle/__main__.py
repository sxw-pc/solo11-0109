import argparse,csv,json,tempfile
from pathlib import Path
from .store import connect
from .ingest import stream,batch
from .rules import reconcile,close
from .report import write
from .payout import (open_reserve,release_reserve,set_agreements,plan_payout,approve_payout,
                     payout_status,decide_reserve,decision_report)


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


def _agreements(merchant='M1'):
    return [
        {'id':'owner-1','merchant':merchant,'beneficiary':'owner','share':'0.60','start_at':'2025-01-01T00:00:00+00:00','end_at':'2027-01-01T00:00:00+00:00','version':1},
        {'id':'operator-1','merchant':merchant,'beneficiary':'operator','share':'0.40','start_at':'2025-01-01T00:00:00+00:00','end_at':'2027-01-01T00:00:00+00:00','version':1}]


def _scenario(root):
    """99.00 frozen net, 10.00 held, 2.00 previously released: 8.00 subject to decision."""
    demo(root);db=root/'data.db';con=connect(db)
    run=con.execute('SELECT MAX(id) FROM reconcile_run').fetchone()[0];con.close()
    open_reserve(db,'dispute-1','M1','D1','10.00','synthetic dispute')
    for sequence in (1,2):release_reserve(db,'dispute-1',sequence,'1.00')
    set_agreements(db,_agreements())
    return db,run


def _snapshot(db,run,plan):
    report=decision_report(db,run,'M1')
    entry=next(p for p in report['plans'] if p['id']==plan)
    lines={l['beneficiary']:l['amount'] for l in entry['lines']}
    return {'plan':plan,'base':entry['basis']['base'],'final_deduction':entry['basis']['final_deduction'],
            'recovery_debt':entry['basis']['recovery_debt'],
            'owner':lines.get('owner'),'operator':lines.get('operator'),
            'reserve_totals':report['reserve_totals'],'cases':report['cases'],
            'recovery_debts':report['recovery_debts']}


def decision_demo(workdir=None):
    root=Path(workdir or tempfile.mkdtemp(prefix='dualsettle-decision-'));root.mkdir(parents=True,exist_ok=True)
    # Route 1: the loss is charged against this settlement period's merchant net.
    one=root/'period_net';db,run=_scenario(one)
    early,status,_=plan_payout(db,run,'M1')
    if status=='ready':approve_payout(db,early)
    before=payout_status(db,early)
    decide_reserve(db,'dispute-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:judgment-1',None)
    decide_reserve(db,'dispute-1',2,'2026-01-04T00:00:00+00:00','5.00','loss','evidence:judgment-2','period_net')
    after=payout_status(db,early)
    plan,status,_=plan_payout(db,run,'M1')
    if status=='ready':approve_payout(db,plan)
    first=_snapshot(db,run,plan)
    first.update({'early_plan':early,'early_status_before_decision':before,
                  'early_status_after_decision':after,'final_status':payout_status(db,plan),
                  'reconcile_evidence_unchanged':decision_report(db,run,'M1')['reconciliation_current']})
    # Route 2: a separate merchant recovery balance carries the loss.  The period
    # base stays 99.00 and the 5.00 is recorded as an explicit outstanding debt.
    two=root/'recovery_balance';db2,run2=_scenario(two)
    decide_reserve(db2,'dispute-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:judgment-1',None)
    decide_reserve(db2,'dispute-1',2,'2026-01-04T00:00:00+00:00','5.00','loss','evidence:judgment-2','recovery_balance')
    plan2,status2,_=plan_payout(db2,run2,'M1')
    if status2=='ready':approve_payout(db2,plan2)
    second=_snapshot(db2,run2,plan2)
    second.update({'final_status':payout_status(db2,plan2),
                   'reconcile_evidence_unchanged':decision_report(db2,run2,'M1')['reconciliation_current']})
    return {'reserve_case':'dispute-1','period_net':first,'recovery_balance':second,
            'db':str(db),'route2_db':str(db2)}


def main():
    p=argparse.ArgumentParser(description='离线实时轨与日终轨封账核对');s=p.add_subparsers(dest='cmd',required=True)
    d=s.add_parser('demo');d.add_argument('--workdir')
    dd=s.add_parser('demo-decisions');dd.add_argument('--workdir')
    i=s.add_parser('ingest-stream');i.add_argument('file');i.add_argument('--db',required=True)
    b=s.add_parser('ingest-batch');b.add_argument('file');b.add_argument('--db',required=True);b.add_argument('--window',required=True)
    r=s.add_parser('reconcile');r.add_argument('--db',required=True);r.add_argument('--window',required=True)
    c=s.add_parser('close');c.add_argument('--db',required=True);c.add_argument('--run',type=int,required=True)
    q=s.add_parser('report');q.add_argument('--db',required=True);q.add_argument('--run',type=int,required=True);q.add_argument('--out',required=True)
    h=s.add_parser('reserve-open');h.add_argument('--db',required=True);h.add_argument('--case',required=True);h.add_argument('--merchant',required=True);h.add_argument('--window',required=True);h.add_argument('--amount',required=True);h.add_argument('--reason',required=True)
    rr=s.add_parser('reserve-release');rr.add_argument('--db',required=True);rr.add_argument('--case',required=True);rr.add_argument('--sequence',type=int,required=True);rr.add_argument('--amount',required=True)
    dc=s.add_parser('reserve-decide');dc.add_argument('--db',required=True);dc.add_argument('--case',required=True);dc.add_argument('--version',type=int,required=True);dc.add_argument('--at',required=True);dc.add_argument('--amount',required=True);dc.add_argument('--outcome',required=True,choices=('win','loss'));dc.add_argument('--route',choices=('period_net','recovery_balance'));dc.add_argument('--evidence',required=True)
    dr=s.add_parser('decisions');dr.add_argument('--db',required=True);dr.add_argument('--run',type=int,required=True);dr.add_argument('--merchant',required=True)
    ag=s.add_parser('agreements');ag.add_argument('file');ag.add_argument('--db',required=True)
    pp=s.add_parser('payout-plan');pp.add_argument('--db',required=True);pp.add_argument('--run',type=int,required=True);pp.add_argument('--merchant',required=True)
    ap=s.add_parser('payout-approve');ap.add_argument('--db',required=True);ap.add_argument('--plan',type=int,required=True)
    ps=s.add_parser('payout-status');ps.add_argument('--db',required=True);ps.add_argument('--plan',type=int,required=True)
    a=p.parse_args()
    if a.cmd=='demo':print(demo(a.workdir))
    elif a.cmd=='demo-decisions':print(json.dumps(decision_demo(a.workdir),ensure_ascii=False,indent=2))
    elif a.cmd=='ingest-stream':print(stream(a.file,a.db))
    elif a.cmd=='ingest-batch':print(batch(a.file,a.db,a.window))
    elif a.cmd=='reconcile':print(reconcile(a.db,a.window))
    elif a.cmd=='close':close(a.db,a.run)
    elif a.cmd=='reserve-open':open_reserve(a.db,a.case,a.merchant,a.window,a.amount,a.reason)
    elif a.cmd=='reserve-release':release_reserve(a.db,a.case,a.sequence,a.amount)
    elif a.cmd=='reserve-decide':decide_reserve(a.db,a.case,a.version,a.at,a.amount,a.outcome,a.evidence,a.route)
    elif a.cmd=='decisions':print(json.dumps(decision_report(a.db,a.run,a.merchant),ensure_ascii=False,indent=2))
    elif a.cmd=='agreements':set_agreements(a.db,json.loads(Path(a.file).read_text(encoding='utf-8')))
    elif a.cmd=='payout-plan':print(plan_payout(a.db,a.run,a.merchant))
    elif a.cmd=='payout-approve':approve_payout(a.db,a.plan)
    elif a.cmd=='payout-status':print(payout_status(a.db,a.plan))
    else:print(write(a.db,a.run,a.out))
if __name__=='__main__':main()
