"""Reserve exposure, case decisions and beneficiary allocation after a frozen reconciliation."""
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from .store import connect
from .rules import input_digest
from .timeline import at

CENT=Decimal('0.01')
DECISION_OUTCOMES=('win','loss')
# period_net: the loss is charged against this settlement period's merchant net.
# recovery_balance: the loss is carried by a separate merchant recovery debt.
DECISION_ROUTES=('period_net','recovery_balance')


def _money(value):
    number=Decimal(str(value))
    if not number.is_finite() or number<0 or number!=number.quantize(CENT):
        raise ValueError('invalid_money')
    return number


def _text(value,error):
    if not isinstance(value,str) or not value.strip():raise ValueError(error)
    return value.strip()


def _centstr(value):
    """Canonical two-decimal rendering for persisted/reported money."""
    return str(Decimal(value).quantize(CENT))


def open_reserve(path,case_id,merchant,window_id,amount,reason):
    amount=_money(amount)
    if amount<=0 or not reason.strip():raise ValueError('invalid_reserve')
    db=connect(path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM account WHERE id=?',(merchant,)).fetchone():raise ValueError('unknown_merchant')
            if not db.execute('SELECT 1 FROM window WHERE id=?',(window_id,)).fetchone():raise ValueError('unknown_window')
            old=db.execute('SELECT * FROM reserve_case WHERE id=?',(case_id,)).fetchone()
            value=(case_id,merchant,window_id,str(amount),reason)
            if old and tuple(old[k] for k in ('id','merchant','window_id','amount','reason'))!=value:
                raise ValueError('reserve_case_conflict')
            db.execute('INSERT OR IGNORE INTO reserve_case VALUES (?,?,?,?,?)',value)
    finally:db.close()


def release_reserve(path,case_id,sequence,amount):
    amount=_money(amount).quantize(CENT)
    if amount<=0:raise ValueError('invalid_release')
    db=connect(path)
    try:
        with db:
            case=db.execute('SELECT * FROM reserve_case WHERE id=?',(case_id,)).fetchone()
            if not case:raise ValueError('unknown_reserve')
            old=db.execute('SELECT amount FROM reserve_release WHERE case_id=? AND sequence=?',(case_id,sequence)).fetchone()
            if old:
                if Decimal(old['amount'])!=amount:raise ValueError('reserve_release_conflict')
                return
            released=sum((Decimal(r['amount']) for r in db.execute('SELECT amount FROM reserve_release WHERE case_id=?',(case_id,))),Decimal(0))
            decided=sum((Decimal(r['amount']) for r in db.execute('SELECT amount FROM reserve_decision WHERE case_id=?',(case_id,))),Decimal(0))
            # A decided amount can never be released (or decided) a second time.
            if released+decided+amount>Decimal(case['amount']):raise ValueError('reserve_overrelease')
            db.execute('INSERT INTO reserve_release(case_id,sequence,amount) VALUES (?,?,?)',(case_id,sequence,str(amount)))
    finally:db.close()


def decide_reserve(path,case_id,case_version,decided_at,amount,outcome,evidence_ref,route):
    """Record one final, versioned decision for a reserve case.

    Decisions are additive: partial wins and partial losses can be recorded in
    separate versions.  Their sum plus prior releases may never exceed the
    original case amount.  Same identity + same content is a no-op; any other
    replay is rejected before touching exposure or approvals.
    """
    amount=_money(amount).quantize(CENT)
    if amount<=0:raise ValueError('invalid_decision')
    if not isinstance(case_version,int) or case_version<=0:raise ValueError('invalid_decision')
    if outcome not in DECISION_OUTCOMES:raise ValueError('invalid_decision_outcome')
    # A win needs no disposal route; a loss must be routed explicitly by the
    # case input.  Never infer a route from a default.
    if route is not None and route not in DECISION_ROUTES:
        raise ValueError('invalid_decision_route')
    if outcome=='loss' and route is None:raise ValueError('invalid_decision_route')
    evidence_ref=_text(evidence_ref,'invalid_decision_evidence')
    when=at(decided_at).isoformat()
    db=connect(path)
    try:
        with db:
            case=db.execute('SELECT * FROM reserve_case WHERE id=?',(case_id,)).fetchone()
            if not case:raise ValueError('unknown_reserve')
            old=db.execute('SELECT * FROM reserve_decision WHERE case_id=? AND case_version=?',(case_id,case_version)).fetchone()
            value=(case_id,case_version,when,str(amount),outcome,route,evidence_ref)
            if old and tuple(old[k] for k in ('case_id','case_version','decided_at','amount','outcome','route','evidence_ref'))!=value:
                raise ValueError('reserve_decision_conflict')
            if old:return
            existing=[dict(r) for r in db.execute('SELECT * FROM reserve_decision WHERE case_id=? ORDER BY case_version',(case_id,))]
            if existing:
                if case_version<=max(r['case_version'] for r in existing):
                    raise ValueError('reserve_decision_time_conflict')
                if at(when)<max(at(r['decided_at']) for r in existing):
                    raise ValueError('reserve_decision_time_conflict')
            released=sum((Decimal(r['amount']) for r in db.execute('SELECT amount FROM reserve_release WHERE case_id=?',(case_id,))),Decimal(0))
            decided=sum((Decimal(r['amount']) for r in existing),Decimal(0))
            remaining=Decimal(case['amount'])-released-decided
            if amount>remaining:raise ValueError('reserve_decision_overamount')
            db.execute('INSERT INTO reserve_decision(case_id,case_version,decided_at,amount,outcome,route,evidence_ref) VALUES (?,?,?,?,?,?,?)',
                       (case_id,case_version,when,str(amount),outcome,route,evidence_ref))
            if outcome=='loss' and route=='recovery_balance':
                db.execute('INSERT INTO recovery_debt(merchant,window_id,case_id,case_version,amount) VALUES (?,?,?,?,?)',
                           (case['merchant'],case['window_id'],case_id,case_version,str(amount)))
    finally:db.close()


def _case_rows(db,merchant,window_id):
    return [dict(r) for r in db.execute('SELECT * FROM reserve_case WHERE merchant=? AND window_id=? ORDER BY id',(merchant,window_id))]


def _reserve_position(db,merchant,window_id):
    """Current held exposure plus the decided/released split for each case."""
    totals={'held':Decimal(0),'released':Decimal(0),'decided':Decimal(0),'won':Decimal(0),
            'loss':Decimal(0),'period_loss':Decimal(0),'recovery_loss':Decimal(0)}
    cases=[]
    for case in _case_rows(db,merchant,window_id):
        releases=[dict(r) for r in db.execute('SELECT * FROM reserve_release WHERE case_id=? ORDER BY sequence',(case['id'],))]
        decisions=[dict(r) for r in db.execute('SELECT * FROM reserve_decision WHERE case_id=? ORDER BY case_version',(case['id'],))]
        released=sum((Decimal(r['amount']) for r in releases),Decimal(0))
        decided=sum((Decimal(r['amount']) for r in decisions),Decimal(0))
        amount=Decimal(case['amount'])
        held=amount-released-decided
        if held<0:raise ValueError('reserve_position_invalid')
        won=sum((Decimal(r['amount']) for r in decisions if r['outcome']=='win'),Decimal(0))
        loss=sum((Decimal(r['amount']) for r in decisions if r['outcome']=='loss'),Decimal(0))
        period_loss=sum((Decimal(r['amount']) for r in decisions if r['outcome']=='loss' and r['route']=='period_net'),Decimal(0))
        recovery_loss=sum((Decimal(r['amount']) for r in decisions if r['outcome']=='loss' and r['route']=='recovery_balance'),Decimal(0))
        for key,value in (('held',held),('released',released),('decided',decided),('won',won),
                          ('loss',loss),('period_loss',period_loss),('recovery_loss',recovery_loss)):
            totals[key]+=value
        cases.append({'case':case['id'],'amount':_centstr(amount),'released':_centstr(released),
                      'decided':_centstr(decided),'held':_centstr(held),'releases':releases,'decisions':decisions})
    for key in totals:totals[key]=totals[key].quantize(CENT)
    return {'totals':totals,'cases':cases}


def reserve_exposure(db,merchant,window_id):
    position=_reserve_position(db,merchant,window_id)
    return position['totals']['held'],[(c['case'],c['held']) for c in position['cases']]

def set_agreements(path,rows):
    if not rows:raise ValueError('empty_agreements')
    db=connect(path)
    try:
        with db:
            for row in rows:
                share=Decimal(str(row['share']))
                if not share.is_finite() or not 0<share<=1 or at(row['start_at'])>=at(row['end_at']):
                    raise ValueError('invalid_agreement')
                if not db.execute('SELECT 1 FROM account WHERE id=?',(row['merchant'],)).fetchone():
                    raise ValueError('unknown_merchant')
                value=(row['id'],row['merchant'],row['beneficiary'],str(share),row['start_at'],row['end_at'],int(row['version']))
                old=db.execute('SELECT * FROM allocation_agreement WHERE id=?',(row['id'],)).fetchone()
                if old and tuple(old[k] for k in ('id','merchant','beneficiary','share','start_at','end_at','version'))!=value:
                    raise ValueError('agreement_identity_conflict')
                db.execute('INSERT OR IGNORE INTO allocation_agreement VALUES (?,?,?,?,?,?,?)',value)
    finally:db.close()


def _digest(db,run_id,merchant):
    run=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run_id,)).fetchone()
    cases=[]
    for case in _case_rows(db,merchant,run['window_id']):
        releases=[[r['sequence'],r['amount']] for r in db.execute(
            'SELECT sequence,amount FROM reserve_release WHERE case_id=? ORDER BY sequence',(case['id'],))]
        decisions=[[r['case_version'],r['decided_at'],r['amount'],r['outcome'],r['route'],r['evidence_ref']] for r in db.execute(
            'SELECT case_version,decided_at,amount,outcome,route,evidence_ref FROM reserve_decision WHERE case_id=? ORDER BY case_version',(case['id'],))]
        cases.append([case['id'],case['amount'],releases,decisions])
    agreements=[tuple(r) for r in db.execute('SELECT * FROM allocation_agreement WHERE merchant=? ORDER BY id',(merchant,))]
    debts=[[r['case_id'],r['case_version'],r['amount'],r['status']] for r in db.execute(
        'SELECT case_id,case_version,amount,status FROM recovery_debt WHERE merchant=? AND window_id=? ORDER BY case_id,case_version',
        (merchant,run['window_id']))]
    payload=[run['source_digest'],input_digest(db),cases,agreements,debts]
    digest=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
    return digest,_reserve_position(db,merchant,run['window_id'])


def plan_payout(path,run_id,merchant):
    db=connect(path)
    try:
        with db:
            run=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run_id,)).fetchone()
            verdict=db.execute('SELECT * FROM verdict WHERE run_id=? AND merchant=?',(run_id,merchant)).fetchone()
            if not run or not verdict:raise ValueError('unknown_run_or_merchant')
            window=db.execute('SELECT * FROM window WHERE id=?',(run['window_id'],)).fetchone()
            digest,position=_digest(db,run_id,merchant)
            totals=position['totals']
            agreements=[dict(r) for r in db.execute('SELECT * FROM allocation_agreement WHERE merchant=?',(merchant,))
                        if at(r['start_at'])<=at(window['start_at'])<at(r['end_at'])]
            versions={r['version'] for r in agreements}
            shares=sum((Decimal(r['share']) for r in agreements),Decimal(0))
            net=Decimal(verdict['stream_net'])
            # A loss is always a final deduction; the route only decides who
            # bears it.  period_net takes it out of this period's base, while
            # recovery_balance leaves the base intact and records a debt.
            available=net-totals['held']-totals['period_loss']
            if run['status']!='已归档' or verdict['status']!='可封账' or input_digest(db)!=run['source_digest']:
                status,reason='blocked','reconciliation_not_current'
            elif available<0:status,reason='blocked','reserve_exceeds_net'
            elif not agreements or len(versions)!=1 or len({r['beneficiary'] for r in agreements})!=len(agreements) or shares!=1:
                status,reason='blocked','agreement_ambiguous'
            else:status,reason='ready','allocatable'
            pid=db.execute('INSERT INTO payout_plan(run_id,merchant,status,reason,input_digest) VALUES (?,?,?,?,?)',
                           (run_id,merchant,status,reason,digest)).lastrowid
            # final_deduction is the total decided loss: it is never allocatable.
            # period_loss shows how much of it reduced this period's base; the
            # remainder is funded by recovery_debt.
            db.execute('INSERT INTO payout_basis(plan_id,net,held,released,won,loss,period_loss,recovery_loss,base,final_deduction,recovery_debt) '
                       'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       (pid,str(net),str(totals['held']),str(totals['released']),str(totals['won']),str(totals['loss']),
                        str(totals['period_loss']),str(totals['recovery_loss']),str(available),
                        str(totals['loss']),str(totals['recovery_loss'])))
            if status=='ready':
                ordered=sorted(agreements,key=lambda r:(-Decimal(r['share']),r['beneficiary']))
                allocated=Decimal(0)
                for i,agreement in enumerate(ordered):
                    amount=(available-allocated if i==len(ordered)-1 else
                            (available*Decimal(agreement['share'])).quantize(CENT,rounding=ROUND_HALF_UP))
                    allocated+=amount
                    db.execute('INSERT INTO payout_line VALUES (?,?,?)',(pid,agreement['beneficiary'],str(amount)))
            return pid,status,reason
    finally:db.close()


def approve_payout(path,plan_id):
    db=connect(path)
    try:
        with db:
            plan=db.execute('SELECT * FROM payout_plan WHERE id=?',(plan_id,)).fetchone()
            if not plan or plan['status']!='ready':raise ValueError('plan_not_ready')
            digest,_=_digest(db,plan['run_id'],plan['merchant'])
            if digest!=plan['input_digest']:raise ValueError('payout_inputs_changed')
            db.execute("UPDATE payout_plan SET status='approved' WHERE id=?",(plan_id,))
    finally:db.close()


def payout_status(path,plan_id):
    """Report the current usability of a historical payout approval."""
    db=connect(path)
    try:
        plan=db.execute('SELECT * FROM payout_plan WHERE id=?',(plan_id,)).fetchone()
        if not plan:raise ValueError('unknown_payout_plan')
        run=db.execute('SELECT * FROM reconcile_run WHERE id=?',(plan['run_id'],)).fetchone()
        digest,_=_digest(db,plan['run_id'],plan['merchant'])
        if run['status']!='已归档' or input_digest(db)!=run['source_digest'] or digest!=plan['input_digest']:
            return 'invalidated'
        return plan['status']
    finally:db.close()


def decision_report(path,run_id,merchant):
    """Trace cases, releases, decisions, recovery debts and allocations for a run."""
    db=connect(path)
    try:
        run=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run_id,)).fetchone()
        verdict=db.execute('SELECT * FROM verdict WHERE run_id=? AND merchant=?',(run_id,merchant)).fetchone()
        if not run or not verdict:raise ValueError('unknown_run_or_merchant')
        position=_reserve_position(db,merchant,run['window_id'])
        plans=[]
        for plan in db.execute('SELECT * FROM payout_plan WHERE run_id=? AND merchant=? ORDER BY id',(run_id,merchant)):
            basis=db.execute('SELECT * FROM payout_basis WHERE plan_id=?',(plan['id'],)).fetchone()
            plans.append({'id':plan['id'],'status':plan['status'],'reason':plan['reason'],
                          'input_digest':plan['input_digest'],
                          'basis':dict(basis) if basis else None,
                          'lines':[dict(r) for r in db.execute('SELECT * FROM payout_line WHERE plan_id=? ORDER BY beneficiary',(plan['id'],))]})
        debts=[dict(r) for r in db.execute('SELECT * FROM recovery_debt WHERE merchant=? AND window_id=? ORDER BY case_id,case_version',
                                           (merchant,run['window_id']))]
        return {'run':dict(run),'verdict':dict(verdict),
                'reconciliation_current':input_digest(db)==run['source_digest'],
                'evidence_count':db.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run_id,)).fetchone()[0],
                'issue_count':db.execute('SELECT COUNT(*) FROM reconcile_issue WHERE run_id=?',(run_id,)).fetchone()[0],
                'reserve_totals':{k:str(v) for k,v in position['totals'].items()},
                'cases':position['cases'],'recovery_debts':debts,'plans':plans}
    finally:db.close()
