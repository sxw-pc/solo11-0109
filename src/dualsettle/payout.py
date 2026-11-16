"""Reserve exposure and beneficiary allocation after a frozen reconciliation."""
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from .store import connect
from .rules import input_digest
from .timeline import at

CENT=Decimal('0.01')


def _money(value):
    number=Decimal(str(value))
    if not number.is_finite() or number<0 or number!=number.quantize(CENT):
        raise ValueError('invalid_money')
    return number


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
    amount=_money(amount)
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
            if released+amount>Decimal(case['amount']):raise ValueError('reserve_overrelease')
            db.execute('INSERT INTO reserve_release(case_id,sequence,amount) VALUES (?,?,?)',(case_id,sequence,str(amount)))
    finally:db.close()


def reserve_exposure(db,merchant,window_id):
    cases=[];total=Decimal(0)
    for case in db.execute('SELECT * FROM reserve_case WHERE merchant=? AND window_id=? ORDER BY id',(merchant,window_id)):
        released=sum((Decimal(r['amount']) for r in db.execute('SELECT amount FROM reserve_release WHERE case_id=?',(case['id'],))),Decimal(0))
        remaining=Decimal(case['amount'])-released
        total+=remaining;cases.append((case['id'],str(remaining)))
    return total,cases


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
    exposure,cases=reserve_exposure(db,merchant,run['window_id'])
    agreements=[tuple(r) for r in db.execute('SELECT * FROM allocation_agreement WHERE merchant=? ORDER BY id',(merchant,))]
    payload=[run['source_digest'],input_digest(db),cases,agreements]
    return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),exposure


def plan_payout(path,run_id,merchant):
    db=connect(path)
    try:
        with db:
            run=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run_id,)).fetchone()
            verdict=db.execute('SELECT * FROM verdict WHERE run_id=? AND merchant=?',(run_id,merchant)).fetchone()
            if not run or not verdict:raise ValueError('unknown_run_or_merchant')
            window=db.execute('SELECT * FROM window WHERE id=?',(run['window_id'],)).fetchone()
            digest,reserve=_digest(db,run_id,merchant)
            agreements=[dict(r) for r in db.execute('SELECT * FROM allocation_agreement WHERE merchant=?',(merchant,))
                        if at(r['start_at'])<=at(window['start_at'])<at(r['end_at'])]
            versions={r['version'] for r in agreements}
            shares=sum((Decimal(r['share']) for r in agreements),Decimal(0))
            net=Decimal(verdict['stream_net']);available=net-reserve
            if run['status']!='已归档' or verdict['status']!='可封账' or input_digest(db)!=run['source_digest']:
                status,reason='blocked','reconciliation_not_current'
            elif available<0:status,reason='blocked','reserve_exceeds_net'
            elif not agreements or len(versions)!=1 or len({r['beneficiary'] for r in agreements})!=len(agreements) or shares!=1:
                status,reason='blocked','agreement_ambiguous'
            else:status,reason='ready','allocatable'
            pid=db.execute('INSERT INTO payout_plan(run_id,merchant,status,reason,input_digest) VALUES (?,?,?,?,?)',
                           (run_id,merchant,status,reason,digest)).lastrowid
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
        return 'invalidated' if run['status']!='已归档' or digest!=plan['input_digest'] else plan['status']
    finally:db.close()
