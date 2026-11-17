import json
import pytest
from dualsettle.__main__ import demo
from dualsettle.store import connect
from dualsettle.rules import input_digest
from dualsettle.payout import (open_reserve,release_reserve,set_agreements,plan_payout,
                               approve_payout,payout_status,decide_reserve,decision_report,
                               reserve_exposure)


def seeded(tmp):
    demo(tmp);db=tmp/'data.db'
    run=connect(db).execute('SELECT id FROM reconcile_run ORDER BY id DESC LIMIT 1').fetchone()[0]
    return db,run


def agreements():
    return [dict(id='A',merchant='M1',beneficiary='owner',share='0.60',start_at='2025-01-01T00:00:00+00:00',end_at='2027-01-01T00:00:00+00:00',version=1),
            dict(id='B',merchant='M1',beneficiary='operator',share='0.40',start_at='2025-01-01T00:00:00+00:00',end_at='2027-01-01T00:00:00+00:00',version=1)]


def dispute(tmp):
    """Frozen net 99.00, held 10.00, already released 2.00 -> 8.00 decidable."""
    db,run=seeded(tmp)
    set_agreements(db,agreements())
    open_reserve(db,'case-1','M1','D1','10.00','open dispute')
    release_reserve(db,'case-1',1,'1.00')
    release_reserve(db,'case-1',2,'1.00')
    return db,run


def lines(db,plan):
    return {r['beneficiary']:r['amount'] for r in connect(db).execute(
        'SELECT beneficiary,amount FROM payout_line WHERE plan_id=?',(plan,))}


def basis(db,plan):
    return dict(connect(db).execute('SELECT * FROM payout_basis WHERE plan_id=?',(plan,)).fetchone())


def totals(db,run):
    return decision_report(db,run,'M1')['reserve_totals']


def test_partial_win_and_loss_change_exposure_base_and_lines(tmp_path):
    db,run=dispute(tmp_path)
    early,status,_=plan_payout(db,run,'M1')
    assert status=='ready';approve_payout(db,early)
    assert lines(db,early)=={'owner':'54.60','operator':'36.40'}  # 91.00 while 8.00 held
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:judgment-1',None)
    decide_reserve(db,'case-1',2,'2026-01-04T00:00:00+00:00','5.00','loss','evidence:judgment-2','period_net')
    exposure,cases=reserve_exposure(connect(db),'M1','D1')
    assert (exposure,cases)==(0,[('case-1','0.00')])
    assert totals(db,run)=={'held':'0.00','released':'2.00','decided':'8.00','won':'3.00',
                            'loss':'5.00','period_loss':'5.00','recovery_loss':'0.00'}
    assert payout_status(db,early)=='invalidated'
    plan,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    assert basis(db,plan)['base']=='94.00'
    assert basis(db,plan)['final_deduction']=='5.00'
    assert lines(db,plan)=={'owner':'56.40','operator':'37.60'}
    approve_payout(db,plan)
    assert payout_status(db,plan)=='approved'
    # The loss never turns into a reconciliation difference or an allocation line.
    verdict=connect(db).execute('SELECT status,stream_net FROM verdict WHERE run_id=? AND merchant=?',(run,'M1')).fetchone()
    assert tuple(verdict)==('可封账','99.00')
    assert '5.00' not in lines(db,plan).values()


def test_period_net_and_recovery_balance_routes(tmp_path):
    # Route 1: the loss is charged to this period's merchant net.
    db,run=dispute(tmp_path/'period')
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:judgment-1',None)
    decide_reserve(db,'case-1',2,'2026-01-04T00:00:00+00:00','5.00','loss','evidence:judgment-2','period_net')
    period,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    assert basis(db,period)['base']=='94.00' and basis(db,period)['recovery_debt']=='0.00'
    assert basis(db,period)['final_deduction']=='5.00'
    assert decision_report(db,run,'M1')['recovery_debts']==[]

    # Route 2: an independent merchant recovery balance carries the loss.
    db2,run2=dispute(tmp_path/'recovery')
    decide_reserve(db2,'case-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:judgment-1',None)
    decide_reserve(db2,'case-1',2,'2026-01-04T00:00:00+00:00','5.00','loss','evidence:judgment-2','recovery_balance')
    recovery,status,_=plan_payout(db2,run2,'M1')
    assert status=='ready'
    assert basis(db2,recovery)['base']=='99.00'
    assert basis(db2,recovery)['final_deduction']=='5.00'
    assert basis(db2,recovery)['period_loss']=='0.00'
    assert basis(db2,recovery)['recovery_debt']=='5.00'
    assert lines(db2,recovery)=={'owner':'59.40','operator':'39.60'}
    debts=decision_report(db2,run2,'M1')['recovery_debts']
    assert [(d['case_id'],d['case_version'],d['amount'],d['status']) for d in debts]==[('case-1',2,'5.00','outstanding')]
    # 99.00 is paid out and a matching 5.00 debt is owed: nothing appears from nowhere.
    assert sum(float(v) for v in lines(db2,recovery).values())==99.0
    assert totals(db2,run2)=={'held':'0.00','released':'2.00','decided':'8.00','won':'3.00',
                              'loss':'5.00','period_loss':'0.00','recovery_loss':'5.00'}


def test_decision_replay_and_conflicts_are_atomic(tmp_path):
    db,run=dispute(tmp_path)
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:a',None)
    before=decision_report(db,run,'M1')['reserve_totals']
    # Identical replay is idempotent.
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','3.00','win','evidence:a',None)
    assert connect(db).execute("SELECT COUNT(*) FROM reserve_decision WHERE case_id='case-1'").fetchone()[0]==1
    assert decision_report(db,run,'M1')['reserve_totals']==before
    approved,status,_=plan_payout(db,run,'M1');assert status=='ready';approve_payout(db,approved)
    # Same identity, different content is rejected and changes nothing.
    with pytest.raises(ValueError,match='reserve_decision_conflict'):
        decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','4.00','win','evidence:a',None)
    # Over-amount against the still-held 5.00 is rejected.
    with pytest.raises(ValueError,match='reserve_decision_overamount'):
        decide_reserve(db,'case-1',2,'2026-01-04T00:00:00+00:00','5.01','loss','evidence:b','period_net')
    # A new version decided earlier than an existing one is rejected.
    with pytest.raises(ValueError,match='reserve_decision_time_conflict'):
        decide_reserve(db,'case-1',2,'2026-01-02T00:00:00+00:00','1.00','win','evidence:b',None)
    # A reused version number is an identity conflict, not a new decision.
    with pytest.raises(ValueError,match='reserve_decision_conflict'):
        decide_reserve(db,'case-1',1,'2026-01-05T00:00:00+00:00','1.00','win','evidence:c',None)
    # Every rejection left both exposure and the prior approval untouched.
    assert decision_report(db,run,'M1')['reserve_totals']['held']=='5.00'
    assert decision_report(db,run,'M1')['reserve_totals']['won']=='3.00'
    assert payout_status(db,approved)=='approved'
    assert connect(db).execute("SELECT COUNT(*) FROM reserve_decision WHERE case_id='case-1'").fetchone()[0]==1
    # A legitimate later version is accepted, then an earlier one is not.
    decide_reserve(db,'case-1',2,'2026-01-04T00:00:00+00:00','1.00','win','evidence:b',None)
    assert payout_status(db,approved)=='invalidated'
    with pytest.raises(ValueError,match='reserve_decision_time_conflict'):
        decide_reserve(db,'case-1',3,'2026-01-03T12:00:00+00:00','1.00','win','evidence:c',None)
    assert decision_report(db,run,'M1')['reserve_totals']['held']=='4.00'
    assert connect(db).execute("SELECT COUNT(*) FROM reserve_decision WHERE case_id='case-1'").fetchone()[0]==2


def test_decision_before_approval_rejects_old_plan(tmp_path):
    db,run=dispute(tmp_path)
    plan,status,_=plan_payout(db,run,'M1');assert status=='ready'
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','8.00','win','evidence:judgment',None)
    with pytest.raises(ValueError,match='payout_inputs_changed'):
        approve_payout(db,plan)
    assert connect(db).execute('SELECT status FROM payout_plan WHERE id=?',(plan,)).fetchone()[0]=='ready'
    fresh,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    assert basis(db,fresh)['base']=='99.00'
    assert lines(db,fresh)=={'owner':'59.40','operator':'39.60'}


def test_decision_after_approval_invalidates_without_touching_frozen_evidence(tmp_path):
    db,run=dispute(tmp_path)
    plan,_,_=plan_payout(db,run,'M1');approve_payout(db,plan)
    con=connect(db)
    snapshot=(input_digest(con),con.execute('SELECT status,source_digest FROM reconcile_run WHERE id=?',(run,)).fetchone()[:],
              con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0],
              con.execute('SELECT status,stream_net,batch_net FROM verdict WHERE run_id=? AND merchant=?',(run,'M1')).fetchone()[:])
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','8.00','loss','evidence:judgment','period_net')
    assert payout_status(db,plan)=='invalidated'
    after=(input_digest(con),con.execute('SELECT status,source_digest FROM reconcile_run WHERE id=?',(run,)).fetchone()[:],
           con.execute('SELECT COUNT(*) FROM evidence WHERE run_id=?',(run,)).fetchone()[0],
           con.execute('SELECT status,stream_net,batch_net FROM verdict WHERE run_id=? AND merchant=?',(run,'M1')).fetchone()[:])
    assert after==snapshot
    fresh,status,_=plan_payout(db,run,'M1')
    assert status=='ready' and basis(db,fresh)['base']=='91.00'
    assert lines(db,fresh)=={'owner':'54.60','operator':'36.40'}


def test_route_must_be_authorized_and_decided_money_is_never_reheld_or_rereleased(tmp_path):
    db,run=dispute(tmp_path)
    with pytest.raises(ValueError,match='invalid_decision_route'):
        decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','5.00','loss','evidence:x',None)
    with pytest.raises(ValueError,match='invalid_decision_route'):
        decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','5.00','loss','evidence:x','guess')
    with pytest.raises(ValueError,match='invalid_decision_outcome'):
        decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','5.00','settled','evidence:x','period_net')
    decide_reserve(db,'case-1',1,'2026-01-03T00:00:00+00:00','5.00','loss','evidence:x','recovery_balance')
    with pytest.raises(ValueError,match='reserve_overrelease'):
        release_reserve(db,'case-1',3,'5.01')
    with pytest.raises(ValueError,match='reserve_decision_overamount'):
        decide_reserve(db,'case-1',2,'2026-01-04T00:00:00+00:00','3.01','win','evidence:y',None)
    # The unreleased, undecided remainder can still be released exactly.
    release_reserve(db,'case-1',3,'3.00')
    assert reserve_exposure(connect(db),'M1','D1')==(0,[('case-1','0.00')])
