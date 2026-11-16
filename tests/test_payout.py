import json
import pytest
from dualsettle.__main__ import demo
from dualsettle.payout import open_reserve,release_reserve,set_agreements,plan_payout,approve_payout,payout_status
from dualsettle.store import connect


def seeded(tmp):
    demo(tmp);db=tmp/'data.db'
    run=connect(db).execute('SELECT id FROM reconcile_run ORDER BY id DESC LIMIT 1').fetchone()[0]
    return db,run


def agreements():
    return [dict(id='A',merchant='M1',beneficiary='owner',share='0.60',start_at='2025-01-01T00:00:00+00:00',end_at='2027-01-01T00:00:00+00:00',version=1),
            dict(id='B',merchant='M1',beneficiary='operator',share='0.40',start_at='2025-01-01T00:00:00+00:00',end_at='2027-01-01T00:00:00+00:00',version=1)]


def test_reserve_exposure_changes_payout_and_release_is_audited(tmp_path):
    db,run=seeded(tmp_path)
    set_agreements(db,agreements())
    open_reserve(db,'case-1','M1','D1','10.00','open dispute')
    plan,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    con=connect(db)
    assert dict(con.execute('SELECT beneficiary,amount FROM payout_line WHERE plan_id=?',(plan,)).fetchall())=={'owner':'53.40','operator':'35.60'}
    approve_payout(db,plan)
    assert payout_status(db,plan)=='approved'
    release_reserve(db,'case-1',1,'4.00')
    assert payout_status(db,plan)=='invalidated'
    plan_mid,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    release_reserve(db,'case-1',2,'6.00')
    with pytest.raises(ValueError,match='payout_inputs_changed'):
        approve_payout(db,plan_mid)
    with pytest.raises(ValueError,match='reserve_overrelease'):
        release_reserve(db,'case-1',3,'0.01')
    plan2,status,_=plan_payout(db,run,'M1')
    assert status=='ready'
    assert sum(float(r['amount']) for r in con.execute('SELECT amount FROM payout_line WHERE plan_id=?',(plan2,)))==99.0


def test_agreement_conflict_and_reserve_overhang_block_plan(tmp_path):
    db,run=seeded(tmp_path)
    only=agreements()[:1];set_agreements(db,only)
    assert plan_payout(db,run,'M1')[1]=='blocked'
    open_reserve(db,'case-big','M1','D1','100.00','held for review')
    set_agreements(db,agreements()[1:])
    assert plan_payout(db,run,'M1')[2]=='reserve_exceeds_net'


def test_reserve_identity_conflict_does_not_change_exposure(tmp_path):
    db,run=seeded(tmp_path)
    open_reserve(db,'case-1','M1','D1','10.00','dispute')
    with pytest.raises(ValueError,match='reserve_case_conflict'):
        open_reserve(db,'case-1','M1','D1','20.00','dispute')
    release_reserve(db,'case-1',1,'5.00')
    with pytest.raises(ValueError,match='reserve_release_conflict'):
        release_reserve(db,'case-1',1,'4.00')
