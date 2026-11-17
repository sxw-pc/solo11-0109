import csv
import json
import sqlite3

import pytest

from dualsettle.ingest import batch, stream
from dualsettle.report import write
from dualsettle.review import (
    freeze_contract,
    list_reviewers,
    register_reviewer,
    review_status,
    sign_review,
    submit_reconciliation,
)
from dualsettle.rules import close, reconcile
from dualsettle.store import connect, migrate

REVIEWERS = [
    ('sub-1', '核对员甲', ['finance']),
    ('fin-1', '财务审核乙', ['finance']),
    ('ops-1', '运营审核丙', ['operations']),
    ('risk-1', '风险审核丁', ['risk']),
    ('risk-2', '风险审核戊', ['risk']),
    ('arb-1', '独立仲裁己', ['arbitrator']),
]


def created(tmp):
    db = tmp / 'ledger.db'
    con = connect(db)
    with con:
        con.execute("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00',"
                    "'2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        con.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',
                    ('M1', '2025-01-01T00:00:00+00:00', '2027-01-01T00:00:00+00:00', '0.01'))
    con.close()
    events = tmp / 'events.jsonl'
    events.write_text('\n'.join(json.dumps({
        'merchant': 'M1', 'tx': 'T1', 'version': n, 'kind': kind, 'amount': '100.00',
        'business_at': f'2026-01-01T0{n}:00:00+00:00'}) for n, kind in [(1, 'authorize'), (2, 'confirm')]),
        encoding='utf-8')
    lines = tmp / 'batch.csv'
    with lines.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['id', 'merchant', 'amount', 'corrects'])
        w.writerow(['B1', 'M1', '99.00', ''])
    stream(events, db)
    batch(lines, db, 'D1')
    return db


def judged(tmp):
    """A fresh judged run over synthetic amount evidence."""
    db = created(tmp)
    run, verdict = reconcile(db, 'D1')
    assert verdict == {'M1': '可封账'}
    for reviewer, name, roles in REVIEWERS:
        register_reviewer(db, reviewer, name, roles)
    submit_reconciliation(db, run, 'sub-1')
    return db, run


def freeze(db, run, route='standard'):
    return freeze_contract(db, run, route, 'sub-1', f'{route} 书面审核合同')


def standard_votes(db, run):
    sign_review(db, run, 'fin-1', 'finance', 'agree', '金额一致')
    sign_review(db, run, 'ops-1', 'operations', 'agree', '批次一致')


def risk_votes(db, run):
    sign_review(db, run, 'fin-1', 'finance', 'agree', '金额一致')
    sign_review(db, run, 'ops-1', 'operations', 'agree', '运营复核一致')
    sign_review(db, run, 'risk-1', 'risk', 'agree', '风险同意')


def split_case(tmp, route):
    """Finance/ops/risk agree, a second risk reviewer objects, arbitration dismisses."""
    db, run = judged(tmp)
    freeze(db, run, route)
    risk_votes(db, run)
    objection = sign_review(db, run, 'risk-2', 'risk', 'object', '保留风险异议')
    sign_review(db, run, 'arb-1', 'arbitrator', 'dismiss', '仲裁驳回异议', objection)
    return db, run, objection


# --- separation of duties and idempotent signing ---------------------------

def test_submitter_cannot_approve_own_reconciliation(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    with pytest.raises(ValueError, match='submitter_cannot_approve'):
        sign_review(db, run, 'sub-1', 'finance', 'agree', '自己批准')
    assert review_status(db, run)['authorized'] is False
    # The submitter may still object to their own reconciliation.
    objection = sign_review(db, run, 'sub-1', 'finance', 'object', '提交后发现有保留')
    assert review_status(db, run)['reason'] == 'objection_pending'
    sign_review(db, run, 'sub-1', 'finance', 'withdraw', '复核后撤回', objection)
    assert review_status(db, run)['authorized'] is False  # still lacks two votes


def test_one_reviewer_cannot_fill_two_route_seats(tmp_path):
    db, run = judged(tmp_path)
    register_reviewer(db, 'both-1', '双角色庚', ['finance', 'operations'])
    freeze(db, run)
    sign_review(db, run, 'both-1', 'finance', 'agree', '财务票')
    with pytest.raises(ValueError, match='dual_role_signature'):
        sign_review(db, run, 'both-1', 'operations', 'agree', '运营票')
    # A distinct role held by two different people satisfies the standard route.
    sign_review(db, run, 'fin-1', 'finance', 'agree', '财务复核')
    sign_review(db, run, 'ops-1', 'operations', 'agree', '运营复核')
    assert review_status(db, run)['authorized'] is True


def test_identical_replay_is_idempotent_and_rewrite_is_rejected(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    first = sign_review(db, run, 'fin-1', 'finance', 'agree', '金额一致')
    replay = sign_review(db, run, 'fin-1', 'finance', 'agree', '金额一致')
    assert replay == first
    with pytest.raises(ValueError, match='opinion_rewrite'):
        sign_review(db, run, 'fin-1', 'finance', 'agree', '金额其实不一致')
    con = connect(db)
    assert con.execute("SELECT COUNT(*) FROM review_signature WHERE reviewer='fin-1' AND decision='agree'"
                       ).fetchone()[0] == 1
    assert con.execute("SELECT opinion FROM review_signature WHERE reviewer='fin-1'").fetchone()[0] == '金额一致'


def test_contract_route_is_locked_after_votes(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'standard')
    standard_votes(db, run)
    assert review_status(db, run)['authorized'] is True
    with pytest.raises(ValueError, match='route_locked'):
        freeze(db, run, 'risk')
    assert review_status(db, run)['route'] == 'standard'


def test_review_required_and_verdict_untouched(tmp_path):
    db, run = judged(tmp_path)
    con = connect(db)
    before = [tuple(r) for r in con.execute('SELECT * FROM verdict WHERE run_id=?', (run,))]
    aggregates = [tuple(r) for r in con.execute('SELECT * FROM stream_aggregate ORDER BY 1,2')]
    lines = [tuple(r) for r in con.execute('SELECT * FROM batch_line ORDER BY id')]
    with pytest.raises(ValueError, match='review_required'):
        close(db, run)
    status = review_status(db, run)
    assert status['authorized'] is False and status['reason'] == 'review_required'
    assert [tuple(r) for r in connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (run,))] == before
    assert [tuple(r) for r in connect(db).execute('SELECT * FROM stream_aggregate ORDER BY 1,2')] == aggregates
    assert [tuple(r) for r in connect(db).execute('SELECT * FROM batch_line ORDER BY id')] == lines
    assert connect(db).execute('SELECT status FROM reconcile_run WHERE id=?', (run,)).fetchone()[0] == '已判定'


def test_review_decisions_never_rewrite_amount_evidence(tmp_path):
    db, run = judged(tmp_path)
    con = connect(db)
    aggregates = [tuple(r) for r in con.execute('SELECT * FROM stream_aggregate ORDER BY 1,2')]
    lines = [tuple(r) for r in con.execute('SELECT * FROM batch_line ORDER BY id')]
    verdicts = [tuple(r) for r in con.execute('SELECT * FROM verdict WHERE run_id=?', (run,))]
    freeze(db, run, 'risk')
    risk_votes(db, run)
    sign_review(db, run, 'risk-2', 'risk', 'object', '保留意见')
    with pytest.raises(ValueError, match='objection_pending'):
        close(db, run)
    # A review refusal is a review reason, never an amount difference.
    verdicts_after = [tuple(r) for r in connect(db).execute('SELECT * FROM verdict WHERE run_id=?', (run,))]
    assert verdicts_after == verdicts
    assert verdicts_after[0][2] == '可封账'
    assert [tuple(r) for r in connect(db).execute('SELECT * FROM stream_aggregate ORDER BY 1,2')] == aggregates
    assert [tuple(r) for r in connect(db).execute('SELECT * FROM batch_line ORDER BY id')] == lines


def test_identical_contract_replay_is_idempotent(tmp_path):
    db, run = judged(tmp_path)
    first = freeze_contract(db, run, 'standard', 'sub-1', '书面审核合同')
    replay = freeze_contract(db, run, 'standard', 'sub-1', '书面审核合同')
    assert replay == first
    with pytest.raises(ValueError, match='contract_conflict'):
        freeze_contract(db, run, 'standard', 'sub-1', '改写后的合同')
    assert connect(db).execute('SELECT COUNT(*) FROM review_contract').fetchone()[0] == 1


# --- objections, withdrawal, arbitration ----------------------------------

def test_objection_blocks_close_and_is_preserved(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    standard_votes(db, run)
    sign_review(db, run, 'risk-2', 'risk', 'object', '批次存在保留意见')
    status = review_status(db, run)
    assert status['authorized'] is False and status['reason'] == 'objection_pending'
    assert status['objections'][0]['opinion'] == '批次存在保留意见'
    with pytest.raises(ValueError, match='objection_pending'):
        close(db, run)


def test_only_original_objector_can_withdraw_and_history_is_kept(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    standard_votes(db, run)
    objection = sign_review(db, run, 'risk-2', 'risk', 'object', '保留意见')
    with pytest.raises(ValueError, match='withdraw_not_objector'):
        sign_review(db, run, 'risk-1', 'risk', 'withdraw', '代别人撤销', objection)
    withdrawal = sign_review(db, run, 'risk-2', 'risk', 'withdraw', '已补充说明，撤销异议', objection)
    assert withdrawal != objection
    status = review_status(db, run)
    assert status['authorized'] is True
    assert status['objections'][0]['resolution'] == 'withdrawn'
    assert status['objections'][0]['withdrawal']['signature_digest']
    close(db, run)
    # The objection and the withdrawal both remain immutable evidence.
    con = connect(db)
    decisions = [r[0] for r in con.execute(
        'SELECT decision FROM review_signature WHERE ref_id=? OR id=? ORDER BY id', (objection, objection))]
    assert decisions == ['object', 'withdraw']


def test_withdraw_requires_objector_role_and_upheld_objection_is_final(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    standard_votes(db, run)
    objection = sign_review(db, run, 'risk-2', 'risk', 'object', '异议')
    # A withdrawn objection can never be withdrawn again by someone else.
    sign_review(db, run, 'arb-1', 'arbitrator', 'dismiss', '驳回', objection)
    # Dismissal does not erase the standard-route obligation; the objector may
    # still withdraw their own objection, leaving both records intact.
    sign_review(db, run, 'risk-2', 'risk', 'withdraw', '尊重仲裁并撤回', objection)
    assert review_status(db, run)['authorized'] is True
    # An upheld objection is final: withdrawal cannot erase a substantiated risk.
    db2, run2 = judged(tmp_path / 'second')
    freeze(db2, run2, 'risk')
    risk_votes(db2, run2)
    objection2 = sign_review(db2, run2, 'risk-2', 'risk', 'object', '风险异议')
    sign_review(db2, run2, 'arb-1', 'arbitrator', 'uphold', '异议成立', objection2)
    with pytest.raises(ValueError, match='objection_upheld'):
        sign_review(db2, run2, 'risk-2', 'risk', 'withdraw', '事后撤销', objection2)
    with pytest.raises(ValueError, match='objection_upheld'):
        close(db2, run2)


def test_arbitration_must_be_independent(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run)
    standard_votes(db, run)
    objection = sign_review(db, run, 'risk-2', 'risk', 'object', '异议')
    # The objector cannot arbitrate their own objection.
    with pytest.raises(ValueError, match='role_not_registered'):
        sign_review(db, run, 'risk-2', 'arbitrator', 'dismiss', '自裁', objection)
    # A reviewer holding both a business role and arbitrator duty is not
    # independent enough to rule on the same contract.
    register_reviewer(db, 'arb-2', '仲裁辛', ['arbitrator', 'risk'])
    with pytest.raises(ValueError, match='arbitration_not_independent'):
        sign_review(db, run, 'arb-2', 'arbitrator', 'dismiss', '兼角色仲裁', objection)


# --- two written-contract routes ------------------------------------------

def test_standard_route_requires_two_distinct_roles(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'standard')
    sign_review(db, run, 'fin-1', 'finance', 'agree', '财务同意')
    assert review_status(db, run)['reason'] == 'route_requirements_not_met'
    sign_review(db, run, 'risk-1', 'risk', 'agree', '风险同意')
    status = review_status(db, run)
    assert status['authorized'] is True and status['route'] == 'standard'
    close(db, run)
    assert review_status(db, run)['run_status'] == '已归档'


def test_risk_route_requires_finance_and_risk(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'risk')
    sign_review(db, run, 'fin-1', 'finance', 'agree', '财务同意')
    sign_review(db, run, 'ops-1', 'operations', 'agree', '运营同意')
    assert review_status(db, run)['reason'] == 'route_requirements_not_met'
    sign_review(db, run, 'risk-1', 'risk', 'agree', '风险同意')
    assert review_status(db, run)['authorized'] is True


def test_standard_route_rejects_even_when_arbitration_dismisses(tmp_path):
    db, run, objection = split_case(tmp_path, 'standard')
    status = review_status(db, run)
    assert status['requirements']['satisfied'] is True
    assert status['authorized'] is False
    assert status['reason'] == 'objection_unresolved'
    assert status['objections'][0]['resolution'] == 'dismissed'
    with pytest.raises(ValueError, match='objection_unresolved'):
        close(db, run)
    # Only the objector's withdrawal unblocks the standard route.
    sign_review(db, run, 'risk-2', 'risk', 'withdraw', '撤回保留意见', objection)
    status = review_status(db, run)
    assert status['authorized'] is True
    assert [o['resolution'] for o in status['objections']] == ['withdrawn']
    close(db, run)


def test_risk_route_closes_after_independent_dismissal(tmp_path):
    db, run, objection = split_case(tmp_path, 'risk')
    status = review_status(db, run)
    assert status['authorized'] is True and status['route'] == 'risk'
    assert status['objections'][0]['resolution'] == 'dismissed'
    assert status['objections'][0]['ruling']['decision'] == 'dismiss'
    close(db, run)
    assert review_status(db, run)['run_status'] == '已归档'


def test_same_signatures_different_route_have_explainable_outcomes(tmp_path):
    """Identical signer set: standard refuses, risk authorizes, chains explain why."""
    outcomes = {}
    for route in ('standard', 'risk'):
        sub = tmp_path / route
        sub.mkdir()
        db, run, _ = split_case(sub, route)
        status = review_status(db, run)
        outcomes[route] = status
        assert status['requirements']['satisfied'] is True
        assert status['signatures'][-1]['role'] == 'arbitrator'
        assert status['route'] == route
        assert status['chain'][1]['route'] == route
        assert status['chain'][-1]['route_requirements']['satisfied'] is True
    assert outcomes['standard']['authorized'] is False
    assert outcomes['standard']['reason'] == 'objection_unresolved'
    assert outcomes['risk']['authorized'] is True
    assert outcomes['risk']['reason'] == 'authorized'


def test_risk_route_rejects_pending_or_upheld_objection(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'risk')
    risk_votes(db, run)
    objection = sign_review(db, run, 'risk-2', 'risk', 'object', '风险异议')
    assert review_status(db, run)['reason'] == 'objection_pending'
    with pytest.raises(ValueError, match='objection_pending'):
        close(db, run)
    sign_review(db, run, 'arb-1', 'arbitrator', 'uphold', '异议成立', objection)
    status = review_status(db, run)
    assert status['authorized'] is False and status['reason'] == 'objection_upheld'
    with pytest.raises(ValueError, match='objection_upheld'):
        close(db, run)
    assert close_untouched(db, run)


def close_untouched(db, run):
    return connect(db).execute('SELECT status FROM reconcile_run WHERE id=?', (run,)).fetchone()[0] == '已判定'


# --- evidence change invalidates signatures, history stays readable -------

def test_changed_evidence_does_not_migrate_signatures(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'standard')
    standard_votes(db, run)
    close(db, run)
    old_md = write(db, run, tmp_path / 'old').with_suffix('.md').read_text(encoding='utf-8')
    assert '可归档' in old_md

    assert review_status(db, run)['authorized'] is True
    con = connect(db)
    with con:
        con.execute("UPDATE rate_version SET rate='0.02'")
    # Signing against the changed evidence is refused, not silently rebound.
    with pytest.raises(ValueError, match='already_closed|review_not_open|source_changed'):
        sign_review(db, run, 'risk-1', 'risk', 'agree', '对新摘要签字')

    new_run, verdict = reconcile(db, 'D1')
    assert verdict['M1'] == '差异'
    status = review_status(db, run)
    assert status['authorized'] is False and status['reason'] == 'source_changed'
    assert status['signatures'] and all(s['status'] == 'invalidated' for s in status['signatures'])

    report = json.loads(write(db, run, tmp_path / 'stale').read_text(encoding='utf-8'))
    assert report['run']['effective_status'] == '已失效'
    assert report['authorization']['authorized'] is False
    assert report['authorization']['reason'] == 'source_changed'
    assert [s['opinion'] for s in report['authorization']['signatures']] == ['金额一致', '批次一致']
    assert all(s['status_label'] == '已失效' for s in report['authorization']['signatures'])

    # The new run has no migrated signatures and cannot inherit the old votes.
    new_status = review_status(db, new_run)
    assert new_status['signatures'] == [] and new_status['authorized'] is False
    with pytest.raises(ValueError, match='not_closable'):
        close(db, new_run)
    # Old signatures keep their original opinion and are bound to the old run.
    signatures = [dict(connect(db).execute(
        'SELECT run_id,evidence_digest FROM review_signature WHERE reviewer=?', (r,)).fetchone())
        for r in ('fin-1', 'ops-1')]
    assert [(s['run_id'], s['evidence_digest']) for s in signatures] == [
        (run, status['signatures'][0]['evidence_digest'])] * 2
    # The old report keeps both the signatures and the invalidation state.
    stale_md = (tmp_path / 'stale.md').read_text(encoding='utf-8')
    assert '金额一致' in stale_md and '已失效' in stale_md
    assert '不可归档（source_changed）' in stale_md


def test_old_signature_digest_is_bound_to_old_evidence(tmp_path):
    db, run = judged(tmp_path)
    freeze(db, run, 'standard')
    standard_votes(db, run)
    old = review_status(db, run)
    old_digest = old['source_digest']
    con = connect(db)
    with con:
        con.execute("UPDATE rate_version SET rate='0.02'")
    new_run, _ = reconcile(db, 'D1')
    assert new_run != run
    # A presenter offering the old digest is refused outright.
    with pytest.raises(ValueError, match='stale_source_digest'):
        sign_review(db, new_run, 'fin-1', 'finance', 'agree', '拿旧摘要签新运行', source_digest=old_digest)
    # Signing on the new run needs its own submission and contract; old votes
    # are never copied across.
    with pytest.raises(ValueError, match='not_closable'):
        close(db, new_run)
    register_reviewer(db, 'fin-1', '财务审核乙', ['finance'])
    with pytest.raises(ValueError, match='review_not_open'):
        sign_review(db, new_run, 'fin-1', 'finance', 'agree', '新摘要签字')
    assert old['signatures'][0]['source_digest'] == old['source_digest']
    assert old['signatures'][0]['evidence_digest'] != review_status(db, new_run)['evidence_digest']


# --- migration, CLI surface, and demos ------------------------------------

def test_migrate_legacy_database_keeps_evidence(tmp_path):
    db_path = tmp_path / 'legacy.db'
    legacy = sqlite3.connect(db_path)
    with legacy:
        legacy.executescript("""
        CREATE TABLE reconcile_run(id INTEGER PRIMARY KEY,window_id TEXT,status TEXT,source_digest TEXT,
          started_at TEXT DEFAULT CURRENT_TIMESTAMP,ended_at TEXT);
        CREATE TABLE verdict(run_id INTEGER,merchant TEXT,status TEXT,stream_net TEXT,batch_net TEXT,reason TEXT);
        INSERT INTO reconcile_run(id,window_id,status,source_digest) VALUES (1,'D1','已判定','abc');
        INSERT INTO verdict VALUES (1,'M','可封账','99.00','99.00','matched');
        """)
    legacy.close()
    db = connect(db_path)
    assert db.execute('PRAGMA user_version').fetchone()[0] == 2
    assert tuple(db.execute('SELECT merchant,status FROM verdict').fetchone()) == ('M', '可封账')
    assert db.execute('SELECT COUNT(*) FROM review_signature').fetchone()[0] == 0
    assert db.execute('SELECT COUNT(*) FROM reconcile_submission').fetchone()[0] == 0
    # Running the migration twice is safe and non-destructive.
    assert migrate(db) == 2
    db.close()


def test_review_cli_registers_and_signs(tmp_path, capsys):
    from dualsettle.__main__ import main
    import sys
    db, run = judged(tmp_path)
    db = str(db)
    argv = sys.argv
    try:
        sys.argv = ['dualsettle', 'review-list', '--db', db]
        main()
        assert 'fin-1' in capsys.readouterr().out
        sys.argv = ['dualsettle', 'review-contract', '--db', db, '--run', str(run),
                    '--route', 'standard', '--submitter', 'sub-1', '--terms', '书面合同']
        main()
        sys.argv = ['dualsettle', 'review-sign', '--db', db, '--run', str(run),
                    '--reviewer', 'fin-1', '--role', 'finance', '--decision', 'agree', '--opinion', '同意']
        main()
        sys.argv = ['dualsettle', 'review-status', '--db', db, '--run', str(run)]
        main()
        out = capsys.readouterr().out
        assert '"authorized": false' in out and 'route_requirements_not_met' in out
        sys.argv = ['dualsettle', 'close', '--db', db, '--run', str(run)]
        with pytest.raises(ValueError, match='route_requirements_not_met'):
            main()
    finally:
        sys.argv = argv


def test_demo_review_uses_risk_route(tmp_path):
    from dualsettle.__main__ import review_demo
    result = review_demo(tmp_path)
    assert result['route'] == 'risk'
    assert result['authorized_before_close'] is True
    assert result['archived_status'] == '已归档'


def test_reviewer_registry_is_idempotent(tmp_path):
    db, _ = judged(tmp_path)
    assert register_reviewer(db, 'fin-1', '财务审核乙', ['finance']) == 'fin-1'
    with pytest.raises(ValueError, match='reviewer_conflict'):
        register_reviewer(db, 'fin-1', '改名', ['finance'])
    assert [r['id'] for r in list_reviewers(db)] == sorted(r[0] for r in REVIEWERS)
