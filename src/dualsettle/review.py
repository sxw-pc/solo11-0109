"""Offline review ledger for close authorization.

A reconciliation run may only be archived after the frozen written review
contract has received the signatures it requires.  Everything here is local,
deterministic bookkeeping over fictional reviewer ids and local key digests;
there is no identity service, network call, or real signature.

The ledger is append-only.  Signatures are never updated or deleted: an
objection is withdrawn by a further signature from the original objector, and
arbitration is a further signature from an independent arbitrator.  Historical
rows therefore remain available to old reports.
"""
import hashlib
import json
from datetime import datetime, timezone

from .store import connect

BUSINESS_ROLES = ('finance', 'operations', 'risk')
KNOWN_ROLES = BUSINESS_ROLES + ('arbitrator',)
ROUTES = ('standard', 'risk')
DECISIONS = ('agree', 'object', 'withdraw', 'dismiss', 'uphold')

ROUTE_LABELS = {'standard': '常规路线', 'risk': '风险复核路线'}
DECISION_LABELS = {
    'agree': '同意',
    'object': '异议',
    'withdraw': '撤销异议',
    'dismiss': '驳回异议',
    'uphold': '裁定异议成立',
}
ROLE_LABELS = {
    'finance': '财务审核人',
    'operations': '运营审核人',
    'risk': '风险审核人',
    'arbitrator': '独立仲裁人',
}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _digest(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _local_key(reviewer_id, name):
    """A local-only key digest; it is not a real credential or identity proof."""
    return hashlib.sha256(f'dualsettle-local-reviewer:{reviewer_id}:{name}'.encode('utf-8')).hexdigest()


def _roles(value):
    if isinstance(value, str):
        value = value.split(',')
    return tuple(sorted({str(x).strip() for x in value if str(x).strip()}))


def _current_digest(db):
    # Imported lazily: rules.close imports this module to evaluate authorization.
    from .rules import input_digest
    return input_digest(db)


def _run(db, run_id):
    return db.execute('SELECT * FROM reconcile_run WHERE id=?', (int(run_id),)).fetchone()


def _contract(db, run_id):
    return db.execute('SELECT * FROM review_contract WHERE run_id=?', (int(run_id),)).fetchone()


def _submission(db, run_id):
    return db.execute('SELECT * FROM reconcile_submission WHERE run_id=?', (int(run_id),)).fetchone()


def evidence_digest(db, run_id):
    """Digest of the frozen reconciliation evidence a signature is bound to.

    It deliberately uses only rows created for this run and the run's frozen
    source digest, so later amount-evidence changes cannot restyle an old
    signature as if it covered the new evidence.
    """
    run = _run(db, run_id)
    if not run:
        raise ValueError('unknown_run')
    evidence = [tuple(r) for r in db.execute(
        'SELECT merchant,event_id,batch_id,rate_id,detail FROM evidence WHERE run_id=? ORDER BY id',
        (run['id'],))]
    verdicts = [tuple(r) for r in db.execute(
        'SELECT merchant,status,stream_net,batch_net,reason FROM verdict WHERE run_id=? ORDER BY merchant',
        (run['id'],))]
    return _digest({'run_id':run['id'], 'source_digest':run['source_digest'],
                    'evidence':evidence, 'verdicts':verdicts})


def register_reviewer(path, reviewer_id, name, roles, active=True):
    """Register a fictional reviewer and the roles they may sign in."""
    reviewer_id = str(reviewer_id).strip()
    name = str(name).strip()
    roles = _roles(roles)
    if not reviewer_id or not name or not roles:
        raise ValueError('invalid_reviewer')
    unknown = set(roles) - set(KNOWN_ROLES)
    if unknown:
        raise ValueError('unknown_role')
    db = connect(path)
    try:
        with db:
            old = db.execute('SELECT * FROM reviewer WHERE id=?', (reviewer_id,)).fetchone()
            value = (reviewer_id, name, ','.join(roles), 1 if active else 0)
            if old:
                if tuple(old[k] for k in ('id', 'name', 'roles', 'active')) != value:
                    raise ValueError('reviewer_conflict')
                return reviewer_id
            db.execute('INSERT INTO reviewer(id,name,roles,active,local_key,created_at) VALUES (?,?,?,?,?,?)',
                       value + (_local_key(reviewer_id, name), _now()))
            return reviewer_id
    finally:
        db.close()


def list_reviewers(path):
    db = connect(path)
    try:
        return [dict(r) for r in db.execute('SELECT id,name,roles,active,created_at FROM reviewer ORDER BY id')]
    finally:
        db.close()


def submit_reconciliation(path, run_id, reviewer_id):
    """Record who submitted this judged reconciliation for review."""
    db = connect(path)
    try:
        with db:
            run = _run(db, run_id)
            if not run:
                raise ValueError('unknown_run')
            if run['status'] == '已归档':
                raise ValueError('already_closed')
            if run['status'] != '已判定':
                raise ValueError('review_not_open')
            if _current_digest(db) != run['source_digest']:
                raise ValueError('source_changed')
            reviewer = db.execute('SELECT * FROM reviewer WHERE id=?', (reviewer_id,)).fetchone()
            if not reviewer or not reviewer['active']:
                raise ValueError('reviewer_unknown')
            old = _submission(db, run_id)
            if old:
                if old['submitter'] == reviewer_id and old['source_digest'] == run['source_digest']:
                    return run_id
                raise ValueError('submission_conflict')
            content = {'run_id':run['id'], 'submitter':reviewer_id, 'source_digest':run['source_digest']}
            content_digest = _digest(content)
            signature_digest = _digest([content_digest, reviewer['local_key']])
            db.execute('INSERT INTO reconcile_submission(run_id,submitter,source_digest,submitted_at,signature_digest) '
                       'VALUES (?,?,?,?,?)',
                       (run['id'], reviewer_id, run['source_digest'], _now(), signature_digest))
            return run_id
    finally:
        db.close()


def freeze_contract(path, run_id, route, submitter, terms):
    """Freeze the written review contract for a run.

    The route is fixed here and cannot be switched after votes are collected.
    Replaying the identical contract is idempotent; any changed term or route
    is rejected.
    """
    route = str(route).strip()
    submitter = str(submitter).strip()
    terms = str(terms).strip()
    if route not in ROUTES:
        raise ValueError('invalid_route')
    if not submitter or not terms:
        raise ValueError('invalid_contract')
    db = connect(path)
    try:
        with db:
            run = _run(db, run_id)
            if not run:
                raise ValueError('unknown_run')
            if run['status'] == '已归档':
                raise ValueError('already_closed')
            if run['status'] != '已判定':
                raise ValueError('review_not_open')
            if _current_digest(db) != run['source_digest']:
                raise ValueError('source_changed')
            submission = _submission(db, run_id)
            if not submission:
                raise ValueError('submission_required')
            if submission['submitter'] != submitter:
                raise ValueError('submission_conflict')
            if submission['source_digest'] != run['source_digest']:
                raise ValueError('submission_stale')
            old = _contract(db, run_id)
            if old:
                if (old['route'], old['submitter'], old['terms']) == (route, submitter, terms):
                    return old['id']
                raise ValueError('route_locked' if old['route'] != route else 'contract_conflict')
            payload = {'run_id':run['id'], 'route':route, 'submitter':submitter,
                       'terms':terms, 'source_digest':run['source_digest']}
            contract_digest = _digest(payload)
            return db.execute(
                'INSERT INTO review_contract(run_id,route,submitter,terms,source_digest,created_at,contract_digest) '
                'VALUES (?,?,?,?,?,?,?)',
                (run['id'], route, submitter, terms, run['source_digest'], _now(), contract_digest)).lastrowid
    finally:
        db.close()


def _signature_rows(db, contract_id):
    return db.execute(
        'SELECT s.*,r.name AS reviewer_name FROM review_signature s '
        'JOIN reviewer r ON r.id=s.reviewer WHERE s.contract_id=? ORDER BY s.id',
        (contract_id,)).fetchall()


def _objection_status(db, objection_id):
    """Return the resolution of an objection from later signatures only."""
    rows = db.execute(
        "SELECT s.*,r.name AS reviewer_name FROM review_signature s JOIN reviewer r ON r.id=s.reviewer "
        "WHERE s.ref_id=? AND s.decision IN ('withdraw','dismiss','uphold') ORDER BY s.id",
        (objection_id,)).fetchall()
    withdrawal = next((r for r in rows if r['decision'] == 'withdraw'), None)
    ruling = None
    for row in rows:
        if row['decision'] in ('dismiss', 'uphold'):
            ruling = row
    return withdrawal, ruling


def _signature_status(sig, fresh):
    if not fresh:
        return 'invalidated', '已失效'
    if sig['decision'] == 'agree':
        return 'valid', '有效'
    if sig['decision'] == 'object':
        return 'pending', '待裁定'
    return 'recorded', '已记录'


def _check_no_prior_vote(db, contract_id, reviewer_id, role, content_digest):
    """One reviewer cannot hold two approval/objection seats on one contract."""
    rows = db.execute(
        "SELECT * FROM review_signature WHERE contract_id=? AND reviewer=? AND decision IN ('agree','object')",
        (contract_id, reviewer_id)).fetchall()
    for row in rows:
        if row['role'] != role:
            raise ValueError('dual_role_signature')
        raise ValueError('opinion_rewrite')
    if not rows:
        arbitration = db.execute(
            "SELECT * FROM review_signature WHERE contract_id=? AND reviewer=? AND decision IN ('dismiss','uphold')",
            (contract_id, reviewer_id)).fetchall()
        for row in arbitration:
            if row['content_digest'] == content_digest:
                return row['id']
            raise ValueError('dual_role_signature')
    return None


def sign_review(path, run_id, reviewer_id, role, decision, opinion, ref=None, source_digest=None):
    """Append one opinion (or a withdrawal/arbitration) to the ledger.

    Identical replays return the existing signature id.  A changed opinion, a
    second role seat, or a signature bound to an old evidence digest is
    rejected rather than overwriting the historical record.  When the caller
    presents an explicit ``source_digest`` it must match the run being signed;
    a signature offered against an older digest is refused.
    """
    role = str(role).strip()
    decision = str(decision).strip()
    opinion = str(opinion).strip()
    if decision not in DECISIONS:
        raise ValueError('invalid_decision')
    if role not in KNOWN_ROLES:
        raise ValueError('unknown_role')
    if not opinion:
        raise ValueError('invalid_opinion')
    ref_id = None if ref in (None, '') else int(ref)

    db = connect(path)
    try:
        with db:
            run = _run(db, run_id)
            if not run:
                raise ValueError('unknown_run')
            if source_digest is not None and str(source_digest) != run['source_digest']:
                raise ValueError('stale_source_digest')
            if run['status'] == '已归档':
                raise ValueError('already_closed')
            if run['status'] != '已判定':
                raise ValueError('review_not_open')
            if _current_digest(db) != run['source_digest']:
                raise ValueError('source_changed')
            contract = _contract(db, run_id)
            if not contract:
                raise ValueError('contract_required')
            if contract['source_digest'] != run['source_digest']:
                raise ValueError('contract_stale')
            reviewer = db.execute('SELECT * FROM reviewer WHERE id=?', (reviewer_id,)).fetchone()
            if not reviewer or not reviewer['active']:
                raise ValueError('reviewer_unknown')
            if role not in _roles(reviewer['roles']):
                raise ValueError('role_not_registered')
            submission = _submission(db, run_id)
            if not submission or submission['submitter'] != contract['submitter']:
                raise ValueError('submission_required')

            if decision in ('agree', 'object'):
                if role == 'arbitrator':
                    raise ValueError('invalid_role_for_vote')
                if decision == 'agree' and reviewer_id == contract['submitter']:
                    raise ValueError('submitter_cannot_approve')

            content = {
                'contract_id':contract['id'], 'run_id':run['id'],
                'source_digest':run['source_digest'],
                'evidence_digest':evidence_digest(db, run['id']),
                'reviewer':reviewer_id, 'role':role,
                'decision':decision, 'opinion':opinion, 'ref':ref_id,
            }
            content_digest = _digest(content)
            replay = db.execute('SELECT id FROM review_signature WHERE content_digest=?',
                                (content_digest,)).fetchone()
            if replay:
                return replay['id']

            if decision in ('agree', 'object'):
                prior = _check_no_prior_vote(db, contract['id'], reviewer_id, role, content_digest)
                if prior:
                    return prior
            elif decision == 'withdraw':
                if not ref_id:
                    raise ValueError('objection_required')
                objection = db.execute(
                    "SELECT * FROM review_signature WHERE id=? AND contract_id=? AND decision='object'",
                    (ref_id, contract['id'])).fetchone()
                if not objection:
                    raise ValueError('unknown_objection')
                if objection['reviewer'] != reviewer_id:
                    raise ValueError('withdraw_not_objector')
                if objection['role'] != role:
                    raise ValueError('withdraw_role_mismatch')
                withdrawal, ruling = _objection_status(db, objection['id'])
                if withdrawal:
                    if withdrawal['content_digest'] == content_digest:
                        return withdrawal['id']
                    raise ValueError('opinion_rewrite')
                if ruling and ruling['decision'] == 'uphold':
                    # A substantiated objection cannot be undone by withdrawal;
                    # it must be addressed rather than erased.
                    raise ValueError('objection_upheld')
            elif decision in ('dismiss', 'uphold'):
                if role != 'arbitrator':
                    raise ValueError('arbitrator_role_required')
                if not ref_id:
                    raise ValueError('objection_required')
                objection = db.execute(
                    "SELECT * FROM review_signature WHERE id=? AND contract_id=? AND decision='object'",
                    (ref_id, contract['id'])).fetchone()
                if not objection:
                    raise ValueError('unknown_objection')
                if reviewer_id in (contract['submitter'], objection['reviewer']):
                    raise ValueError('arbitration_not_independent')
                if role not in ('arbitrator',) or _roles(reviewer['roles']) != ('arbitrator',):
                    raise ValueError('arbitration_not_independent')
                participated = db.execute(
                    "SELECT 1 FROM review_signature WHERE contract_id=? AND reviewer=? "
                    "AND decision IN ('agree','object') LIMIT 1",
                    (contract['id'], reviewer_id)).fetchone()
                if participated:
                    raise ValueError('arbitration_not_independent')
                withdrawal, ruling = _objection_status(db, objection['id'])
                if withdrawal:
                    raise ValueError('objection_withdrawn')
                if ruling:
                    if ruling['content_digest'] == content_digest:
                        return ruling['id']
                    raise ValueError('arbitration_conflict')

            signed_at = _now()
            signature_digest = _digest([content_digest, signed_at, reviewer['local_key']])
            cur = db.execute(
                'INSERT INTO review_signature(contract_id,run_id,source_digest,evidence_digest,reviewer,role,'
                'decision,opinion,ref_id,signed_at,content_digest,signature_digest) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                (contract['id'], run['id'], run['source_digest'], content['evidence_digest'],
                 reviewer_id, role, decision, opinion, ref_id, signed_at, content_digest, signature_digest))
            return cur.lastrowid
    finally:
        db.close()


def _objection_view(db, obj, fresh):
    withdrawal, ruling = _objection_status(db, obj['id'])
    if not fresh:
        resolution, label = 'invalidated', '已失效'
    elif withdrawal:
        resolution, label = 'withdrawn', '已撤销'
    elif ruling and ruling['decision'] == 'dismiss':
        resolution, label = 'dismissed', '仲裁已驳回'
    elif ruling and ruling['decision'] == 'uphold':
        resolution, label = 'upheld', '仲裁裁定成立'
    else:
        resolution, label = 'pending', '待裁定'
    return {
        'signature_id':obj['id'], 'reviewer':obj['reviewer'],
        'reviewer_name':obj['reviewer_name'], 'role':obj['role'],
        'role_label':ROLE_LABELS.get(obj['role'], obj['role']),
        'opinion':obj['opinion'], 'signed_at':obj['signed_at'],
        'source_digest':obj['source_digest'], 'evidence_digest':obj['evidence_digest'],
        'signature_digest':obj['signature_digest'],
        'resolution':resolution, 'resolution_label':label,
        'withdrawal': None if not withdrawal else {
            'signature_id':withdrawal['id'], 'reviewer':withdrawal['reviewer'],
            'reviewer_name':withdrawal['reviewer_name'], 'role':withdrawal['role'],
            'opinion':withdrawal['opinion'], 'signed_at':withdrawal['signed_at'],
            'signature_digest':withdrawal['signature_digest'],
        },
        'ruling': None if not ruling else {
            'signature_id':ruling['id'], 'reviewer':ruling['reviewer'],
            'reviewer_name':ruling['reviewer_name'], 'role':ruling['role'],
            'decision':ruling['decision'], 'decision_label':DECISION_LABELS[ruling['decision']],
            'opinion':ruling['opinion'], 'signed_at':ruling['signed_at'],
            'signature_digest':ruling['signature_digest'],
        },
    }


def authorization_for_run(db, run_id):
    """Explain whether the frozen evidence is authorized to close.

    The result is derived only from the append-only review ledger.  It never
    changes stream aggregates, batch lines, or verdicts, and a refusal is
    reported as a review reason rather than an amount difference.
    """
    run = _run(db, run_id)
    if not run:
        return {'authorized':False, 'reason':'unknown_run', 'run_id':None}
    fresh = _current_digest(db) == run['source_digest']
    contract = _contract(db, run['id'])
    submission = _submission(db, run['id'])
    signatures = _signature_rows(db, contract['id']) if contract else []
    evidence = None
    try:
        evidence = evidence_digest(db, run['id'])
    except ValueError:
        pass

    signature_views = []
    objections = []
    objection_by_signature = {}
    for sig in signatures:
        if sig['decision'] == 'object':
            view = _objection_view(db, sig, fresh)
            objections.append(view)
            objection_by_signature[sig['id']] = view
    for sig in signatures:
        if sig['decision'] == 'object':
            view_obj = objection_by_signature[sig['id']]
            status, label = view_obj['resolution'], view_obj['resolution_label']
        else:
            status, label = _signature_status(sig, fresh)
        signature_views.append({
            'signature_id':sig['id'], 'reviewer':sig['reviewer'],
            'reviewer_name':sig['reviewer_name'], 'role':sig['role'],
            'role_label':ROLE_LABELS.get(sig['role'], sig['role']),
            'decision':sig['decision'], 'decision_label':DECISION_LABELS.get(sig['decision'], sig['decision']),
            'opinion':sig['opinion'], 'ref_id':sig['ref_id'], 'signed_at':sig['signed_at'],
            'source_digest':sig['source_digest'], 'evidence_digest':sig['evidence_digest'],
            'content_digest':sig['content_digest'], 'signature_digest':sig['signature_digest'],
            'status':status, 'status_label':label,
            'counts_toward_route':sig['decision'] == 'agree' and status == 'valid' and contract is not None
                                  and sig['reviewer'] != contract['submitter'],
        })

    agree = [s for s in signature_views if s['counts_toward_route']]
    agree_roles = {s['role'] for s in agree}
    # A distinct person must hold each agreeing seat.  Two seats by one
    # reviewer can never satisfy a two-person route even if the WRITE path were
    # bypassed, so authorization re-checks it from the ledger.
    agree_reviewers = [s['reviewer'] for s in agree]
    distinct_people = len(set(agree_reviewers)) == len(agree_reviewers)
    submitter_conflict = bool(contract and any(
        s['decision'] == 'agree' and s['reviewer'] == contract['submitter'] for s in signature_views))
    submission_conflict = bool(contract and (
        not submission or submission['submitter'] != contract['submitter']))

    route = contract['route'] if contract else None
    if route == 'standard':
        route_ok = len(agree_roles) >= 2
        required = {'min_distinct_roles':2, 'present_roles':sorted(agree_roles), 'satisfied':route_ok}
    elif route == 'risk':
        route_ok = {'finance', 'risk'}.issubset(agree_roles)
        required = {'required_roles':['finance', 'risk'], 'present_roles':sorted(agree_roles), 'satisfied':route_ok}
    else:
        route_ok = False
        required = {'satisfied':False}

    blocking = []
    for obj in objections:
        if not fresh:
            continue
        if route == 'standard':
            # Arbitration does not override an objection on the standard route;
            # the original objector must withdraw it.
            if obj['resolution'] != 'withdrawn':
                blocking.append(obj)
        elif route == 'risk':
            if obj['resolution'] in ('pending', 'upheld'):
                blocking.append(obj)
        else:
            blocking.append(obj)

    reason = 'authorized'
    if not fresh or run['status'] == '已失效':
        reason = 'source_changed'
    elif run['status'] not in ('已判定', '已归档'):
        reason = 'review_not_open'
    elif not contract:
        reason = 'review_required'
    elif contract['source_digest'] != run['source_digest']:
        reason = 'contract_stale'
    elif submission_conflict or submitter_conflict:
        reason = 'separation_violation'
    elif not distinct_people:
        reason = 'separation_violation'
    elif any(s['source_digest'] != run['source_digest'] for s in signature_views):
        reason = 'signature_stale'
    elif any(s['evidence_digest'] != evidence for s in signature_views):
        reason = 'signature_stale'
    elif blocking:
        if route == 'risk' and any(o['resolution'] == 'upheld' for o in blocking):
            reason = 'objection_upheld'
        elif route == 'standard' and any(o['resolution'] == 'dismissed' for o in blocking):
            reason = 'objection_unresolved'
        else:
            reason = 'objection_pending'
    elif not route_ok:
        reason = 'route_requirements_not_met'

    authorized = reason == 'authorized'
    blocking_reasons = []
    if blocking:
        blocking_reasons = [o['opinion'] for o in blocking]
    if not route_ok and fresh:
        blocking_reasons.append('route_requirements_not_met')

    return {
        'run_id':run['id'],
        'run_status':run['status'],
        'effective_status':run['status'] if fresh else '已失效',
        'source_digest':run['source_digest'],
        'evidence_digest':evidence,
        'fresh':fresh,
        'route':route,
        'route_label':ROUTE_LABELS.get(route, route),
        'contract_id':contract['id'] if contract else None,
        'contract_digest':contract['contract_digest'] if contract else None,
        'contract_terms':contract['terms'] if contract else None,
        'contract_source_digest':contract['source_digest'] if contract else None,
        'submitter':contract['submitter'] if contract else (submission['submitter'] if submission else None),
        'submission':None if not submission else {
            'submitter':submission['submitter'],
            'source_digest':submission['source_digest'],
            'submitted_at':submission['submitted_at'],
            'signature_digest':submission['signature_digest'],
        },
        'requirements':required,
        'authorized':authorized,
        'reason':reason,
        'blocking_reasons':blocking_reasons,
        'signatures':signature_views,
        'objections':objections,
        'chain':[
            {'step':'提交核对', 'reviewer':submission['submitter'] if submission else None,
             'at':submission['submitted_at'] if submission else None},
            {'step':'冻结审核合同', 'route':route, 'route_label':ROUTE_LABELS.get(route, route),
             'contract_digest':contract['contract_digest'] if contract else None,
             'at':contract['created_at'] if contract else None},
            {'step':'同意席位', 'seats':[
                {'reviewer':s['reviewer'], 'reviewer_name':s['reviewer_name'],
                 'role':s['role'], 'role_label':s['role_label'],
                 'signature_id':s['signature_id'], 'valid':s['status'] == 'valid'}
                for s in agree]},
            {'step':'异议处理', 'objections':[
                {'signature_id':o['signature_id'], 'reviewer':o['reviewer'],
                 'resolution':o['resolution'], 'resolution_label':o['resolution_label']}
                for o in objections]},
            {'step':'授权结论', 'authorized':authorized, 'reason':reason,
             'route_requirements':required},
        ],
    }


def review_status(path, run_id):
    db = connect(path)
    try:
        result = authorization_for_run(db, run_id)
        if result['run_id'] is None:
            raise ValueError('unknown_run')
        return result
    finally:
        db.close()
