"""Freeze one synthetic transaction cohort for later refund-lag analysis."""
import hashlib
import json
from pathlib import Path

from .store import connect
from .timeline import at, transactions


def register(db_path, case_id, fixture):
    if not case_id:
        raise ValueError('missing_cohort_case_id')
    path = Path(fixture).resolve()
    raw = path.read_bytes()
    doc = json.loads(raw)
    if (not doc.get('merchant') or not doc.get('transactions') or
            not doc.get('horizons_days')):
        raise ValueError('bad_cohort_contract')
    as_of = at(doc['as_of'])
    if any(not isinstance(h, int) or h <= 0 for h in doc['horizons_days']):
        raise ValueError('bad_cohort_horizon')
    db = connect(db_path)
    try:
        with db:
            selected = list(db.execute('SELECT * FROM transaction_event WHERE merchant=? ORDER BY tx,version',
                                       (doc['merchant'],)))
            by_tx = {tx: (items,confirmed,refunded,valid)
                     for (merchant,tx),items,confirmed,refunded,valid
                     in transactions([dict(r) for r in selected])}
            point_hashes = []
            for tx in doc['transactions']:
                if tx not in by_tx:
                    raise ValueError('missing_cohort_transaction')
                items,confirmed,refunded,valid = by_tx[tx]
                if not valid or confirmed <= 0 or any(at(e['business_at']) > as_of for e in items):
                    raise ValueError('invalid_cohort_transaction')
                point_hashes.extend(e['sha'] for e in items)
            payload = json.dumps({'fixture':str(path),
                                  'digest':hashlib.sha256(raw).hexdigest(),
                                  'doc':doc,'event_hashes':point_hashes},
                                 sort_keys=True,separators=(',',':'))
            old = db.execute('SELECT payload FROM refund_cohort_case WHERE id=?',
                             (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('cohort_case_conflict')
            db.execute('INSERT OR IGNORE INTO refund_cohort_case VALUES (?,?)',
                       (case_id,payload))
    finally:
        db.close()
    return {'case_id':case_id,'digest':hashlib.sha256(raw).hexdigest(),
            'transaction_count':len(doc['transactions'])}
