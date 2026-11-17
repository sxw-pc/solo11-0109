"""Freeze a synthetic closed-window merchant activity comparison case."""
import hashlib
import json
from pathlib import Path

from .store import connect


def register(db_path, case_id, fixture):
    if not case_id:
        raise ValueError('missing_volume_case_id')
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    if (doc.get('baseline_window') == doc.get('target_window') or
            doc.get('baseline_days', 0) <= 0 or doc.get('target_days', 0) <= 0 or
            not doc.get('focus_merchant')):
        raise ValueError('bad_volume_contract')
    db = connect(db_path)
    try:
        with db:
            runs = {}
            for label in ('baseline', 'target'):
                row = db.execute(
                    "SELECT id,source_digest FROM reconcile_run WHERE window_id=? AND status='已归档' ORDER BY id DESC LIMIT 1",
                    (doc[f'{label}_window'],)).fetchone()
                if not row:
                    raise ValueError('closed_window_required')
                runs[label] = dict(row)
            for label in ('baseline', 'target'):
                verdict = db.execute('SELECT status FROM verdict WHERE run_id=? AND merchant=?',
                                     (runs[label]['id'], doc['focus_merchant'])).fetchone()
                if not verdict or verdict['status'] != '可封账':
                    raise ValueError('matched_merchant_required')
            digest = hashlib.sha256(raw).hexdigest()
            payload = json.dumps({'digest': digest, 'fixture': str(source),
                                  'doc': doc, 'runs': runs},
                                 sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT payload FROM volume_case WHERE id=?',
                             (case_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('volume_case_conflict')
            db.execute('INSERT OR IGNORE INTO volume_case VALUES (?,?)',
                       (case_id, payload))
    finally:
        db.close()
    return {'case_id': case_id, 'digest': digest,
            'baseline_run': runs['baseline']['id'], 'target_run': runs['target']['id']}
