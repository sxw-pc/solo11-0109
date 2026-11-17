"""Freeze a synthetic merchant fee-tax invoice contract for a closed run."""
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .store import connect


def register(db_path, contract_id, merchant, fixture):
    if not contract_id or not merchant:
        raise ValueError('missing_tax_identity')
    source = Path(fixture).resolve()
    raw = source.read_bytes()
    doc = json.loads(raw)
    try:
        rate = Decimal(doc['tax_rate'])
    except (KeyError, InvalidOperation, TypeError):
        raise ValueError('bad_tax_contract') from None
    if (not rate.is_finite() or rate < 0 or rate >= 1 or
            not doc.get('seller') or not doc.get('buyer_tax_id') or
            set(doc.get('invoice_basis', [])) != {'per_transaction', 'daily_consolidated'}):
        raise ValueError('bad_tax_contract')
    db = connect(db_path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM account WHERE id=?',
                              (merchant,)).fetchone():
                raise ValueError('unknown_tax_merchant')
            digest = hashlib.sha256(raw).hexdigest()
            payload = json.dumps({'digest': digest, 'fixture': str(source),
                                  'doc': doc}, sort_keys=True, separators=(',', ':'))
            old = db.execute('SELECT merchant,payload FROM tax_contract WHERE id=?',
                             (contract_id,)).fetchone()
            if old and (old['merchant'] != merchant or old['payload'] != payload):
                raise ValueError('tax_contract_conflict')
            db.execute('INSERT OR IGNORE INTO tax_contract VALUES (?,?,?)',
                       (contract_id, merchant, payload))
    finally:
        db.close()
    return {'contract_id': contract_id, 'merchant': merchant, 'digest': digest}
