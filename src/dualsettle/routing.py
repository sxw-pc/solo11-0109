"""Freeze synthetic next-cycle orders and gateway quotes; no route assignment yet."""
import hashlib
import json
from pathlib import Path

from .contract import aware
from .store import connect


def register(db_path, batch_id, source_path):
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError('bad_routing_batch_id')
    raw = Path(source_path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    body = json.loads(raw)
    if not isinstance(body, dict) or not isinstance(body.get('cycle_id'), str) \
            or not body['cycle_id'] or not isinstance(body.get('orders'), list) \
            or not isinstance(body.get('gateways'), list) or not body['orders'] or not body['gateways']:
        raise ValueError('bad_routing_batch')
    orders = []
    order_ids = set()
    for item in body['orders']:
        tx, merchant, amount, submitted = (item[k] for k in
            ('tx_id', 'merchant', 'amount_cents', 'submitted_at'))
        if not isinstance(tx, str) or not tx or tx in order_ids \
                or not isinstance(merchant, str) or not merchant \
                or type(amount) is not int or amount <= 0:
            raise ValueError('bad_routing_order')
        aware(submitted)
        orders.append((batch_id, tx, merchant, amount, submitted))
        order_ids.add(tx)
    gateways = []
    gateway_ids = set()
    for item in body['gateways']:
        gateway, capacity, activation, rates, latency = (item[k] for k in
            ('gateway_id', 'capacity', 'activation_cents', 'rate_bps_by_unit', 'latency_hours'))
        if not isinstance(gateway, str) or not gateway or gateway in gateway_ids \
                or type(capacity) is not int or capacity <= 0 \
                or type(activation) is not int or activation < 0 \
                or not isinstance(rates, list) or len(rates) != capacity \
                or any(type(rate) is not int or rate < 0 for rate in rates) \
                or type(latency) is not int or latency < 0:
            raise ValueError('bad_routing_gateway')
        gateways.append((batch_id, gateway, capacity, activation,
                         json.dumps(rates), latency))
        gateway_ids.add(gateway)
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT source_hash FROM routing_batch WHERE id=?',
                             (batch_id,)).fetchone()
            if old:
                if old['source_hash'] != digest:
                    raise ValueError('routing_batch_identity_conflict')
                return digest
            for _, _, merchant, _, _ in orders:
                if not db.execute('SELECT 1 FROM account WHERE id=?', (merchant,)).fetchone():
                    raise ValueError('unknown_routing_merchant')
            db.execute('INSERT INTO routing_batch VALUES (?,?,?,?)',
                       (batch_id, body['cycle_id'], digest, raw))
            db.executemany('INSERT INTO routing_order VALUES (?,?,?,?,?)', orders)
            db.executemany('INSERT INTO routing_gateway VALUES (?,?,?,?,?,?)', gateways)
            return digest
    finally:
        db.close()


def summary(db_path, batch_id):
    """Raw demand and quote counts; no assignment or price optimization."""
    db = connect(db_path)
    try:
        row = db.execute('SELECT cycle_id,source_hash FROM routing_batch WHERE id=?',
                         (batch_id,)).fetchone()
        if not row:
            raise ValueError('unknown_routing_batch')
        orders = db.execute('SELECT count(*) n,sum(amount_cents) cents FROM routing_order '
                            'WHERE batch_id=?', (batch_id,)).fetchone()
        gateways = db.execute('SELECT count(*) n FROM routing_gateway WHERE batch_id=?',
                              (batch_id,)).fetchone()['n']
        return {'cycle_id': row['cycle_id'], 'orders': orders['n'],
                'amount_cents': orders['cents'], 'gateways': gateways,
                'source_hash': row['source_hash']}
    finally:
        db.close()
