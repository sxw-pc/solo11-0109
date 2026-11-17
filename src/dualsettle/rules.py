import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP

from .store import connect
from .timeline import at, window_for, transactions
from .contract import validate_window, validate_rate

Q = Decimal('0.01')


def money(x):
    return str(Decimal(x).quantize(Q, rounding=ROUND_HALF_UP))


def input_digest(db):
    """Digest every input that can change a reconciliation outcome.

    Currency statements, FX versions and settlement agreements are part of the
    identity: changing any of them invalidates an old run's current state while
    leaving its stored amounts and frozen source evidence untouched.
    """
    rows = []
    for table, cols in [
        ('transaction_event', 'sha,business_at,arrival_at,currency'),
        ('batch_line', 'id,amount,corrects'),
        ('rate_version', 'merchant,start_at,end_at,rate'),
        ('adjustment', 'window_id,merchant,amount,reason'),
        ('window', 'id,start_at,end_at,cutoff_at,version'),
        ('merchant_currency', 'merchant,currency,digest'),
        ('fx_rate_version', 'id,base,quote,start_at,end_at,quote_rate,digest'),
        ('settlement_agreement', 'id,merchant,route,start_at,end_at,version,digest'),
        ('frozen_contract', 'merchant,window_id,agreement_id,route,digest'),
    ]:
        rows.append((table, [tuple(r) for r in db.execute(f'SELECT {cols} FROM {table} ORDER BY 1,2')]))
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def reconcile(path, window_id):
    db = connect(path)
    result = {}
    with db:
        window = db.execute('SELECT * FROM window WHERE id=?', (window_id,)).fetchone()
        if not window:
            raise ValueError('unknown_window')
        validate_window(window)
        windows = [dict(x) for x in db.execute('SELECT * FROM window')]
        events = [dict(x) for x in db.execute('SELECT * FROM transaction_event')]
        dig = input_digest(db)
        # Any input change (including FX or contract) makes old runs non-current
        # while retaining their amounts and frozen evidence rows.
        db.execute("UPDATE reconcile_run SET status='已失效' WHERE window_id=? AND status<>'已失效' AND source_digest<>?",
                   (window_id, dig))
        run = db.execute("INSERT INTO reconcile_run(window_id,status,source_digest) VALUES (?,'待接收',?)",
                         (window_id, dig)).lastrowid
        db.execute("UPDATE reconcile_run SET status='已校验' WHERE id=?", (run,))
        db.execute('INSERT INTO reconcile_issue(run_id,code,merchant,tx,detail) SELECT ?,code,merchant,tx,detail FROM issue',
                   (run,))

        def record(code, merchant, tx):
            db.execute('INSERT INTO reconcile_issue(run_id,code,merchant,tx) VALUES (?,?,?,?)',
                       (run, code, merchant, tx))

        aggregates = {}
        unresolved = set()
        for t in transactions(events, db, window):
            merchant, tx = t['merchant'], t['tx']
            items = t['events']
            try:
                wid = window_for(at(items[0]['business_at']), windows)
            except ValueError:
                unresolved.add(merchant)
                record('window_ambiguous', merchant, tx)
                continue
            if wid != window_id:
                continue
            if any(at(e['arrival_at']) > at(window['cutoff_at']) for e in items):
                unresolved.add(merchant)
                record('late_arrival', merchant, tx)
                continue
            if not t['valid']:
                unresolved.add(merchant)
                record(t['reason'], merchant, tx)
                continue

            confirm = t['confirm_fx']
            rates = [r for r in db.execute('SELECT * FROM rate_version WHERE merchant=?', (merchant,))
                     if at(r['start_at']) <= at(items[0]['business_at']) < at(r['end_at'])]
            try:
                for rate in rates:
                    validate_rate(rate)
            except ValueError:
                unresolved.add(merchant)
                record('rate_invalid', merchant, tx)
                continue
            if len(rates) != 1:
                unresolved.add(merchant)
                record('rate_ambiguous', merchant, tx)
                continue
            # Fee applies only to the settled-currency confirmed amount, using
            # the existing per-transaction rounding order.  It is never charged
            # on a mixed original-currency sum or on the pre-conversion sum.
            gross = t['gross_settlement']
            fee = (gross * Decimal(rates[0]['rate'])).quantize(Q, rounding=ROUND_HALF_UP)

            a = aggregates.setdefault(merchant, {
                'gross': Decimal(0), 'fee': Decimal(0), 'refund': Decimal(0),
                'ids': [], 'currency': t['settlement_currency'], 'transactions': [],
            })
            a['gross'] += gross
            a['fee'] += fee
            a['refund'] += t['refund_settlement']
            a['ids'] += [e['id'] for e in items]
            transaction_record = {
                'tx': tx,
                'route': t['route'],
                'contract_agreement_id': t['contract_agreement_id'],
                'contract_digest': t['contract_digest'],
                'tx_currency': t['currency'],
                'settlement_currency': t['settlement_currency'],
                'original_confirmed_amount': str(t['confirmed_amount']),
                'original_refunded_amount': str(t['refunded_amount']),
                'confirm': confirm,
                'refunds': t['refund_fx'],
                'fee': {'rate': str(rates[0]['rate']), 'rate_id': rates[0]['id'],
                        'basis': str(gross), 'amount': str(fee)},
                'rounding': {
                    'confirm_residual': confirm['rounding_residual'] if confirm else '0',
                    'refund_residual': t['rounding_residual'],
                },
            }
            a['transactions'].append(transaction_record)
            db.execute('INSERT OR REPLACE INTO run_transaction(run_id,window_id,merchant,tx,detail) VALUES (?,?,?,?,?)',
                       (run, window_id, merchant, tx, json.dumps(transaction_record, sort_keys=True)))

            for e in items:
                fx_detail = None
                fx_rate_id = None
                fx_role = None
                if confirm is not None and e['id'] == confirm['event_id']:
                    fx_detail = json.dumps(confirm, sort_keys=True)
                    fx_rate_id = confirm['fx_rate_id']
                    fx_role = 'confirm'
                for entry in t['refund_fx']:
                    if entry['event_id'] == e['id']:
                        fx_detail = json.dumps(entry, sort_keys=True)
                        fx_rate_id = entry['fx_rate_id']
                        fx_role = entry['role']
                db.execute('INSERT INTO evidence(run_id,merchant,event_id,rate_id,fx_rate_id,fx_role,fx_detail,detail) VALUES (?,?,?,?,?,?,?,?)',
                           (run, merchant, e['id'], rates[0]['id'], fx_rate_id, fx_role, fx_detail, 'stream'))

        merchants = ({r['id'] for r in db.execute('SELECT * FROM account')} |
                     {r['merchant'] for r in db.execute('SELECT * FROM batch_line WHERE window_id=?', (window_id,))})
        for merchant in sorted(merchants):
            a = aggregates.get(merchant, {'gross': Decimal(0), 'fee': Decimal(0), 'refund': Decimal(0),
                                          'ids': [], 'currency': None, 'transactions': []})
            net = a['gross'] - a['fee'] - a['refund']
            aggregate_values = (money(a['gross']), money(a['fee']), money(a['refund']), money(net),
                                json.dumps(a['ids']), a['currency'])
            db.execute('INSERT OR REPLACE INTO stream_aggregate(window_id,merchant,gross,fee,refund,net,event_ids,currency,fx_detail) VALUES (?,?,?,?,?,?,?,?,?)',
                       (window_id, merchant, *aggregate_values, json.dumps(a['transactions'], sort_keys=True)))
            db.execute('INSERT OR REPLACE INTO run_aggregate(run_id,window_id,merchant,gross,fee,refund,net,event_ids,currency) VALUES (?,?,?,?,?,?,?,?,?)',
                       (run, window_id, merchant, *aggregate_values))
            lines = [dict(x) for x in db.execute('SELECT * FROM batch_line WHERE window_id=? AND merchant=?',
                                                 (window_id, merchant))]
            batch_net = sum((Decimal(x['amount']) for x in lines), Decimal(0))
            adjustment = sum((Decimal(x['amount']) for x in db.execute(
                'SELECT amount FROM adjustment WHERE window_id=? AND merchant=?', (window_id, merchant))), Decimal(0))
            for line in lines:
                db.execute('INSERT INTO evidence(run_id,merchant,batch_id,detail) VALUES (?,?,?,?)',
                           (run, merchant, line['id'], 'batch'))
            issues = [r['code'] for r in db.execute('SELECT code FROM reconcile_issue WHERE run_id=? AND merchant=?',
                                                    (run, merchant))]
            if merchant in unresolved or issues:
                status = '证据不足'
                reason = ','.join(sorted(set(issues)))
            elif not lines:
                status = '证据不足'
                reason = 'missing_batch'
            elif net + adjustment != batch_net:
                status = '差异'
                reason = 'amount_mismatch'
            else:
                status = '可封账'
                reason = 'matched'
            db.execute('INSERT INTO verdict VALUES (?,?,?,?,?,?)',
                       (run, merchant, status, money(net), money(batch_net), reason))
            result[merchant] = status
        db.execute("UPDATE reconcile_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",
                   ('待核' if any(v != '可封账' for v in result.values()) else '已判定', run))
    db.close()
    return run, result


def close(path, run):
    db = connect(path)
    with db:
        r = db.execute('SELECT * FROM reconcile_run WHERE id=?', (run,)).fetchone()
        if not r or r['status'] != '已判定':
            raise ValueError('not_closable')
        if input_digest(db) != r['source_digest']:
            raise ValueError('source_changed')
        db.execute("UPDATE reconcile_run SET status='已归档' WHERE id=?", (run,))
    db.close()
