import csv
import hashlib
import io
import json
from pathlib import Path

from .store import connect
from .identity import event_identity, content_hash
from .contract import (agreement_digest, aware, fx_digest, validate_currency,
                       validate_fx_rate, validate_settlement_agreement)


def source(db, path):
    raw = Path(path).read_bytes()
    h = hashlib.sha256(raw).hexdigest()
    old = db.execute('SELECT id FROM source_file WHERE sha=?', (h,)).fetchone()
    if old:
        return old['id'], True
    return db.execute('INSERT INTO source_file(path,sha,raw) VALUES (?,?,?)', (str(path), h, raw)).lastrowid, False


def stream(path, db_path):
    """Ingest transaction events.

    ``currency`` is the event's original currency.  Legacy fixtures without a
    currency stay valid: their amount is treated as already being in the
    merchant settlement currency at an exact 1:1 (see rules.py).  Every other
    currency needs an explicit directional FX quote.
    """
    db = connect(db_path)
    count = 0
    try:
        sid, dup = source(db, path)
        if dup:
            return 0
        with db:
            lines = Path(path).read_text(encoding='utf-8').splitlines()
            for n, line in enumerate(lines, 1):
                try:
                    e = json.loads(line)
                    merchant, tx, version = event_identity(e)
                    kind = e['kind']
                    if kind not in ('authorize', 'confirm', 'cancel', 'refund'):
                        raise ValueError('kind')
                    currency = e.get('currency')
                    if currency is not None:
                        try:
                            currency = validate_currency(currency)
                        except ValueError:
                            # Unknown currency taints this merchant's evidence; it
                            # must never silently fall back to a same-currency net.
                            db.execute('INSERT INTO issue(code,merchant,tx,detail) VALUES (?,?,?,?)',
                                       ('currency_invalid', merchant, tx, line))
                            continue
                    sha = content_hash(line)
                    old = db.execute('SELECT sha FROM transaction_event WHERE merchant=? AND tx=? AND version=?',
                                     (merchant, tx, version)).fetchone()
                    if old:
                        if old['sha'] != sha:
                            db.execute('INSERT INTO issue(code,merchant,tx,detail) VALUES (?,?,?,?)',
                                       ('identity_conflict', merchant, tx, line))
                        continue
                    db.execute('INSERT OR IGNORE INTO account VALUES (?)', (merchant,))
                    db.execute('INSERT INTO transaction_event(source_id,line_no,merchant,tx,version,kind,amount,currency,business_at,arrival_at,sha,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                               (sid, n, merchant, tx, version, kind, str(e['amount']), currency,
                                e['business_at'], e.get('arrival_at', e['business_at']), sha, line))
                    count += 1
                except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                    db.execute('INSERT INTO issue(code,detail) VALUES (?,?)', ('parse_error', f'line {n}: {exc}'))
    finally:
        db.close()
    return count


def batch(path, db_path, window):
    db = connect(db_path)
    count = 0
    try:
        sid, dup = source(db, path)
        if dup:
            return 0
        rows = list(csv.DictReader(io.StringIO(Path(path).read_text(encoding='utf-8-sig'))))
        with db:
            for row in rows:
                if row.get('corrects') and not db.execute('SELECT 1 FROM batch_line WHERE id=?', (row['corrects'],)).fetchone():
                    db.execute('INSERT INTO issue(code,merchant,detail) VALUES (?,?,?)',
                               ('orphan_correction', row.get('merchant'), row['id']))
                    continue
                try:
                    db.execute('INSERT INTO batch_line VALUES (?,?,?,?,?,?)',
                               (row['id'], window, row['merchant'], row['amount'], row.get('corrects') or None, sid))
                    count += 1
                except Exception:
                    db.execute('INSERT INTO issue(code,merchant,detail) VALUES (?,?,?)',
                               ('batch_conflict', row.get('merchant'), row.get('id')))
    finally:
        db.close()
    return count


def _rows(path):
    raw = Path(path).read_text(encoding='utf-8-sig')
    if Path(path).suffix.lower() in ('.csv', '.tsv'):
        dialect = 'excel-tab' if Path(path).suffix.lower() == '.tsv' else 'excel'
        return list(csv.DictReader(io.StringIO(raw), dialect=dialect))
    data = json.loads(raw)
    if isinstance(data, dict):
        data = data.get('rates') or data.get('fx') or data.get('rows') or []
    if not isinstance(data, list):
        raise ValueError('fx_invalid')
    return data


def import_fx(path, db_path):
    """Import local FX quotes.

    Each row is an explicit ``base`` -> ``quote`` direction with a half-open
    effective interval.  The reverse direction is never inferred from a
    reciprocal: a caller needing USD->EUR must import that direction itself.
    """
    db = connect(db_path)
    changed = 0
    try:
        sid, dup = source(db, path)
        if dup:
            return 0
        with db:
            seen = {}
            for row in _rows(path):
                try:
                    validate_currency(row.get('base'))
                    validate_currency(row.get('quote'))
                    validate_fx_rate(row)
                except (KeyError, ValueError):
                    # A bad quote must never be stored: it could otherwise
                    # satisfy a later lookup and produce a closable net.
                    raise ValueError('fx_invalid') from None
                digest = fx_digest(row)
                identity = (row['base'], row['quote'], row['start_at'], row['end_at'])
                if identity in seen and seen[identity] != digest:
                    raise ValueError('fx_invalid')
                seen[identity] = digest
                old = db.execute('SELECT * FROM fx_rate_version WHERE base=? AND quote=? AND start_at=? AND end_at=?',
                                 identity).fetchone()
                values = (row['base'], row['quote'], row['start_at'], row['end_at'],
                          str(row['quote_rate']), row.get('source'), digest)
                if old:
                    if old['digest'] != digest:
                        db.execute('UPDATE fx_rate_version SET quote_rate=?,source=?,digest=? WHERE id=?',
                                   (str(row['quote_rate']), row.get('source'), digest, old['id']))
                        changed += 1
                    continue
                db.execute('INSERT INTO fx_rate_version(base,quote,start_at,end_at,quote_rate,source,digest) VALUES (?,?,?,?,?,?,?)',
                           values)
                changed += 1
    finally:
        db.close()
    return changed


def _contract_rows(path):
    raw = Path(path).read_text(encoding='utf-8-sig')
    if Path(path).suffix.lower() == '.csv':
        return {'agreements': list(csv.DictReader(io.StringIO(raw)))}
    data = json.loads(raw)
    if isinstance(data, list):
        return {'agreements': data}
    if not isinstance(data, dict):
        raise ValueError('contract_invalid')
    return data


def import_contract(path, db_path):
    """Import merchant settlement currency, route agreements and freezes.

    The route is selected *in advance* by an agreement effective over the
    window; the freeze pins the agreement (and its digest) a run used.
    """
    db = connect(db_path)
    result = {'currencies': 0, 'agreements': [], 'frozen': 0}
    try:
        sid, dup = source(db, path)
        if dup:
            return result
        data = _contract_rows(path)
        with db:
            for row in data.get('currencies', []):
                merchant = str(row['merchant'])
                currency = validate_currency(row['currency'])
                digest = hashlib.sha256(json.dumps(
                    {'merchant': merchant, 'currency': currency, 'source': row.get('source')},
                    sort_keys=True).encode()).hexdigest()
                db.execute('INSERT OR REPLACE INTO account VALUES (?)', (merchant,))
                db.execute('INSERT OR REPLACE INTO merchant_currency(merchant,currency,source,digest) VALUES (?,?,?,?)',
                           (merchant, currency, row.get('source'), digest))
                result['currencies'] += 1
            for row in data.get('agreements', []):
                if row.get('id') is None:
                    raise ValueError('contract_invalid')
                version = validate_settlement_agreement(row)
                merchant = str(row['merchant'])
                agreement_id = row.get('id')
                digest = agreement_digest(row)
                old = db.execute('SELECT * FROM settlement_agreement WHERE id=?', (int(agreement_id),)).fetchone()
                if old:
                    if old['digest'] != digest:
                        db.execute('UPDATE settlement_agreement SET merchant=?,route=?,start_at=?,end_at=?,version=?,source=?,digest=? WHERE id=?',
                                   (merchant, row['route'], row['start_at'], row['end_at'], version,
                                    row.get('source'), digest, int(agreement_id)))
                    result['agreements'].append(int(agreement_id))
                    continue
                same = db.execute('SELECT * FROM settlement_agreement WHERE merchant=? AND start_at=? AND end_at=? AND version=?',
                                  (merchant, row['start_at'], row['end_at'], version)).fetchone()
                if same:
                    raise ValueError(f'contract_identity_conflict:{same["id"]}')
                cursor = db.execute('INSERT INTO settlement_agreement(merchant,route,start_at,end_at,version,source,digest) VALUES (?,?,?,?,?,?,?)',
                                    (merchant, row['route'], row['start_at'], row['end_at'], version, row.get('source'), digest))
                result['agreements'].append(cursor.lastrowid)
            for row in data.get('freeze', []):
                merchant = str(row['merchant'])
                window_id = str(row['window'])
                agreement_id = int(row.get('agreement', row.get('agreement_id')))
                agreement = db.execute('SELECT * FROM settlement_agreement WHERE id=?', (agreement_id,)).fetchone()
                if not agreement or agreement['merchant'] != merchant:
                    raise ValueError('contract_unknown')
                freeze_contract_row(db, merchant, window_id, dict(agreement))
                result['frozen'] += 1
    finally:
        db.close()
    return result


def freeze_contract_row(db, merchant, window_id, agreement):
    window = db.execute('SELECT * FROM window WHERE id=?', (window_id,)).fetchone()
    if not window:
        raise ValueError('unknown_window')
    if not (aware(agreement['start_at']) <= aware(window['start_at']) and
            aware(window['end_at']) <= aware(agreement['end_at'])):
        raise ValueError('contract_bounds')
    validate_settlement_agreement(agreement)
    digest = agreement['digest'] if agreement.get('digest') else agreement_digest(agreement)
    db.execute('INSERT OR REPLACE INTO frozen_contract(merchant,window_id,agreement_id,route,digest) VALUES (?,?,?,?,?)',
               (merchant, window_id, agreement['id'], agreement['route'], digest))


def freeze_contract(db_path, merchant, window_id, agreement_id):
    db = connect(db_path)
    try:
        with db:
            agreement = db.execute('SELECT * FROM settlement_agreement WHERE id=? AND merchant=?',
                                   (agreement_id, merchant)).fetchone()
            if not agreement:
                raise ValueError('contract_unknown')
            freeze_contract_row(db, merchant, window_id, dict(agreement))
    finally:
        db.close()
