"""Offline cross-currency reconciliation tests.

All data is synthetic.  No network, no real payment data.
"""
import csv
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from dualsettle.store import connect
from dualsettle.ingest import stream, batch, import_fx, import_contract, freeze_contract
from dualsettle.rules import reconcile, close
from dualsettle.report import write

OPEN = '2025-01-01T00:00:00+00:00'
FAR = '2027-01-01T00:00:00+00:00'


def base_db(tmp_path, fee='0'):
    db = tmp_path / 'x.db'
    c = connect(db)
    with c:
        c.execute("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        c.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)', ('M', OPEN, FAR, fee))
    c.close()
    return db


def write_events(tmp_path, rows):
    p = tmp_path / 'events.jsonl'
    p.write_text('\n'.join(json.dumps(x) for x in rows), encoding='utf-8')
    return p


def write_csv(tmp_path, name, fieldnames, rows):
    p = tmp_path / name
    with p.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return p


def write_fx(tmp_path, rows):
    return write_csv(tmp_path, 'fx.csv', ['base', 'quote', 'start_at', 'end_at', 'quote_rate', 'source'], rows)


def write_contract(tmp_path, currency='USD', route='original_lock', agreement_id=1):
    p = tmp_path / 'contract.json'
    p.write_text(json.dumps({
        'currencies': [{'merchant': 'M', 'currency': currency, 'source': 'synthetic'}],
        'agreements': [{'id': agreement_id, 'merchant': 'M', 'route': route,
                        'start_at': OPEN, 'end_at': FAR, 'version': 1, 'source': 'synthetic'}],
    }), encoding='utf-8')
    return p


def write_batch(tmp_path, amount):
    return write_csv(tmp_path, 'batch.csv', ['id', 'merchant', 'amount', 'corrects'],
                     [{'id': 'B', 'merchant': 'M', 'amount': amount, 'corrects': ''}])


def tx_events(refund_amounts=('50.00',), currency='EUR', confirm_amount='100.00'):
    """Confirm 100 EUR at 02:00, refund(s) later in the same business day."""
    rows = [
        {'merchant': 'M', 'tx': 'T', 'version': 1, 'kind': 'authorize', 'amount': confirm_amount,
         'currency': currency, 'business_at': '2026-01-01T01:00:00+00:00'},
        {'merchant': 'M', 'tx': 'T', 'version': 2, 'kind': 'confirm', 'amount': confirm_amount,
         'currency': currency, 'business_at': '2026-01-01T02:00:00+00:00'},
    ]
    for i, amount in enumerate(refund_amounts, 3):
        rows.append({'merchant': 'M', 'tx': 'T', 'version': i, 'kind': 'refund', 'amount': amount,
                     'currency': currency, 'business_at': f'2026-01-01T0{i}:30:00+00:00'})
    return rows


def two_fx_versions(tmp_path, boundary='2026-01-01T02:30:00+00:00'):
    return write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': boundary, 'quote_rate': '1.20', 'source': 'open'},
        {'base': 'EUR', 'quote': 'USD', 'start_at': boundary, 'end_at': FAR, 'quote_rate': '1.10', 'source': 'later'},
    ])


def load(tmp_path, db, events, fx, contract, batch_amount):
    stream(events, db)
    import_fx(fx, db)
    import_contract(contract, db)
    batch(write_batch(tmp_path, batch_amount), db, 'D1')


def verdict(db, run=1):
    row = connect(db).execute('SELECT * FROM verdict WHERE run_id=? AND merchant=?', (run, 'M')).fetchone()
    return dict(row)


def transaction(db, run=1):
    row = connect(db).execute('SELECT detail FROM run_transaction WHERE run_id=? AND merchant=? AND tx=?',
                              (run, 'M', 'T')).fetchone()
    return json.loads(row['detail'])


def test_original_lock_net_is_60_and_matches(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='original_lock'), '60.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert verdict(db)['stream_net'] == '60.00'
    tx = transaction(db)
    assert tx['route'] == 'original_lock'
    assert tx['confirm']['quote_rate'] == '1.20'
    assert tx['refunds'][0]['role'] == 'refund_original_lock'
    assert tx['refunds'][0]['quote_rate'] == '1.20'
    assert tx['refunds'][0]['converted'] == '60.00'
    close(db, run)


def test_refund_spot_net_is_65_and_same_batch_is_a_difference(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '60.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '差异'}
    assert verdict(db)['stream_net'] == '65.00'
    assert verdict(db)['reason'] == 'amount_mismatch'
    tx = transaction(db)
    assert tx['refunds'][0]['role'] == 'refund_spot'
    assert tx['refunds'][0]['fx_source'] == 'later'
    assert tx['refunds'][0]['quote_rate'] == '1.10'
    with pytest.raises(ValueError, match='not_closable'):
        close(db, run)


def test_same_60_batch_cannot_be_used_to_infer_contract(tmp_path):
    # The batch total is identical; only the pre-selected route decides.
    results = {}
    for route in ('original_lock', 'refund_spot'):
        root = tmp_path / route
        root.mkdir()
        db = base_db(root)
        load(root, db, write_events(root, tx_events()), two_fx_versions(root),
             write_contract(root, route=route), '60.00')
        results[route] = reconcile(db, 'D1')[1]['M']
    assert results == {'original_lock': '可封账', 'refund_spot': '差异'}


def test_fee_is_charged_only_on_converted_confirm_amount(tmp_path):
    db = base_db(tmp_path, fee='0.01')
    # 100 EUR @1.20 -> 120 USD, fee 1% = 1.20, refund 50 EUR locked @1.20 = 60 USD.
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='original_lock'), '58.80')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert verdict(db)['stream_net'] == '58.80'
    row = connect(db).execute('SELECT gross,fee,refund,currency FROM stream_aggregate').fetchone()
    assert (row['gross'], row['fee'], row['refund'], row['currency']) == ('120.00', '1.20', '60.00', 'USD')
    assert transaction(db)['fee'] == {'rate': '0.01', 'rate_id': 1, 'basis': '120.00', 'amount': '1.20'}


def test_multiple_partial_refunds_conserve_original_currency(tmp_path):
    db = base_db(tmp_path)
    # 40 EUR @1.20 and 35 EUR @1.10 refunds under refund_spot: 48 + 38.50.
    load(tmp_path, db, write_events(tmp_path, tx_events(('40.00', '35.00'))),
         two_fx_versions(tmp_path, '2026-01-01T04:00:00+00:00'),
         write_contract(tmp_path, route='refund_spot'), '33.50')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert verdict(db)['stream_net'] == '33.50'
    tx = transaction(db)
    assert tx['original_refunded_amount'] == '75.00'
    assert [r['converted'] for r in tx['refunds']] == ['48.00', '38.50']
    assert Decimal(tx['rounding']['refund_residual']) == Decimal('0')


def test_original_currency_over_refund_never_reaches_fx(tmp_path):
    db = base_db(tmp_path)
    # 60 + 50 EUR > 100 EUR confirmed.  Even if 50 USD would make the batch fit,
    # original-currency conservation fails first.
    load(tmp_path, db, write_events(tmp_path, tx_events(('60.00', '50.00'))), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '55.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'refund_exceeds_confirm' in verdict(db)['reason']
    with pytest.raises(ValueError, match='not_closable'):
        close(db, run)


def test_rounding_residual_is_explained_not_absorbed(tmp_path):
    db = base_db(tmp_path)
    # 33.335 EUR at 1.10 => 36.6685, rounded once at the refund boundary.
    load(tmp_path, db, write_events(tmp_path, tx_events(('33.335',))), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '100.00')
    run, out = reconcile(db, 'D1')
    tx = transaction(db)
    refund = tx['refunds'][0]
    assert refund['exact_converted'] == '36.66850'
    assert refund['converted'] == '36.67'
    assert refund['rounding_residual'] == '-0.00150'
    assert tx['rounding']['refund_residual'] == '-0.00150'
    data = json.loads(write(db, run, tmp_path / 'r').read_text(encoding='utf-8'))
    assert data['transactions'][0]['refunds'][0]['rounding_residual'] == '-0.00150'


def test_version_boundary_hits_exactly_one_version(tmp_path):
    db = base_db(tmp_path)
    boundary = '2026-01-01T03:30:00+00:00'  # exactly the refund business time
    fx = two_fx_versions(tmp_path, boundary)
    load(tmp_path, db, write_events(tmp_path, tx_events()), fx, write_contract(tmp_path, route='refund_spot'), '65.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    refund = transaction(db)['refunds'][0]
    assert refund['quote_rate'] == '1.10'  # [start, end): the later version only
    assert refund['fx_source'] == 'later'


def test_gap_blocks_conversion_instead_of_defaulting_one_to_one(tmp_path):
    db = base_db(tmp_path)
    fx = write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': '2026-01-01T02:30:00+00:00',
         'quote_rate': '1.20', 'source': 'open'},
        {'base': 'EUR', 'quote': 'USD', 'start_at': '2026-01-01T04:00:00+00:00', 'end_at': FAR,
         'quote_rate': '1.10', 'source': 'later'},
    ])
    load(tmp_path, db, write_events(tmp_path, tx_events()), fx, write_contract(tmp_path, route='refund_spot'), '60.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'fx_rate_missing' in verdict(db)['reason']
    with pytest.raises(ValueError, match='not_closable'):
        close(db, run)


def test_overlapping_versions_block_conversion(tmp_path):
    db = base_db(tmp_path)
    fx = write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': FAR, 'quote_rate': '1.20', 'source': 'a'},
        {'base': 'EUR', 'quote': 'USD', 'start_at': '2026-01-01T02:30:00+00:00', 'end_at': FAR,
         'quote_rate': '1.10', 'source': 'b'},
    ])
    load(tmp_path, db, write_events(tmp_path, tx_events()), fx, write_contract(tmp_path, route='refund_spot'), '65.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'fx_rate_ambiguous' in verdict(db)['reason']


def test_reverse_direction_is_not_inferred_from_reciprocal(tmp_path):
    db = base_db(tmp_path)
    # Only USD -> EUR is quoted; EUR -> USD must not be guessed as 1/1.20.
    fx = write_fx(tmp_path, [
        {'base': 'USD', 'quote': 'EUR', 'start_at': OPEN, 'end_at': FAR, 'quote_rate': '0.833333333333',
         'source': 'reverse-only'},
    ])
    load(tmp_path, db, write_events(tmp_path, tx_events()), fx, write_contract(tmp_path, route='original_lock'), '60.00')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'fx_rate_missing' in verdict(db)['reason']


def test_unknown_event_currency_is_insufficient(tmp_path):
    db = base_db(tmp_path)
    rows = tx_events()
    rows[2]['currency'] = 'usd'  # lowercase is not a valid ISO code here
    load(tmp_path, db, write_events(tmp_path, rows), two_fx_versions(tmp_path), write_contract(tmp_path), '60.00')
    assert reconcile(db, 'D1')[1]['M'] == '证据不足'
    assert 'currency_invalid' in verdict(db)['reason']


def test_currency_conflict_inside_one_transaction_is_insufficient(tmp_path):
    db = base_db(tmp_path)
    rows = tx_events()
    rows[2]['currency'] = 'GBP'
    load(tmp_path, db, write_events(tmp_path, rows), two_fx_versions(tmp_path), write_contract(tmp_path), '60.00')
    assert reconcile(db, 'D1')[1]['M'] == '证据不足'
    assert 'currency_conflict' in verdict(db)['reason']


def test_missing_settlement_currency_is_insufficient(tmp_path):
    db = base_db(tmp_path)
    stream(write_events(tmp_path, tx_events()), db)
    import_fx(two_fx_versions(tmp_path), db)
    import_contract(write_contract(tmp_path), db)
    c = connect(db)
    with c:
        c.execute('DELETE FROM merchant_currency')
    batch(write_batch(tmp_path, '60.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'settlement_currency_missing' in verdict(db)['reason']


def test_missing_contract_route_is_insufficient(tmp_path):
    db = base_db(tmp_path)
    stream(write_events(tmp_path, tx_events()), db)
    import_fx(two_fx_versions(tmp_path), db)
    # Settlement currency but no agreement / freeze.
    p = tmp_path / 'cur.json'
    p.write_text(json.dumps({'currencies': [{'merchant': 'M', 'currency': 'USD'}]}), encoding='utf-8')
    import_contract(p, db)
    batch(write_batch(tmp_path, '60.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'contract_missing' in verdict(db)['reason']


@pytest.mark.parametrize('rate', ['0', '-1.10', 'nan', 'inf', 'abc'])
def test_invalid_nonfinite_fx_quote_is_rejected_at_import(tmp_path, rate):
    db = base_db(tmp_path)
    p = write_fx(tmp_path, [{'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': FAR,
                             'quote_rate': rate, 'source': 'bad'}])
    with pytest.raises(ValueError, match='fx_invalid'):
        import_fx(p, db)
    assert connect(db).execute('SELECT COUNT(*) FROM fx_rate_version').fetchone()[0] == 0


def test_invalid_quote_cannot_be_smuggled_into_a_closable_net(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path), '60.00')
    # Bypass import validation and corrupt the stored quote directly.
    c = connect(db)
    with c:
        c.execute("UPDATE fx_rate_version SET quote_rate='nan' WHERE quote_rate='1.20'")
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'fx_rate_invalid' in verdict(db)['reason']
    with pytest.raises(ValueError, match='not_closable'):
        close(db, run)


def test_fx_input_change_invalidates_old_archived_run_but_keeps_evidence(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path), '60.00')
    old, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    close(db, old)
    before = json.loads(write(db, old, tmp_path / 'before').read_text(encoding='utf-8'))
    assert before['run']['effective_status'] == '已归档'
    assert before['transactions'][0]['refunds'][0]['quote_rate'] == '1.20'

    # Restating the confirm-time quote changes the locked conversion basis.
    import_fx(write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': '2026-01-01T02:30:00+00:00',
         'quote_rate': '1.30', 'source': 'restated'},
    ]), db)
    stale = json.loads(write(db, old, tmp_path / 'stale').read_text(encoding='utf-8'))
    assert stale['run']['effective_status'] == '已失效'
    assert stale['run']['superseded_by'] is None  # report written before a new run
    assert stale['transactions'][0]['refunds'][0]['quote_rate'] == '1.20'
    assert stale['transactions'][0]['refunds'][0]['fx_source'] == 'open'
    assert 'refund_original_lock' in (tmp_path / 'stale.md').read_text(encoding='utf-8')

    new, out = reconcile(db, 'D1')
    assert new != old
    assert out == {'M': '差异'}  # 130 - 65 = 65, batch 60
    assert connect(db).execute('SELECT status FROM reconcile_run WHERE id=?', (old,)).fetchone()[0] == '已失效'
    after = json.loads(write(db, new, tmp_path / 'after').read_text(encoding='utf-8'))
    assert after['transactions'][0]['refunds'][0]['quote_rate'] == '1.30'


def test_refund_time_version_change_is_causal_across_layers(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '65.00')
    run1, out1 = reconcile(db, 'D1')
    assert out1 == {'M': '可封账'}
    tx1 = transaction(db, run1)
    assert tx1['refunds'][0]['quote_rate'] == '1.10'

    import_fx(write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': '2026-01-01T02:30:00+00:00', 'end_at': FAR,
         'quote_rate': '1.05', 'source': 'refund-restated'},
    ]), db)
    run2, out2 = reconcile(db, 'D1')
    tx2 = transaction(db, run2)
    # The refund-time version now converts the refund at 1.05 => 52.50, net 67.50.
    assert tx2['refunds'][0]['quote_rate'] == '1.05'
    assert verdict(db, run2)['stream_net'] == '67.50'
    assert verdict(db, run2)['status'] == '差异'
    old = json.loads(write(db, run1, tmp_path / 'old').read_text(encoding='utf-8'))
    new = json.loads(write(db, run2, tmp_path / 'new').read_text(encoding='utf-8'))
    assert old['transactions'][0]['refunds'][0]['fx_source'] == 'later'
    assert new['transactions'][0]['refunds'][0]['fx_source'] == 'refund-restated'
    assert old['run']['effective_status'] == '已失效'


def test_contract_change_invalidates_old_run(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='original_lock'), '60.00')
    old, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    close(db, old)
    import_contract(write_contract(tmp_path, route='refund_spot'), db)
    stale = json.loads(write(db, old, tmp_path / 'stale').read_text(encoding='utf-8'))
    assert stale['run']['effective_status'] == '已失效'
    assert stale['transactions'][0]['route'] == 'original_lock'
    new, out = reconcile(db, 'D1')
    assert out == {'M': '差异'}
    assert transaction(db, new)['route'] == 'refund_spot'


def test_frozen_contract_is_used_and_overrides_effective_agreement(tmp_path):
    db = base_db(tmp_path)
    stream(write_events(tmp_path, tx_events()), db)
    import_fx(two_fx_versions(tmp_path), db)
    import_contract(write_contract(tmp_path, route='original_lock', agreement_id=1), db)
    # A second, still-effective agreement makes the unfrozen choice ambiguous.
    second = tmp_path / 'second.json'
    second.write_text(json.dumps({'agreements': [
        {'id': 2, 'merchant': 'M', 'route': 'refund_spot', 'start_at': OPEN, 'end_at': FAR,
         'version': 2, 'source': 'synthetic-2'}]}), encoding='utf-8')
    import_contract(second, db)
    batch(write_batch(tmp_path, '60.00'), db, 'D1')
    ambiguous, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'contract_ambiguous' in verdict(db, ambiguous)['reason']

    # The pre-selected frozen route is authoritative and yields the lock result.
    freeze_contract(db, 'M', 'D1', 1)
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert transaction(db, run)['route'] == 'original_lock'


def test_late_refund_across_cutoff_does_not_rewrite_archived_run(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '65.00')
    old, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    close(db, old)

    late = write_events(tmp_path, tx_events() + [
        {'merchant': 'M', 'tx': 'T', 'version': 9, 'kind': 'refund', 'amount': '10.00', 'currency': 'EUR',
         'business_at': '2026-01-01T05:00:00+00:00', 'arrival_at': '2026-01-03T00:00:00+00:00'},
    ])
    stream(late, db)
    stale = json.loads(write(db, old, tmp_path / 'stale').read_text(encoding='utf-8'))
    assert stale['run']['effective_status'] == '已失效'
    assert stale['verdicts'][0]['stream_net'] == '65.00'
    assert len(stale['transactions'][0]['refunds']) == 1
    new, out = reconcile(db, 'D1')
    # The late refund is unresolved evidence; the archived amount/evidence is kept.
    assert out == {'M': '证据不足'}
    assert 'late_arrival' in verdict(db, new)['reason']
    assert connect(db).execute('SELECT status FROM reconcile_run WHERE id=?', (old,)).fetchone()[0] == '已失效'
    assert connect(db).execute('SELECT stream_net FROM verdict WHERE run_id=?', (old,)).fetchone()[0] == '65.00'


def test_legacy_currencyless_fixture_keeps_one_to_one(tmp_path):
    db = base_db(tmp_path, fee='0.01')
    rows = [
        {'merchant': 'M', 'tx': 'T', 'version': 1, 'kind': 'authorize', 'amount': '100.00',
         'business_at': '2026-01-01T01:00:00+00:00'},
        {'merchant': 'M', 'tx': 'T', 'version': 2, 'kind': 'confirm', 'amount': '100.00',
         'business_at': '2026-01-01T02:00:00+00:00'},
        {'merchant': 'M', 'tx': 'T', 'version': 3, 'kind': 'refund', 'amount': '10.00',
         'business_at': '2026-01-01T03:00:00+00:00'},
    ]
    stream(write_events(tmp_path, rows), db)
    batch(write_batch(tmp_path, '89.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert verdict(db)['stream_net'] == '89.00'
    tx = transaction(db)
    assert tx['confirm']['mode'] == 'legacy_currencyless_1:1'
    assert tx['refunds'][0]['role'] == 'refund_legacy_1:1'
    assert tx['confirm']['converted'] == '100.00'


def test_same_currency_fixture_needs_no_fx_or_contract(tmp_path):
    db = base_db(tmp_path)
    rows = tx_events()
    for row in rows:
        row['currency'] = 'USD'
    stream(write_events(tmp_path, rows), db)
    p = tmp_path / 'cur.json'
    p.write_text(json.dumps({'currencies': [{'merchant': 'M', 'currency': 'USD'}]}), encoding='utf-8')
    import_contract(p, db)
    batch(write_batch(tmp_path, '50.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    # 100 USD confirm, 50 USD refund: no FX quote or route agreement required.
    assert out == {'M': '可封账'}
    assert verdict(db)['stream_net'] == '50.00'
    assert transaction(db)['confirm']['mode'] == 'same_currency'


def test_sqlite_migration_adds_currency_columns_to_existing_db(tmp_path):
    path = tmp_path / 'legacy.db'
    raw = sqlite3.connect(path)
    raw.executescript('''
        CREATE TABLE transaction_event(id INTEGER PRIMARY KEY,source_id INTEGER,line_no INTEGER,merchant TEXT,
            tx TEXT,version INTEGER,kind TEXT,amount TEXT,business_at TEXT,arrival_at TEXT,sha TEXT,raw TEXT,
            UNIQUE(merchant,tx,version));
        CREATE TABLE stream_aggregate(window_id TEXT,merchant TEXT,gross TEXT,fee TEXT,refund TEXT,net TEXT,
            event_ids TEXT,PRIMARY KEY(window_id,merchant));
        CREATE TABLE evidence(id INTEGER PRIMARY KEY,run_id INTEGER,merchant TEXT,event_id INTEGER,batch_id TEXT,
            rate_id INTEGER,detail TEXT);
        INSERT INTO transaction_event(merchant,tx,version,kind,amount,business_at,arrival_at,sha,raw)
            VALUES ('M','T',1,'confirm','1.00','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00','x','{}');
    ''')
    raw.commit()
    raw.close()
    db = connect(path)
    columns = {r['name'] for r in db.execute('PRAGMA table_info(transaction_event)')}
    assert 'currency' in columns
    assert {r['name'] for r in db.execute('PRAGMA table_info(stream_aggregate)')} >= {'currency', 'fx_detail'}
    assert {r['name'] for r in db.execute('PRAGMA table_info(evidence)')} >= {'fx_rate_id', 'fx_role', 'fx_detail'}
    assert db.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0] == 1


def test_cli_fx_demo_shows_both_route_sources(tmp_path):
    from dualsettle.__main__ import fx_demo
    result = fx_demo(tmp_path / 'demo')
    assert result['verdicts'] == {'M1': '可封账'}
    data = json.loads(Path(result['report']).read_text(encoding='utf-8'))
    assert data['transactions'][0]['route'] == 'original_lock'
    assert data['fx_sources']
    assert any(fx['fx_role'] == 'refund_original_lock' for fx in data['fx_sources'])


def test_frozen_contract_stops_working_when_agreement_is_rewritten(tmp_path):
    db = base_db(tmp_path)
    stream(write_events(tmp_path, tx_events()), db)
    import_fx(two_fx_versions(tmp_path), db)
    import_contract(write_contract(tmp_path, route='original_lock', agreement_id=1), db)
    freeze_contract(db, 'M', 'D1', 1)
    # Rewriting agreement 1's route invalidates the frozen identity.
    import_contract(write_contract(tmp_path, route='refund_spot', agreement_id=1), db)
    batch(write_batch(tmp_path, '60.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'contract_changed' in verdict(db, run)['reason']


def test_report_shows_old_and_new_fx_sources_side_by_side(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events(('50.00',))), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '65.00')
    old, _ = reconcile(db, 'D1')
    import_fx(write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': '2026-01-01T02:30:00+00:00', 'end_at': FAR,
         'quote_rate': '1.05', 'source': 'new-source'},
    ]), db)
    new, _ = reconcile(db, 'D1')
    data = json.loads(write(db, old, tmp_path / 'old').read_text(encoding='utf-8'))
    assert data['run']['superseded_by'] == new
    old_sources = [x['source'] for x in data['fx_sources']]
    new_sources = [x['source'] for x in data['successor_fx_sources']]
    assert 'later' in old_sources and 'new-source' in new_sources
    assert 'new-source' not in old_sources
    assert 'new-source' in (tmp_path / 'old.md').read_text(encoding='utf-8')


def test_currencyless_event_with_declared_settlement_currency_is_insufficient(tmp_path):
    db = base_db(tmp_path)
    rows = tx_events()
    del rows[0]['currency']  # one currencyless event inside a known-currency tx
    stream(write_events(tmp_path, rows), db)
    p = tmp_path / 'cur.json'
    p.write_text(json.dumps({'currencies': [{'merchant': 'M', 'currency': 'USD'}]}), encoding='utf-8')
    import_contract(p, db)
    batch(write_batch(tmp_path, '60.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '证据不足'}
    assert 'currency_conflict' in verdict(db, run)['reason']


def test_import_fx_rejects_self_pair_and_requires_identity(tmp_path):
    db = base_db(tmp_path)
    p = write_fx(tmp_path, [{'base': 'USD', 'quote': 'USD', 'start_at': OPEN, 'end_at': FAR,
                             'quote_rate': '1.0', 'source': 'bad'}])
    with pytest.raises(ValueError, match='fx_invalid'):
        import_fx(p, db)
    q = tmp_path / 'two.json'
    q.write_text(json.dumps({'currencies': [{'merchant': 'M', 'currency': 'USD'}],
                             'agreements': [{'merchant': 'M', 'route': 'original_lock',
                                             'start_at': OPEN, 'end_at': FAR, 'version': 1}]}), encoding='utf-8')
    with pytest.raises(ValueError, match='contract_invalid'):
        import_contract(q, db)


def test_fx_import_is_idempotent_and_restatement_updates_digest(tmp_path):
    db = base_db(tmp_path)
    p = two_fx_versions(tmp_path)
    assert import_fx(p, db) == 2
    assert import_fx(p, db) == 0  # same file digest -> duplicate source, no change
    before = connect(db).execute('SELECT digest FROM fx_rate_version ORDER BY id').fetchall()
    p2 = write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': '2026-01-01T02:30:00+00:00',
         'quote_rate': '1.25', 'source': 'open'},
        {'base': 'EUR', 'quote': 'USD', 'start_at': '2026-01-01T02:30:00+00:00', 'end_at': FAR,
         'quote_rate': '1.10', 'source': 'later'},
    ])
    assert import_fx(p2, db) == 1
    after = connect(db).execute('SELECT quote_rate,digest FROM fx_rate_version ORDER BY id').fetchall()
    assert after[0]['quote_rate'] == '1.25'
    assert after[0]['digest'] != before[0]['digest']


def test_batch_total_is_never_used_to_offset_the_fx_net(tmp_path):
    db = base_db(tmp_path)
    # refund_spot net is 65.00; a batch restatement to 67.50 must stay a visible
    # difference and can never rewrite the stream's conversion net.
    load(tmp_path, db, write_events(tmp_path, tx_events(('50.00',))), two_fx_versions(tmp_path),
         write_contract(tmp_path, route='refund_spot'), '67.50')
    run, out = reconcile(db, 'D1')
    row = connect(db).execute('SELECT net FROM stream_aggregate').fetchone()
    assert row['net'] == '65.00'
    assert verdict(db, run)['stream_net'] == '65.00'
    assert verdict(db, run)['batch_net'] == '67.50'
    assert verdict(db, run)['status'] == '差异'
    assert verdict(db, run)['reason'] == 'amount_mismatch'


def test_old_report_aggregates_are_frozen_not_live(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path), '60.00')
    old, _ = reconcile(db, 'D1')
    old_data = json.loads(write(db, old, tmp_path / 'old').read_text(encoding='utf-8'))
    assert (old_data['aggregates'][0]['gross'], old_data['aggregates'][0]['net']) == ('120.00', '60.00')
    import_fx(write_fx(tmp_path, [
        {'base': 'EUR', 'quote': 'USD', 'start_at': OPEN, 'end_at': '2026-01-01T02:30:00+00:00',
         'quote_rate': '1.30', 'source': 'restated'},
    ]), db)
    new, _ = reconcile(db, 'D1')
    # The live aggregate reflects the new basis; the old report must not.
    live = connect(db).execute('SELECT gross,net FROM stream_aggregate').fetchone()
    assert live['net'] == '65.00'
    stale = json.loads(write(db, old, tmp_path / 'stale').read_text(encoding='utf-8'))
    assert (stale['aggregates'][0]['gross'], stale['aggregates'][0]['net']) == ('120.00', '60.00')
    fresh = json.loads(write(db, new, tmp_path / 'fresh').read_text(encoding='utf-8'))
    assert (fresh['aggregates'][0]['gross'], fresh['aggregates'][0]['net']) == ('130.00', '65.00')


def test_cli_commands_import_fx_contract_and_freeze(tmp_path, monkeypatch, capsys):
    from dualsettle import __main__ as cli
    db = base_db(tmp_path)
    events = write_events(tmp_path, tx_events())
    fx = two_fx_versions(tmp_path)
    contract = write_contract(tmp_path, route='refund_spot')
    batch_file = write_batch(tmp_path, '65.00')

    def run(*args):
        monkeypatch.setattr('sys.argv', ['dualsettle', *args])
        cli.main()

    run('ingest-stream', str(events), '--db', str(db))
    run('ingest-fx', str(fx), '--db', str(db))
    run('ingest-contract', str(contract), '--db', str(db))
    run('ingest-batch', str(batch_file), '--db', str(db), '--window', 'D1')
    run('freeze-contract', '--db', str(db), '--merchant', 'M', '--window', 'D1', '--agreement', '1')
    run('reconcile', '--db', str(db), '--window', 'D1')
    run('report', '--db', str(db), '--run', '1', '--out', str(tmp_path / 'cli-report'))
    data = json.loads((tmp_path / 'cli-report.json').read_text(encoding='utf-8'))
    assert data['transactions'][0]['route'] == 'refund_spot'
    assert data['fx_sources']
    assert connect(db).execute('SELECT COUNT(*) FROM frozen_contract').fetchone()[0] == 1


def test_repeat_run_on_identical_inputs_is_not_superseded(tmp_path):
    db = base_db(tmp_path)
    load(tmp_path, db, write_events(tmp_path, tx_events()), two_fx_versions(tmp_path),
         write_contract(tmp_path), '60.00')
    first, out1 = reconcile(db, 'D1')
    second, out2 = reconcile(db, 'D1')
    assert first != second and out1 == out2 == {'M': '可封账'}
    data = json.loads(write(db, first, tmp_path / 'first').read_text(encoding='utf-8'))
    assert data['run']['effective_status'] == '已判定'
    assert data['run']['superseded_by'] is None


def test_authorize_only_transaction_contributes_zero_like_before(tmp_path):
    db = base_db(tmp_path)
    p = write_events(tmp_path, [
        {'merchant': 'M', 'tx': 'T', 'version': 1, 'kind': 'authorize', 'amount': '100.00',
         'business_at': '2026-01-01T01:00:00+00:00'},
    ])
    stream(p, db)
    batch(write_batch(tmp_path, '0.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    assert out == {'M': '可封账'}
    assert verdict(db, run)['stream_net'] == '0.00'


def test_authorize_only_report_renders_without_confirm(tmp_path):
    db = base_db(tmp_path)
    stream(write_events(tmp_path, [
        {'merchant': 'M', 'tx': 'T', 'version': 1, 'kind': 'authorize', 'amount': '100.00',
         'business_at': '2026-01-01T01:00:00+00:00'}]), db)
    batch(write_batch(tmp_path, '0.00'), db, 'D1')
    run, out = reconcile(db, 'D1')
    data = json.loads(write(db, run, tmp_path / 'r').read_text(encoding='utf-8'))
    assert data['transactions'][0]['confirm'] is None
    assert '无确认' in (tmp_path / 'r.md').read_text(encoding='utf-8')
