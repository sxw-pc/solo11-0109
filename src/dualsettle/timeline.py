from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from .contract import aware, validate_currency, validate_fx_rate, validate_settlement_agreement, FxFailure

Q = Decimal('0.01')


def at(s):
    return aware(s)


def window_for(t, windows):
    hits = [w for w in windows if at(w['start_at']) <= t < at(w['end_at'])]
    if len(hits) != 1:
        raise ValueError('window_ambiguous')
    return hits[0]['id']


def find_fx(db, base, quote, when):
    """Resolve exactly one directional quote effective at ``when``.

    Only base -> quote is considered; the reverse pair is never inferred from a
    reciprocal.  Half-open intervals make a timestamp on a version boundary hit
    exactly one version; a gap or overlap is a failure, never a 1:1 default.
    """
    rows = [dict(r) for r in db.execute('SELECT * FROM fx_rate_version WHERE base=? AND quote=?', (base, quote))
            if at(r['start_at']) <= when < at(r['end_at'])]
    for row in rows:
        try:
            validate_fx_rate(row)
        except ValueError:
            raise FxFailure('fx_rate_invalid') from None
    if not rows:
        raise FxFailure('fx_rate_missing')
    if len(rows) != 1:
        raise FxFailure('fx_rate_ambiguous')
    row = rows[0]
    return row, Decimal(row['quote_rate'])


def _settlement_currency(db, merchant):
    row = db.execute('SELECT * FROM merchant_currency WHERE merchant=?', (merchant,)).fetchone()
    return row['currency'] if row else None


def select_route(db, merchant, window):
    """Return the route selected in advance for this merchant/window.

    A frozen contract wins; otherwise exactly one agreement effective over the
    whole window must exist.  Overlap or an invalid route makes conversion
    evidence insufficient instead of being resolved arbitrarily.
    """
    frozen = db.execute('SELECT * FROM frozen_contract WHERE merchant=? AND window_id=?',
                        (merchant, window['id'])).fetchone()
    if frozen:
        agreement = db.execute('SELECT * FROM settlement_agreement WHERE id=?', (frozen['agreement_id'],)).fetchone()
        if not agreement:
            raise FxFailure('contract_unknown')
        try:
            validate_settlement_agreement(dict(agreement))
        except ValueError:
            raise FxFailure('contract_invalid') from None
        if agreement['route'] != frozen['route'] or agreement['digest'] != frozen['digest']:
            # The frozen identity no longer matches its agreement; do not
            # silently substitute the updated route.
            raise FxFailure('contract_changed')
        return {'route': frozen['route'], 'agreement_id': frozen['agreement_id'],
                'digest': frozen['digest'], 'mode': 'frozen'}
    agreements = [dict(row) for row in db.execute('SELECT * FROM settlement_agreement WHERE merchant=?', (merchant,))
                  if at(row['start_at']) <= at(window['start_at']) and at(window['end_at']) <= at(row['end_at'])]
    if not agreements:
        return None
    if len(agreements) != 1:
        raise FxFailure('contract_ambiguous')
    agreement = agreements[0]
    return {'route': agreement['route'], 'agreement_id': agreement['id'],
            'digest': agreement['digest'], 'mode': 'agreement'}


def _money(value):
    return value.quantize(Q, rounding=ROUND_HALF_UP)


def _convert(amount, rate, base, quote, event, when, row, role, needs_fx):
    exact = amount * rate
    converted = _money(exact) if needs_fx else exact
    return {
        'role': role,
        'event_id': event['id'],
        'tx_version': event['version'],
        'at': when.isoformat(),
        'base': base,
        'quote': quote,
        'fx_rate_id': row['id'] if row else None,
        'fx_digest': row['digest'] if row else None,
        'fx_source': row['source'] if row else None,
        'fx_start_at': row['start_at'] if row else None,
        'fx_end_at': row['end_at'] if row else None,
        'quote_rate': str(rate),
        'original_amount': str(amount),
        'exact_converted': str(exact),
        'converted': str(converted),
        'rounding_residual': str(exact - converted),
    }


def _identity(role, event, mode, base, quote, row, quote_rate):
    return {
        'role': role, 'event_id': event['id'], 'tx_version': event['version'],
        'mode': mode, 'base': base, 'quote': quote,
        'fx_rate_id': row['id'] if row else None,
        'fx_digest': row['digest'] if row else None,
        'fx_source': row['source'] if row else None,
        'quote_rate': str(quote_rate),
    }


def merge_events(events):
    """Merge event rows into one logical transaction keyed by merchant + tx.

    Original-currency conservation is checked *before* any conversion: each
    partial refund accumulates in the original currency and may never exceed
    the confirmed original amount.  Currency statements are part of the merged
    evidence.
    """
    groups = defaultdict(list)
    for e in events:
        groups[(e['merchant'], e['tx'])].append(e)
    for key, items in groups.items():
        items.sort(key=lambda e: (at(e['business_at']), e['version']))
        confirmed = Decimal(0)
        confirmed_event = None
        authorized = False
        authorized_amount = Decimal(0)
        refunds = []
        refunded = Decimal(0)
        valid = True
        reason = 'bad_state'
        currencies = set()
        currency_invalid = False
        for e in items:
            raw = e.get('currency')
            if raw is None:
                currencies.add(None)
            else:
                try:
                    currencies.add(validate_currency(raw))
                except ValueError:
                    currency_invalid = True
                    valid = False
                    reason = 'currency_invalid'
            try:
                amount = Decimal(e['amount'])
            except (InvalidOperation, TypeError, ValueError):
                valid = False
                continue
            if not amount.is_finite() or amount <= 0:
                valid = False
                continue
            kind = e['kind']
            if kind == 'authorize':
                if authorized or confirmed_event is not None:
                    valid = False
                authorized = True
                authorized_amount = amount
            elif kind == 'confirm':
                if not authorized or confirmed_event is not None or amount > authorized_amount:
                    valid = False
                confirmed = amount
                confirmed_event = e
            elif kind == 'cancel':
                if not authorized or confirmed_event is not None:
                    valid = False
                authorized = False
            elif kind == 'refund':
                refunded += amount
                refunds.append({'amount': amount, 'event': e})
                if confirmed_event is None:
                    valid = False
                elif refunded > confirmed:
                    # Original-currency conservation is checked first, before
                    # any conversion, so batch amounts can never mask it.
                    valid = False
                    reason = 'refund_exceeds_confirm'
        explicit = {c for c in currencies if c is not None}
        if currency_invalid:
            currency = None
        elif len(explicit) > 1 or (explicit and None in currencies):
            valid = False
            reason = 'currency_conflict'
            currency = None
        else:
            currency = next(iter(explicit)) if explicit else None
        yield {
            'merchant': key[0], 'tx': key[1], 'events': items, 'currency': currency,
            'authorized_amount': authorized_amount, 'confirmed_amount': confirmed,
            'confirm_event': confirmed_event, 'refunded_amount': refunded,
            'refunds': refunds, 'valid': valid, 'reason': reason,
        }


def transactions(events, db=None, window=None):
    """Merge events and, when context is supplied, convert to settlement money.

    The conversion basis is decided here so that a new refund-time FX version
    changes the merged/refund conversion evidence itself.  ``original_lock``
    converts every partial refund at the confirmation-time quote;
    ``refund_spot`` matches each refund's own business time.  Without db/window
    the merged original-currency amounts are yielded unchanged.
    """
    for t in merge_events(events):
        if db is None or window is None:
            yield t
            continue
        t['settlement_currency'] = _settlement_currency(db, t['merchant'])
        t['conversion_error'] = None
        if not t['valid']:
            yield t
            continue
        try:
            _convert_transaction(db, t, window)
        except FxFailure as exc:
            t['conversion_error'] = exc.code
            t['valid'] = False
            t['reason'] = exc.code
        yield t


def _convert_transaction(db, t, window):
    merchant = t['merchant']
    tx_currency = t['currency']
    settle = t['settlement_currency']
    confirm = t['confirm_event']
    if confirm is None:
        # Authorize/cancel-only transactions contribute zero.  This preserves
        # the legacy meaning; a refund without a confirm is already invalid.
        t['route'] = 'no_confirm'
        t['contract_agreement_id'] = None
        t['contract_digest'] = None
        t['confirm_fx'] = None
        t['refund_fx'] = []
        t['gross_settlement'] = Decimal(0)
        t['refund_settlement'] = Decimal(0)
        t['rounding_residual'] = '0'
        return
    if tx_currency is not None and settle is None:
        # A known original currency with no settlement currency cannot be
        # proven identical: it must not silently reuse the original amount.
        raise FxFailure('settlement_currency_missing')
    if tx_currency is None and settle is not None:
        # Once a settlement currency is declared, a currencyless event can no
        # longer be assumed to be 1:1 with it.  Truly legacy fixtures (no
        # settlement currency configured) remain 1:1 compatible.
        raise FxFailure('currency_missing')
    needs_fx = tx_currency is not None and settle is not None and tx_currency != settle
    route = select_route(db, merchant, window) if needs_fx else None
    if needs_fx and route is None:
        raise FxFailure('contract_missing')
    if needs_fx:
        confirm_row, confirm_rate = find_fx(db, tx_currency, settle, at(confirm['business_at']))
        mode = 'fx_confirm'
    elif tx_currency is None:
        confirm_row, confirm_rate = None, Decimal(1)
        mode = 'legacy_currencyless_1:1'
    else:
        confirm_row, confirm_rate = None, Decimal(1)
        mode = 'same_currency'
    confirm_fx = _identity('confirm', confirm, mode, tx_currency, settle, confirm_row, confirm_rate)
    convert = _convert(t['confirmed_amount'], confirm_rate, tx_currency, settle,
                       confirm, at(confirm['business_at']), confirm_row, 'confirm', needs_fx)
    convert['mode'] = mode
    confirm_fx.update(convert)

    refund_total = Decimal(0)
    refund_fx = []
    for refund in t['refunds']:
        amount = refund['amount']
        event = refund['event']
        if needs_fx and route['route'] == 'original_lock':
            row, rate = confirm_row, confirm_rate
            when = at(confirm['business_at'])
            role = 'refund_original_lock'
        elif needs_fx:
            when = at(event['business_at'])
            row, rate = find_fx(db, tx_currency, settle, when)
            role = 'refund_spot'
        else:
            when = at(event['business_at'])
            row, rate = None, Decimal(1)
            role = 'refund_same_currency' if tx_currency is not None else 'refund_legacy_1:1'
        entry = _convert(amount, rate, tx_currency, settle, event, when, row, role, needs_fx)
        if needs_fx:
            entry['route'] = route['route']
            entry['contract_agreement_id'] = route['agreement_id']
        refund_total += Decimal(entry['converted'])
        refund_fx.append(entry)

    # Original-currency conservation was enforced before conversion.  The
    # settlement-currency total is whatever the contract's quotes produce; it
    # is compared with the batch, never clamped or offset to fit it.
    gross = Decimal(confirm_fx['converted'])

    t['route'] = route['route'] if route else ('same_currency' if not needs_fx else None)
    t['contract_agreement_id'] = route['agreement_id'] if route else None
    t['contract_digest'] = route['digest'] if route else None
    t['confirm_fx'] = confirm_fx
    t['refund_fx'] = refund_fx
    t['gross_settlement'] = gross
    t['refund_settlement'] = refund_total
    t['rounding_residual'] = str(sum((Decimal(x['rounding_residual']) for x in refund_fx), Decimal(0)))
