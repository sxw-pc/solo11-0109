import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

CURRENCY=re.compile(r'^[A-Z]{3}$')
ROUTES=('original_lock','refund_spot')


class FxFailure(Exception):
    """Conversion evidence is insufficient; no net may be sealed."""
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def aware(s):
    value=datetime.fromisoformat(str(s).replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('timezone_required')
    return value


def validate_window(window):
    if not aware(window['start_at'])<aware(window['end_at'])<=aware(window['cutoff_at']):raise ValueError('window_bounds')


def validate_rate(rate):
    """Merchant fee rate: an existing [0,1) proportion, not an FX quote."""
    if not aware(rate['start_at'])<aware(rate['end_at']):raise ValueError('rate_bounds')
    try:value=Decimal(rate['rate'])
    except (InvalidOperation,TypeError,ValueError):raise ValueError('rate_invalid') from None
    if not value.is_finite() or not 0<=value<1:raise ValueError('rate_invalid')


def validate_currency(currency):
    if not isinstance(currency,str) or not CURRENCY.fullmatch(currency):
        raise ValueError('currency_invalid')
    return currency


def validate_fx_rate(rate):
    """FX quote: 1 unit of base buys quote_rate units of quote.

    The pair direction is part of the quote identity.  A positive finite quote
    is required; no reciprocal is derived here or anywhere else.
    """
    try:base=validate_currency(rate['base']);quote=validate_currency(rate['quote'])
    except KeyError:raise ValueError('fx_invalid') from None
    if base==quote:raise ValueError('fx_pair_invalid')
    if not aware(rate['start_at'])<aware(rate['end_at']):raise ValueError('fx_bounds')
    try:value=Decimal(rate['quote_rate'])
    except (InvalidOperation,TypeError,ValueError,KeyError):raise ValueError('fx_invalid') from None
    if not value.is_finite() or value<=0:raise ValueError('fx_invalid')
    return value


def validate_settlement_agreement(agreement):
    try:route=agreement['route'];start=agreement['start_at'];end=agreement['end_at'];version=agreement['version']
    except KeyError:raise ValueError('contract_invalid') from None
    if route not in ROUTES:raise ValueError('contract_route')
    if not aware(start)<aware(end):raise ValueError('contract_bounds')
    try:version=int(version)
    except (TypeError,ValueError):raise ValueError('contract_invalid') from None
    if version<0:raise ValueError('contract_invalid')
    return version


def _digest(payload):
    return hashlib.sha256(json.dumps(payload,sort_keys=True,default=str).encode('utf-8')).hexdigest()


def fx_digest(rate):
    return _digest({'base':rate.get('base'),'quote':rate.get('quote'),'start_at':rate.get('start_at'),
                    'end_at':rate.get('end_at'),'quote_rate':str(rate.get('quote_rate')),
                    'source':rate.get('source')})


def agreement_digest(agreement):
    return _digest({'merchant':agreement.get('merchant'),'route':agreement.get('route'),
                    'start_at':agreement.get('start_at'),'end_at':agreement.get('end_at'),
                    'version':agreement.get('version'),'source':agreement.get('source')})
