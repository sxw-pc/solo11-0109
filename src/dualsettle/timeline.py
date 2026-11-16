from collections import defaultdict
from datetime import datetime,timedelta
from decimal import Decimal
from .contract import aware
def at(s):
    return aware(s)
def window_for(t,windows):
    hits=[w for w in windows if at(w['start_at'])<=t<at(w['end_at'])]
    if len(hits)!=1:raise ValueError('window_ambiguous')
    return hits[0]['id']
def transactions(events):
    groups=defaultdict(list)
    for e in events:groups[(e['merchant'],e['tx'])].append(e)
    for key,items in groups.items():
        items.sort(key=lambda e:(at(e['business_at']),e['version']))
        confirmed=Decimal('0');refunded=Decimal('0');authorized=False;authorized_amount=Decimal('0');valid=True
        for e in items:
            try:amount=Decimal(e['amount'])
            except Exception:valid=False;continue
            if not amount.is_finite() or amount<=0:valid=False;continue
            if e['kind']=='authorize':
                if authorized or confirmed:valid=False
                authorized=True;authorized_amount=amount
            elif e['kind']=='confirm':
                if not authorized or confirmed or amount>authorized_amount:valid=False
                confirmed=amount
            elif e['kind']=='cancel':
                if not authorized or confirmed:valid=False
                authorized=False
            elif e['kind']=='refund':
                refunded+=amount
                if not confirmed or refunded>confirmed:valid=False
        yield key,items,confirmed,refunded,valid
