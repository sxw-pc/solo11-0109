import hashlib,json
from decimal import Decimal,ROUND_HALF_UP
from .store import connect
from .timeline import at,window_for,transactions
from .contract import validate_window,validate_rate
Q=Decimal('0.01')
def money(x):return str(Decimal(x).quantize(Q,rounding=ROUND_HALF_UP))
def input_digest(db):
    rows=[]
    for table,cols in [('transaction_event','sha,business_at,arrival_at'),('batch_line','id,amount,corrects'),('rate_version','merchant,start_at,end_at,rate'),('adjustment','window_id,merchant,amount,reason'),('window','id,start_at,end_at,cutoff_at,version')]:
        rows.append((table,[tuple(r) for r in db.execute(f'SELECT {cols} FROM {table} ORDER BY 1,2')]))
    return hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()
def reconcile(path,window_id):
    db=connect(path);result={}
    with db:
        db.execute("DELETE FROM issue WHERE code IN ('bad_state','window_ambiguous','rate_ambiguous','late_arrival')")
        window=db.execute('SELECT * FROM window WHERE id=?',(window_id,)).fetchone()
        if not window:raise ValueError('unknown_window')
        validate_window(window)
        windows=[dict(x) for x in db.execute('SELECT * FROM window')]
        events=[dict(x) for x in db.execute('SELECT * FROM transaction_event')]
        dig=input_digest(db)
        run=db.execute("INSERT INTO reconcile_run(window_id,status,source_digest) VALUES (?,'待接收',?)",(window_id,dig)).lastrowid
        db.execute("UPDATE reconcile_run SET status='已校验' WHERE id=?",(run,))
        aggregates={};unresolved=set()
        for (merchant,tx),items,confirmed,refunded,valid in transactions(events):
            try:wid=window_for(at(items[0]['business_at']),windows)
            except ValueError:
                unresolved.add(merchant);db.execute('INSERT INTO issue(code,merchant,tx) VALUES (?,?,?)',('window_ambiguous',merchant,tx));continue
            if wid!=window_id:continue
            if any(at(e['arrival_at'])>at(window['cutoff_at']) for e in items):
                unresolved.add(merchant);db.execute('INSERT INTO issue(code,merchant,tx) VALUES (?,?,?)',('late_arrival',merchant,tx));continue
            if not valid:
                unresolved.add(merchant);db.execute('INSERT INTO issue(code,merchant,tx) VALUES (?,?,?)',('bad_state',merchant,tx));continue
            rates=[r for r in db.execute('SELECT * FROM rate_version WHERE merchant=?',(merchant,)) if at(r['start_at'])<=at(items[0]['business_at'])<at(r['end_at'])]
            for rate in rates:validate_rate(rate)
            if len(rates)!=1:
                unresolved.add(merchant);db.execute('INSERT INTO issue(code,merchant,tx) VALUES (?,?,?)',('rate_ambiguous',merchant,tx));continue
            fee=(confirmed*Decimal(rates[0]['rate'])).quantize(Q,rounding=ROUND_HALF_UP)
            a=aggregates.setdefault(merchant,{'gross':Decimal(0),'fee':Decimal(0),'refund':Decimal(0),'ids':[]})
            a['gross']+=confirmed;a['fee']+=fee;a['refund']+=refunded;a['ids'] += [e['id'] for e in items]
            for e in items:db.execute('INSERT INTO evidence(run_id,merchant,event_id,rate_id,detail) VALUES (?,?,?,?,?)',(run,merchant,e['id'],rates[0]['id'],'stream'))
        merchants={r['id'] for r in db.execute('SELECT * FROM account')}|{r['merchant'] for r in db.execute('SELECT * FROM batch_line WHERE window_id=?',(window_id,))}
        for merchant in sorted(merchants):
            a=aggregates.get(merchant,{'gross':Decimal(0),'fee':Decimal(0),'refund':Decimal(0),'ids':[]})
            net=a['gross']-a['fee']-a['refund']
            db.execute('INSERT OR REPLACE INTO stream_aggregate VALUES (?,?,?,?,?,?,?)',(window_id,merchant,money(a['gross']),money(a['fee']),money(a['refund']),money(net),json.dumps(a['ids'])))
            lines=[dict(x) for x in db.execute('SELECT * FROM batch_line WHERE window_id=? AND merchant=?',(window_id,merchant))]
            batch_net=sum((Decimal(x['amount']) for x in lines),Decimal(0))
            adjustment=sum((Decimal(x['amount']) for x in db.execute('SELECT amount FROM adjustment WHERE window_id=? AND merchant=?',(window_id,merchant))),Decimal(0))
            for line in lines:db.execute('INSERT INTO evidence(run_id,merchant,batch_id,detail) VALUES (?,?,?,?)',(run,merchant,line['id'],'batch'))
            issues=[r['code'] for r in db.execute('SELECT code FROM issue WHERE merchant=?',(merchant,))]
            if merchant in unresolved or issues:status='证据不足';reason=','.join(sorted(set(issues)))
            elif not lines:status='证据不足';reason='missing_batch'
            elif net+adjustment!=batch_net:status='差异';reason='amount_mismatch'
            else:status='可封账';reason='matched'
            db.execute('INSERT INTO verdict VALUES (?,?,?,?,?,?)',(run,merchant,status,money(net),money(batch_net),reason));result[merchant]=status
        db.execute("UPDATE reconcile_run SET status=?,ended_at=CURRENT_TIMESTAMP WHERE id=?",('待核' if any(v!='可封账' for v in result.values()) else '已判定',run))
    db.close();return run,result
def close(path,run):
    db=connect(path)
    with db:
        r=db.execute('SELECT * FROM reconcile_run WHERE id=?',(run,)).fetchone()
        if not r or r['status']!='已判定':raise ValueError('not_closable')
        if input_digest(db)!=r['source_digest']:raise ValueError('source_changed')
        db.execute("UPDATE reconcile_run SET status='已归档' WHERE id=?",(run,))
    db.close()
