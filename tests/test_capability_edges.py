import csv,json
import pytest
from dualsettle.store import connect
from dualsettle.ingest import stream,batch
from dualsettle.rules import reconcile,close
from dualsettle.report import write
def setup(tmp,rate='0.01'):
    db=tmp/'x.db';c=connect(db)
    with c:
        c.execute("INSERT INTO window VALUES ('D','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        c.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',('M','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00',rate))
    return db
def events(tmp,db,values):
    p=tmp/'e.jsonl';p.write_text('\n'.join(json.dumps({'merchant':'M','tx':'T','version':i,'kind':kind,'amount':amount,'business_at':f'2026-01-01T0{i}:00:00+00:00'}) for i,(kind,amount) in enumerate(values,1)),encoding='utf-8')
    return stream(p,db)
def lines(tmp,db,amount):
    p=tmp/'b.csv'
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B','M',amount,''])
    return batch(p,db,'D')
def test_event_identity_normal_and_conflict(tmp_path):
    db=setup(tmp_path);assert events(tmp_path,db,[('authorize','100'),('confirm','100')])==2
    assert stream(tmp_path/'e.jsonl',db)==0
    assert connect(db).execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==2
def test_legal_refund_reduces_net_without_negative(tmp_path):
    db=setup(tmp_path);events(tmp_path,db,[('authorize','100'),('confirm','100'),('refund','10')]);lines(tmp_path,db,'89')
    run,out=reconcile(db,'D');assert out['M']=='可封账'
    assert connect(db).execute('SELECT net FROM stream_aggregate').fetchone()[0]=='89.00'
def test_exact_window_end_excluded(tmp_path):
    from dualsettle.timeline import window_for,at
    db=setup(tmp_path);c=connect(db);windows=[dict(x) for x in c.execute('SELECT * FROM window')]
    assert window_for(at('2026-01-01T00:00:00+00:00'),windows)=='D'
    with pytest.raises(ValueError,match='window_ambiguous'):window_for(at('2026-01-02T00:00:00+00:00'),windows)
def test_fee_rounding_and_missing_rate(tmp_path):
    db=setup(tmp_path,rate='0.015');events(tmp_path,db,[('authorize','1.00'),('confirm','1.00')]);lines(tmp_path,db,'0.98')
    assert reconcile(db,'D')[1]['M']=='可封账'
    with connect(db) as c:c.execute("UPDATE rate_version SET start_at='2026-01-02T00:00:00+00:00'")
    assert reconcile(db,'D')[1]['M']=='证据不足'
def test_batch_correction_and_missing_reference(tmp_path):
    db=setup(tmp_path);p=tmp_path/'b.csv'
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B1','M','98','']);w.writerow(['B2','M','1','B1']);w.writerow(['B3','M','1','absent'])
    assert batch(p,db,'D')==2
    assert connect(db).execute("SELECT COUNT(*) FROM issue WHERE code='orphan_correction'").fetchone()[0]==1
def test_report_links_event_rate_batch_and_source(tmp_path):
    db=setup(tmp_path);events(tmp_path,db,[('authorize','100'),('confirm','100')]);lines(tmp_path,db,'99')
    run,_=reconcile(db,'D');data=json.loads(write(db,run,tmp_path/'r').read_text(encoding='utf-8'))
    assert any(e['event_id'] and e['rate_id'] for e in data['evidence'])
    assert any(e['batch_id'] for e in data['evidence']) and len(data['sources'])==2
