import csv,json
import pytest
from dualsettle.store import connect
from dualsettle.ingest import stream,batch
from dualsettle.rules import reconcile,close
from dualsettle.report import write
def setup(tmp,rate='0.01'):
    db=tmp/'x.db';c=connect(db)
    with c:
        c.execute("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
        c.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',('M','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00',rate))
    c.close();return db
def event(kind,version,amount='100.00',time=None):return {'merchant':'M','tx':'T','version':version,'kind':kind,'amount':amount,'business_at':time or f'2026-01-01T0{version}:00:00+00:00'}
def load(tmp,db,events,batch_amount='99.00'):
    p=tmp/'events.jsonl';p.write_text('\n'.join(json.dumps(x) for x in events),encoding='utf-8');stream(p,db)
    if batch_amount is not None:
        p=tmp/'batch.csv'
        with p.open('w',newline='',encoding='utf-8') as f:
            w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B','M',batch_amount,''])
        batch(p,db,'D1')
def test_matched_close_and_evidence(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)])
    run,out=reconcile(db,'D1');assert out=={'M':'可封账'}
    close(db,run);data=json.loads(write(db,run,tmp_path/'out').read_text(encoding='utf-8'))
    assert data['verdicts'][0]['stream_net']=='99.00' and len(data['evidence'])==3
def test_repeat_and_conflict(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)])
    assert stream(tmp_path/'events.jsonl',db)==0
    p=tmp_path/'conflict.jsonl';p.write_text(json.dumps(event('confirm',2,'200.00')))
    stream(p,db);assert reconcile(db,'D1')[1]['M']=='证据不足'
def test_illegal_refund(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2),event('refund',3,'120.00')])
    assert reconcile(db,'D1')[1]['M']=='证据不足'
def test_window_half_open_and_missing_timezone(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1,time='2026-01-02T00:00:00+00:00'),event('confirm',2,time='2026-01-02T00:01:00+00:00')])
    assert reconcile(db,'D1')[1]['M']=='证据不足'
    from dualsettle.timeline import at
    with pytest.raises(ValueError,match='timezone_required'):at('2026-01-01T02:00:00')
def test_rate_gap_and_overlap(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)])
    c=connect(db)
    with c:c.execute('DELETE FROM rate_version')
    assert reconcile(db,'D1')[1]['M']=='证据不足'
def test_amount_difference_and_no_close(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)],'98.00')
    run,out=reconcile(db,'D1');assert out['M']=='差异'
    with pytest.raises(ValueError,match='not_closable'):close(db,run)
def test_missing_batch(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)],None)
    assert reconcile(db,'D1')[1]['M']=='证据不足'
def test_correction_requires_prior_line(tmp_path):
    db=setup(tmp_path);p=tmp_path/'b.csv'
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B2','M','1.00','missing'])
    assert batch(p,db,'D1')==0
    assert connect(db).execute("SELECT COUNT(*) FROM issue WHERE code='orphan_correction'").fetchone()[0]==1
def test_adjustment_does_not_mutate_stream(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2)],'100.00')
    c=connect(db)
    with c:c.execute('INSERT INTO adjustment(window_id,merchant,amount,reason) VALUES (?,?,?,?)',('D1','M','1.00','reviewed'))
    run,out=reconcile(db,'D1');assert out['M']=='可封账'
    assert connect(db).execute('SELECT net FROM stream_aggregate').fetchone()[0]=='99.00'
def test_late_arrival_and_input_change(tmp_path):
    db=setup(tmp_path)
    a=event('authorize',1);a['arrival_at']='2026-01-03T00:00:00+00:00'
    load(tmp_path,db,[a,event('confirm',2)])
    assert reconcile(db,'D1')[1]['M']=='证据不足'
    c=connect(db)
    with c:c.execute("UPDATE transaction_event SET arrival_at='2026-01-01T01:00:00+00:00'")
    run,out=reconcile(db,'D1')
    assert out['M']=='可封账'
    with c:c.execute('UPDATE rate_version SET rate=?',('0.02',))
    with pytest.raises(ValueError,match='source_changed'):close(db,run)
def test_rate_overlap_and_refund(tmp_path):
    db=setup(tmp_path);load(tmp_path,db,[event('authorize',1),event('confirm',2),event('refund',3,'10.00')],'89.00')
    assert reconcile(db,'D1')[1]['M']=='可封账'
    c=connect(db)
    with c:c.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',('M','2026-01-01T00:00:00+00:00','2026-01-02T00:00:00+00:00','0.02'))
    assert reconcile(db,'D1')[1]['M']=='证据不足'
def test_batch_correction_chain(tmp_path):
    db=setup(tmp_path);p=tmp_path/'batch.csv'
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['id','merchant','amount','corrects']);w.writerow(['B1','M','98.00','']);w.writerow(['B2','M','1.00','B1'])
    assert batch(p,db,'D1')==2
    assert connect(db).execute('SELECT COUNT(*) FROM batch_line WHERE corrects=?',('B1',)).fetchone()[0]==1
