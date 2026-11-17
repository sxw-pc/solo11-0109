"""Resumable segment ingest: checkpoints, crash recovery, tamper isolation.

These tests are pure stdlib + the project package and use fault-injection
hooks (``before_commit`` / ``after_commit``) instead of real process kills,
so the same assertions hold under real pytest and under the offline shim.
"""
import csv,json
import pytest
from dualsettle.store import connect
from dualsettle.ingest import (stream,batch,stream_resumable,visible_events,
                               import_progress,iter_segments,ImportFailure)
from dualsettle.rules import reconcile,close
from dualsettle.report import write

WINDOW=("INSERT INTO window VALUES ('D1','2026-01-01T00:00:00+00:00',"
        "'2026-01-02T00:00:00+00:00','2026-01-02T01:00:00+00:00',1)")
RATE=("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES "
      "('M1','2025-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00','0.01')")

def ev(tx,version,kind,amount):
    return json.dumps({'merchant':'M1','tx':tx,'version':version,'kind':kind,
                       'amount':amount,'business_at':'2026-01-01T01:00:00+00:00'})

def two_segment_file(path):
    """A=100 pair then B=20 pair; budget below cuts exactly between them."""
    rows=[ev('A',1,'authorize','100.00'),ev('A',2,'confirm','100.00'),
          ev('B',1,'authorize','20.00'),ev('B',2,'confirm','20.00')]
    text='\n'.join(rows)+'\n'
    path.write_text(text,encoding='utf-8')
    budget=len((rows[0]+'\n'+rows[1]+'\n').encode())
    return budget

def seed(tmp_path,batch_amount='118.80'):
    db=tmp_path/'db.sqlite'
    con=connect(db)
    with con:
        con.execute(WINDOW);con.execute(RATE)
    con.close()
    if batch_amount is not None:
        bp=tmp_path/'batch.csv'
        with bp.open('w',newline='',encoding='utf-8') as f:
            w=csv.writer(f);w.writerow(['id','merchant','amount','corrects'])
            w.writerow(['B1','M1',batch_amount,''])
        batch(bp,db,'D1')
    return db

def crash_after_segment_zero(run_id,seq):
    if seq==0:raise RuntimeError('injected_crash_after_segment_0')

# ---------------------------------------------------------------- segmenter
def test_iter_segments_never_splits_a_record(tmp_path):
    p=tmp_path/'f'
    data=b''.join(b'row-%d\n'%i for i in range(20))
    p.write_bytes(data)
    for budget in (1,2,3,8,31,128,4096):
        got=b'';pos=0;line=1
        for seg,start,line_start in iter_segments(p,budget):
            assert start==pos and line_start==line
            assert len(seg)<=budget or seg.count(b'\n')==1
            got+=seg;pos+=len(seg);line+=seg.count(b'\n')
        assert got==data

def test_iter_segments_handles_missing_trailing_newline_and_empty(tmp_path):
    p=tmp_path/'f';p.write_bytes(b'a\nb')
    pieces=list(iter_segments(p,10))
    assert b''.join(x[0] for x in pieces)==b'a\nb'
    empty=tmp_path/'e';empty.write_bytes(b'')
    assert list(iter_segments(empty,10))==[]

# --------------------------------------------------- acceptance scenario A/B
def test_whole_acceptance_hides_everything_until_full_sha_matches(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError,match='injected_crash'):
        stream_resumable(p,db,budget=budget,accept='whole',
                         hooks={'after_commit':crash_after_segment_zero})
    con=connect(db)
    # No formal visibility, and whole-policy segments never enter previews.
    assert len(visible_events(con))==0
    assert len(visible_events(con,preview=True))==0
    run,_=reconcile(db,'D1')
    row=con.execute('SELECT mode,preview,sealable FROM reconcile_run WHERE id=?',(run,)).fetchone()
    assert tuple(row)==('formal',0,0)
    with pytest.raises(ValueError,match='not_sealable_preview'):close(db,run)
    # Recover from the last verified boundary: remaining B pair is new and
    # nothing from segment zero is re-counted.
    rid,new=stream_resumable(p,db,budget=budget,accept='whole')
    assert new==2
    final,verdicts=reconcile(db,'D1');close(db,final)
    con=connect(db)
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4
    assert con.execute('SELECT status FROM import_run WHERE id=?',(rid,)).fetchone()[0]=='completed'
    assert verdicts=={'M1':'可封账'}

def test_segment_acceptance_preview_shows_only_verified_segments(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError,match='injected_crash'):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':crash_after_segment_zero})
    con=connect(db)
    assert len(visible_events(con))==0  # not sealable-visible yet
    preview=visible_events(con,preview=True)
    assert sorted((r['tx'],r['version']) for r in preview)==[('A',1),('A',2)]
    # Preview reconcile is explicitly non-sealing and close is refused.
    prun,verdicts=reconcile(db,'D1',preview=True)
    row=con.execute('SELECT mode,preview,sealable FROM reconcile_run WHERE id=?',(prun,)).fetchone()
    assert tuple(row)==('preview',1,0)
    with pytest.raises(ValueError,match='not_sealable_preview'):close(db,prun)
    # Resume; only the remaining B pair is newly committed, nothing doubles.
    _rid,new=stream_resumable(p,db,budget=budget,accept='segment')
    assert new==2
    final,verdicts=reconcile(db,'D1');close(db,final)
    con=connect(db)
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4
    ids=[r[0] for r in con.execute('SELECT id FROM transaction_event ORDER BY id')]
    assert len(set(ids))==4
    assert sorted(r[0] for r in con.execute('SELECT DISTINCT amount FROM transaction_event'))==['100.00','20.00']
    assert verdicts=={'M1':'可封账'}

# ------------------------------------------------------------- crash safety
def test_pre_commit_crash_leaves_no_partial_segment(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    def boom(run_id,seq):
        if seq==1:raise RuntimeError('injected_pre_commit_crash')
    with pytest.raises(RuntimeError,match='injected_pre_commit'):
        stream_resumable(p,db,budget=budget,accept='segment',hooks={'before_commit':boom})
    con=connect(db)
    assert con.execute('SELECT COUNT(*) FROM source_segment').fetchone()[0]==1
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==2
    # Recovery does not re-count segment zero and finishes cleanly.
    _rid,new=stream_resumable(p,db,budget=budget,accept='segment')
    assert new==2
    assert connect(db).execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4

def test_repeated_resume_is_idempotent_and_keeps_line_continuity(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    calls={'n':0}
    def alternating_crash(run_id,seq):
        calls['n']+=1
        if calls['n']==1:raise RuntimeError('crash once')
    with pytest.raises(RuntimeError):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':alternating_crash})
    stream_resumable(p,db,budget=budget,accept='segment')
    # A third invocation against the completed run adds nothing.
    rid,new=stream_resumable(p,db,budget=budget,accept='segment')
    assert new==0
    con=connect(db)
    lines=[n for s in con.execute('SELECT line_start,line_end FROM source_segment ORDER BY seq')
           for n in range(s['line_start'],s['line_end']+1)]
    assert lines==[1,2,3,4]
    assert con.execute('SELECT next_line_no FROM import_run WHERE id=?',(rid,)).fetchone()[0]==5

def test_record_crossing_segment_boundary_with_tiny_budget(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';two_segment_file(p)
    rid,new=stream_resumable(p,db,budget=7,accept='whole')
    assert new==4
    con=connect(db);segs=con.execute('SELECT * FROM source_segment ORDER BY seq').fetchall()
    assert [n for s in segs for n in range(s['line_start'],s['line_end']+1)]==[1,2,3,4]
    assert segs[0]['byte_start']==0 and segs[-1]['byte_end']==p.stat().st_size
    assert all(segs[i]['byte_end']==segs[i+1]['byte_start'] for i in range(len(segs)-1))
    import hashlib
    raw=p.read_bytes()
    for s in segs:
        assert hashlib.sha256(raw[s['byte_start']:s['byte_end']]).hexdigest()==s['byte_sha']

# ------------------------------------------------------------- tamper / lock
def test_changed_source_bytes_quarantine_old_progress(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':crash_after_segment_zero})
    data=bytearray(p.read_bytes());data[10]=ord('Z');p.write_bytes(bytes(data))
    with pytest.raises(ImportFailure,match='source_bytes_changed'):
        stream_resumable(p,db,budget=budget,accept='segment')
    con=connect(db)
    row=con.execute('SELECT status,error_code FROM import_run').fetchone()
    assert tuple(row)==('quarantined','source_bytes_changed')
    # Even restoring the original bytes cannot revive the isolated checkpoint.
    two_segment_file(p)
    with pytest.raises(ImportFailure,match='import_quarantined'):
        stream_resumable(p,db,budget=budget,accept='segment')
    # Old events never leak into either visibility channel.
    assert len(visible_events(connect(db)))==0
    assert len(visible_events(connect(db),preview=True))==0

def test_truncated_source_is_detected_at_resume(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':crash_after_segment_zero})
    p.write_bytes(p.read_bytes()[:20])  # now smaller than the verified range
    with pytest.raises(ImportFailure,match='source_bytes_changed'):
        stream_resumable(p,db,budget=budget,accept='segment')
    assert connect(db).execute("SELECT status FROM import_run").fetchone()[0]=='quarantined'

def test_acceptance_policy_cannot_switch_mid_run(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':crash_after_segment_zero})
    with pytest.raises(ImportFailure,match='accept_policy_conflict'):
        stream_resumable(p,db,budget=budget,accept='whole')

# ------------------------------------------------------------- dedup/conflict
def test_identity_conflict_is_recorded_and_amounts_not_double_counted(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    stream_resumable(p,db,budget=budget,accept='whole')
    other=tmp_path/'conflict.jsonl'
    other.write_text(ev('A',2,'confirm','999.00')+'\n')
    _rid,new=stream_resumable(other,db,budget=budget,accept='whole')
    assert new==0
    con=connect(db)
    assert con.execute("SELECT COUNT(*) FROM issue WHERE code='identity_conflict'").fetchone()[0]==1
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4
    # Original amount remains authoritative.
    assert con.execute("SELECT amount FROM transaction_event WHERE tx='A' AND version=2").fetchone()[0]=='100.00'

def test_duplicate_file_idempotent_across_resumable_and_legacy_stream(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    stream_resumable(p,db,budget=budget,accept='whole')
    assert stream_resumable(p,db,budget=budget,accept='whole')[1]==0
    assert stream(p,db)==0
    con=connect(db)
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4
    assert con.execute('SELECT COUNT(*) FROM source_file').fetchone()[0]==2  # batch + events

# ------------------------------------------------------------- parse evidence
def test_parse_problem_keeps_raw_bytes_and_is_listed_in_progress(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    p.write_text('not-json\n'+p.read_text(),encoding='utf-8')
    stream_resumable(p,db,budget=budget,accept='segment')
    con=connect(db)
    issue=con.execute("SELECT * FROM issue WHERE code='parse_error' AND line_no=1").fetchone()
    assert issue is not None and issue['import_id'] is not None
    assert b'not-json' in con.execute('SELECT raw FROM source_segment ORDER BY seq LIMIT 1').fetchone()[0]
    prog=import_progress(con)
    assert prog[0]['issues'][0]['line_no']==1
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==4

def test_blank_line_keeps_line_numbering_and_is_reported_once(tmp_path):
    db=seed(tmp_path,None);p=tmp_path/'blank.jsonl'
    p.write_text(ev('T',1,'authorize','5.00')+'\n\n'+ev('T',2,'confirm','5.00')+'\n')
    rid,_=stream_resumable(p,db,budget=64,accept='whole')
    con=connect(db)
    assert con.execute("SELECT line_no FROM issue WHERE code='parse_error' AND import_id=?",(rid,)).fetchone()[0]==2
    assert [r['line_no'] for r in con.execute(
        'SELECT line_no FROM transaction_event WHERE import_id=? ORDER BY line_no',(rid,))]==[1,3]

def test_crlf_hashes_match_legacy_content_hash(tmp_path):
    from dualsettle.identity import content_hash
    db=seed(tmp_path,None);p=tmp_path/'crlf.jsonl'
    l1=ev('T',1,'authorize','5.00');l2=ev('T',2,'confirm','5.00')
    p.write_bytes((l1+'\r\n'+l2+'\r\n').encode())
    rid,n=stream_resumable(p,db,budget=8,accept='whole')  # splits inside records
    assert n==2
    con=connect(db)
    raws=[r['raw'] for r in con.execute('SELECT raw FROM transaction_event WHERE import_id=? ORDER BY line_no',(rid,))]
    assert raws==[l1,l2]
    shas=[r['sha'] for r in con.execute('SELECT sha FROM transaction_event WHERE import_id=? ORDER BY line_no',(rid,))]
    assert shas==[content_hash(l1),content_hash(l2)]

def test_file_without_trailing_newline_sets_correct_boundaries(tmp_path):
    db=seed(tmp_path,None);p=tmp_path/'nonl.jsonl'
    l1=ev('T',1,'authorize','5.00');l2=ev('T',2,'confirm','5.00')
    p.write_text(l1+'\n'+l2)
    rid,n=stream_resumable(p,db,budget=len(l1)+2,accept='segment')
    assert n==2
    con=connect(db)
    assert [(s['line_start'],s['line_end']) for s in con.execute(
        'SELECT line_start,line_end FROM source_segment ORDER BY seq')]==[(1,1),(2,2)]
    assert con.execute('SELECT next_line_no FROM import_run WHERE id=?',(rid,)).fetchone()[0]==3

def test_empty_file_completes_with_sha_and_no_events(tmp_path):
    db=seed(tmp_path,None);p=tmp_path/'empty.jsonl';p.write_bytes(b'')
    rid,n=stream_resumable(p,db,budget=64,accept='segment')
    assert n==0
    row=connect(db).execute('SELECT status,whole_sha,next_line_no FROM import_run WHERE id=?',(rid,)).fetchone()
    assert row['status']=='completed' and row['whole_sha'] is not None and row['next_line_no']==1

# ------------------------------------------------------------------- reports
def test_preview_and_formal_reports_list_ranges_gaps_and_problems(tmp_path):
    db=seed(tmp_path);p=tmp_path/'events.jsonl';budget=two_segment_file(p)
    with pytest.raises(RuntimeError):
        stream_resumable(p,db,budget=budget,accept='segment',
                         hooks={'after_commit':crash_after_segment_zero})
    prun,_=reconcile(db,'D1',preview=True)
    data=json.loads(write(db,prun,tmp_path/'preview').read_text(encoding='utf-8'))
    assert data['report_kind']=='preview' and data['sealable'] is False
    imp=data['imports'][0]
    assert imp['verified_ranges']==[[0,budget]]
    assert imp['missing_ranges']==[[budget,p.stat().st_size]]
    assert imp['status']=='in_progress' and imp['accept_policy']=='segment'
    md=(tmp_path/'preview.md').read_text(encoding='utf-8')
    assert '已验证字节范围' in md and '缺段字节范围' in md
    stream_resumable(p,db,budget=budget,accept='segment')
    frun,_=reconcile(db,'D1')
    formal=json.loads(write(db,frun,tmp_path/'formal').read_text(encoding='utf-8'))
    assert formal['report_kind']=='formal' and formal['sealable'] is True
    assert formal['imports'][0]['missing_ranges']==[]
    assert formal['imports'][0]['whole_sha']

# ------------------------------------------------- legacy small-file semantics
def test_legacy_stream_and_repeat_semantics_unchanged(tmp_path):
    db=seed(tmp_path,batch_amount='99.00');p=tmp_path/'legacy.jsonl'
    p.write_text('\n'.join([ev('A',1,'authorize','100.00'),ev('A',2,'confirm','100.00')]),encoding='utf-8')
    assert stream(p,db)==2
    assert stream(p,db)==0
    con=connect(db)
    assert con.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==2
    assert con.execute('SELECT COUNT(*) FROM import_run').fetchone()[0]==0
    run,verdicts=reconcile(db,'D1');close(db,run)
    assert verdicts=={'M1':'可封账'}
