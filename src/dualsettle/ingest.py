import csv,hashlib,io,json,os
from pathlib import Path
from .store import connect
from .identity import event_identity,content_hash
def source(db,path):
    raw=Path(path).read_bytes();h=hashlib.sha256(raw).hexdigest()
    old=db.execute('SELECT id FROM source_file WHERE sha=?',(h,)).fetchone()
    if old:return old['id'],True
    return db.execute('INSERT INTO source_file(path,sha,raw) VALUES (?,?,?)',(str(path),h,raw)).lastrowid,False
def stream(path,db_path):
    db=connect(db_path);count=0
    with db:
        sid,dup=source(db,path)
        if dup:return 0
        lines=Path(path).read_text(encoding='utf-8').splitlines()
        for n,line in enumerate(lines,1):
            try:
                e=json.loads(line);merchant,tx,version=event_identity(e);kind=e['kind']
                if kind not in ('authorize','confirm','cancel','refund'):raise ValueError('kind')
                sha=content_hash(line)
                old=db.execute('SELECT sha FROM transaction_event WHERE merchant=? AND tx=? AND version=?',(merchant,tx,version)).fetchone()
                if old:
                    if old['sha']!=sha:db.execute('INSERT INTO issue(code,merchant,tx,detail) VALUES (?,?,?,?)',('identity_conflict',merchant,tx,line))
                    continue
                db.execute('INSERT OR IGNORE INTO account VALUES (?)',(merchant,))
                db.execute('INSERT INTO transaction_event(source_id,line_no,merchant,tx,version,kind,amount,business_at,arrival_at,sha,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                  (sid,n,merchant,tx,version,kind,str(e['amount']),e['business_at'],e.get('arrival_at',e['business_at']),sha,line));count+=1
            except (KeyError,ValueError,TypeError,json.JSONDecodeError) as exc:
                db.execute('INSERT INTO issue(code,detail) VALUES (?,?)',('parse_error',f'line {n}: {exc}'))
    db.close();return count
def batch(path,db_path,window):
    db=connect(db_path);count=0
    with db:
        sid,dup=source(db,path)
        if dup:return 0
        rows=list(csv.DictReader(io.StringIO(Path(path).read_text(encoding='utf-8-sig'))))
        for row in rows:
            if row.get('corrects') and not db.execute('SELECT 1 FROM batch_line WHERE id=?',(row['corrects'],)).fetchone():
                db.execute('INSERT INTO issue(code,merchant,detail) VALUES (?,?,?)',('orphan_correction',row.get('merchant'),row['id']));continue
            try:
                db.execute('INSERT INTO batch_line VALUES (?,?,?,?,?,?)',(row['id'],window,row['merchant'],row['amount'],row.get('corrects') or None,sid));count+=1
            except Exception:
                db.execute('INSERT INTO issue(code,merchant,detail) VALUES (?,?,?)',('batch_conflict',row.get('merchant'),row.get('id')))
    db.close();return count

# ---------------------------------------------------------------------------
# Resumable, fixed-memory JSONL ingest
#
# A run (import_run) owns one external source file. The file is processed in
# segments cut on complete JSONL record boundaries. Each segment persists its
# raw byte evidence, verified byte range, line range, digest and the event /
# issue identities committed inside it. Segment rows, the segment's events
# and issues, and run progress move in one transaction, so a crash leaves
# either the whole segment durable or none of it.
# ---------------------------------------------------------------------------
ACCEPT_POLICIES=('whole','segment')
ACTIVE_STATES=('in_progress','quarantined')

class ImportFailure(Exception):
    """Deterministic resumable-ingest failure; ``args[0]`` is the error code."""

def _sha256(buf):return hashlib.sha256(buf).hexdigest()

def iter_segments(path,budget):
    """Yield ``(segment_bytes, byte_start, line_start)`` on record boundaries.

    Reads are budget-sized and a segment only closes once appending the next
    complete record would cross the budget, so a JSONL record is never split
    across segments. A single oversize record may exceed the budget; all
    other buffering stays within roughly two budgets.
    """
    if budget<=0:raise ValueError('invalid_budget')
    seg_start=0;line_start=1;seg=b'';pending=b''
    with open(path,'rb') as f:
        while True:
            chunk=f.read(budget)
            if not chunk:
                if seg:
                    yield seg,seg_start,line_start
                    seg_start+=len(seg);line_start+=seg.count(b'\n');seg=b''
                if pending:
                    yield pending,seg_start,line_start
                return
            pending+=chunk
            nl=pending.rfind(b'\n')
            if nl<0:continue
            complete,pending=pending[:nl+1],pending[nl+1:]
            for line in complete.splitlines(keepends=True):
                if seg and len(seg)+len(line)>budget:
                    yield seg,seg_start,line_start
                    seg_start+=len(seg);line_start+=seg.count(b'\n');seg=b''
                seg+=line

def _whole_sha(path,budget):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        while True:
            chunk=f.read(budget)
            if not chunk:return h.hexdigest()
            h.update(chunk)

def _quarantine(db,run_id,code,detail):
    db.execute("UPDATE import_run SET status='quarantined',error_code=?,error_detail=? WHERE id=?",(code,detail,run_id))

def _verify_checkpoints(db,run_id,f,total_bytes):
    """Re-hash every committed byte range against the live external file."""
    pos=0;next_line=1
    for seg in db.execute('SELECT * FROM source_segment WHERE import_id=? ORDER BY seq',(run_id,)):
        if seg['byte_start']!=pos or seg['line_start']!=next_line:
            raise ImportFailure('checkpoint_corrupt')
        f.seek(seg['byte_start'])
        buf=f.read(seg['byte_end']-seg['byte_start'])
        if len(buf)!=seg['byte_end']-seg['byte_start'] or _sha256(buf)!=seg['byte_sha']:
            raise ImportFailure('source_bytes_changed')
        pos=seg['byte_end'];next_line=seg['line_end']+1
    if pos>total_bytes:raise ImportFailure('source_bytes_changed')
    return pos,next_line

def _process_segment(db,run,sid,seg_bytes,byte_start,line_start,seq,hooks):
    """One atomic segment commit: events + issues + evidence + progress."""
    run_id=run['id']
    lines=seg_bytes.split(b'\n')
    if lines and lines[-1]==b'':lines.pop()
    event_ids=[];issue_ids=[];line_no=line_start
    for raw in lines:
        text_raw=raw[:-1] if raw.endswith(b'\r') else raw
        try:
            line=text_raw.decode('utf-8')
            e=json.loads(line)
            merchant,tx_id,version=event_identity(e);kind=e['kind']
            if kind not in ('authorize','confirm','cancel','refund'):raise ValueError('kind')
            sha=content_hash(line)
            old=db.execute('SELECT sha FROM transaction_event WHERE merchant=? AND tx=? AND version=?',(merchant,tx_id,version)).fetchone()
            if old:
                if old['sha']!=sha:
                    cur=db.execute('INSERT INTO issue(code,merchant,tx,detail,import_id,line_no) VALUES (?,?,?,?,?,?)',
                                   ('identity_conflict',merchant,tx_id,line,run_id,line_no)).lastrowid
                    issue_ids.append(cur)
            else:
                db.execute('INSERT OR IGNORE INTO account VALUES (?)',(merchant,))
                cur=db.execute('INSERT INTO transaction_event(source_id,line_no,merchant,tx,version,kind,amount,business_at,arrival_at,sha,raw,import_id) '
                               'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                               (sid,line_no,merchant,tx_id,version,kind,str(e['amount']),e['business_at'],
                                e.get('arrival_at',e['business_at']),sha,line,run_id)).lastrowid
                event_ids.append(cur)
        except (KeyError,ValueError,TypeError,json.JSONDecodeError,UnicodeDecodeError) as exc:
            cur=db.execute('INSERT INTO issue(code,detail,import_id,line_no) VALUES (?,?,?,?)',
                           ('parse_error',f'line {line_no}: {exc}',run_id,line_no)).lastrowid
            issue_ids.append(cur)
        line_no+=1
    byte_end=byte_start+len(seg_bytes)
    # ``line_no`` was advanced once per parsed record, so the last record
    # number is line_no-1; this also covers a final segment without newline.
    line_end=line_no-1
    if hooks and 'before_commit' in hooks:hooks['before_commit'](run_id,seq)
    seg_id=db.execute('INSERT INTO source_segment(import_id,seq,byte_start,byte_end,line_start,line_end,byte_sha,event_ids,issue_ids,raw) '
                      'VALUES (?,?,?,?,?,?,?,?,?,?)',
                      (run_id,seq,byte_start,byte_end,line_start,line_end,_sha256(seg_bytes),
                       json.dumps(event_ids),json.dumps(issue_ids),seg_bytes)).lastrowid
    if event_ids:
        db.execute('UPDATE transaction_event SET segment_id=? WHERE id IN (%s)'%','.join('?'*len(event_ids)),[seg_id,*event_ids])
    if issue_ids:
        db.execute('UPDATE issue SET segment_id=? WHERE id IN (%s)'%','.join('?'*len(issue_ids)),[seg_id,*issue_ids])
    db.execute('UPDATE import_run SET next_line_no=? WHERE id=?',(line_end+1,run_id))
    return seg_id,len(event_ids),len(issue_ids),line_end

def stream_resumable(path,db_path,*,budget=1<<20,accept='whole',hooks=None):
    """Import a JSONL source in resumable segments under a fixed byte budget.

    ``accept`` is declared up-front by the import mandate and cannot change
    mid-run: ``'whole'`` only reveals events once the end-of-file SHA matches;
    ``'segment'`` also allows verified complete segments in non-sealing
    preview reconciles. Returns ``(import_run_id, committed_event_count)``.
    Re-running on a completed run is idempotent and returns ``(id, 0)``.
    """
    if accept not in ACCEPT_POLICIES:raise ValueError('invalid_accept_policy')
    p=Path(path)
    db=connect(db_path)
    try:
        try:total_bytes=os.path.getsize(p)
        except OSError:raise ImportFailure('source_missing')
        run=db.execute("SELECT * FROM import_run WHERE path=? AND mode='stream' AND status IN ('in_progress','quarantined') ORDER BY id",
                       (str(p),)).fetchone()
        if run is None:
            run=db.execute("SELECT * FROM import_run WHERE path=? AND mode='stream' AND status='completed' ORDER BY id DESC",
                           (str(p),)).fetchone()
            if run is not None:
                # Idempotent re-drive of a finished run: nothing new to do.
                return run['id'],0
        if run is not None:
            if run['status']=='quarantined':raise ImportFailure('import_quarantined')
            if run['accept_policy']!=accept:raise ImportFailure('accept_policy_conflict')
            if run['total_bytes']!=total_bytes:
                with db:_quarantine(db,run['id'],'source_bytes_changed',
                                    f'size {run["total_bytes"]}->{total_bytes}')
                raise ImportFailure('source_bytes_changed')
        else:
            with db:
                sid=db.execute('INSERT INTO source_file(path,sha,raw) VALUES (?,?,?)',(str(p),None,None)).lastrowid
                run_id=db.execute("INSERT INTO import_run(source_id,mode,accept_policy,status,path,total_bytes,next_line_no) "
                                  "VALUES (?,'stream',?,'in_progress',?,?,1)",
                                  (sid,accept,str(p),total_bytes)).lastrowid
            run=db.execute('SELECT * FROM import_run WHERE id=?',(run_id,)).fetchone()
        run_id=run['id'];sid=run['source_id']
        try:f=open(path,'rb')
        except OSError:
            with db:_quarantine(db,run_id,'source_missing',str(p))
            raise ImportFailure('source_missing')
        with f:
            try:resume_pos,_resume_line=_verify_checkpoints(db,run_id,f,total_bytes)
            except ImportFailure as exc:
                with db:_quarantine(db,run_id,exc.args[0],'checkpoint verification failed')
                raise
            new_events=0
            seq=db.execute('SELECT COALESCE(MAX(seq),-1)+1 FROM source_segment WHERE import_id=?',(run_id,)).fetchone()[0]
            for seg_bytes,byte_start,line_start in iter_segments(p,budget):
                if byte_start+len(seg_bytes)<=resume_pos:continue
                with db:
                    _seg_id,n_events,_n_issues,_line_end=_process_segment(db,run,sid,seg_bytes,byte_start,line_start,seq,hooks)
                if hooks and 'after_commit' in hooks:hooks['after_commit'](run_id,seq)
                seq+=1;new_events+=n_events
        whole=_whole_sha(p,budget)
        with db:
            other_sf=db.execute('SELECT id FROM source_file WHERE sha=?',(whole,)).fetchone()
            if other_sf is not None:
                # Idempotent duplicate: identical bytes already fully imported.
                other_run=db.execute("SELECT id FROM import_run WHERE source_id=? AND status='completed' ORDER BY id",
                                     (other_sf['id'],)).fetchone()
                db.execute('UPDATE import_run SET status=\'completed\',whole_sha=?,duplicate_of=?,finished_at=CURRENT_TIMESTAMP WHERE id=?',
                           (whole,other_run['id'] if other_run else None,run_id))
                db.execute('UPDATE transaction_event SET source_id=? WHERE import_id=? AND source_id=?',(other_sf['id'],run_id,sid))
                db.execute('UPDATE import_run SET source_id=? WHERE id=?',(other_sf['id'],run_id))
                db.execute('DELETE FROM source_file WHERE id=?',(sid,))
                return run_id,0
            db.execute('UPDATE source_file SET sha=? WHERE id=?',(whole,sid))
            db.execute("UPDATE import_run SET status='completed',whole_sha=?,finished_at=CURRENT_TIMESTAMP WHERE id=?",(whole,run_id))
        return run_id,new_events
    finally:db.close()

def visible_events(db,preview=False):
    """Events a reconcile may see, chosen by import progress and acceptance.

    Legacy events (no import_run) are always visible. Resumable events are
    visible when their run completed, or — only in preview reconciles — when
    the run declared segment acceptance and every segment carrying the event
    is durably verified. Quarantined and whole-policy partial runs stay
    invisible until completion.
    """
    sql=('SELECT te.* FROM transaction_event te WHERE te.import_id IS NULL OR EXISTS('
         "SELECT 1 FROM import_run ir WHERE ir.id=te.import_id AND ir.status='completed')")
    if preview:
        sql+=(" OR EXISTS(SELECT 1 FROM import_run ir WHERE ir.id=te.import_id "
              "AND ir.status='in_progress' AND ir.accept_policy='segment' "
              "AND te.segment_id IS NOT NULL)")
    return [dict(r) for r in db.execute(sql)]

def incomplete_imports(db):
    return [dict(r) for r in db.execute(
        "SELECT * FROM import_run WHERE status IN ('in_progress','quarantined') ORDER BY id")]

def import_progress(db):
    """Structured per-source progress used by reports and previews."""
    out=[]
    for run in db.execute('SELECT * FROM import_run ORDER BY id'):
        segs=[dict(s) for s in db.execute('SELECT * FROM source_segment WHERE import_id=? ORDER BY seq',(run['id'],))]
        verified=[[s['byte_start'],s['byte_end']] for s in segs]
        missing=[]
        if run['status'] in ('in_progress','quarantined'):
            start=segs[-1]['byte_end'] if segs else 0
            if start<run['total_bytes']:missing.append([start,run['total_bytes']])
        problems=[dict(r) for r in db.execute(
            'SELECT id,code,merchant,tx,line_no,detail,segment_id FROM issue WHERE import_id=? ORDER BY line_no,id',(run['id'],))]
        out.append({'run':dict(run),'segments':[{'seq':s['seq'],'byte_start':s['byte_start'],'byte_end':s['byte_end'],
                                                 'line_start':s['line_start'],'line_end':s['line_end'],
                                                 'byte_sha':s['byte_sha'],'event_ids':json.loads(s['event_ids']),
                                                 'issue_ids':json.loads(s['issue_ids'])} for s in segs],
                    'verified_ranges':verified,'missing_ranges':missing,'issues':problems})
    return out
