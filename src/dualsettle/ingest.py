import csv,hashlib,io,json
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
