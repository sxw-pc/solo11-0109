"""Issue an offline merchant-specific statement from a closed run."""
import hashlib
import json
from .store import connect
from .rules import input_digest

SCHEMA="""
CREATE TABLE IF NOT EXISTS statement_issue(
 id TEXT PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES reconcile_run(id),
 merchant TEXT NOT NULL, recipient TEXT NOT NULL, body_hash TEXT NOT NULL,
 body TEXT NOT NULL);
"""

def issue(path, statement_id, run_id, merchant, recipient):
    if not statement_id or not merchant or not recipient:raise ValueError('invalid_statement')
    db=connect(path)
    try:
        db.executescript(SCHEMA)
        with db:
            run=db.execute('SELECT status,source_digest,window_id FROM reconcile_run WHERE id=?',(run_id,)).fetchone()
            if not run or run['status']!='已归档' or run['source_digest']!=input_digest(db):
                raise ValueError('run_not_current_closed')
            verdict=db.execute('SELECT status,stream_net,batch_net,reason FROM verdict WHERE run_id=? AND merchant=?',(run_id,merchant)).fetchone()
            if not verdict:raise ValueError('unknown_merchant_verdict')
            obj={'run_id':run_id,'window':run['window_id'],'merchant':merchant,'recipient':recipient,
                 'verdict':dict(verdict)}
            body=json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'))
            digest=hashlib.sha256(body.encode()).hexdigest()
            old=db.execute('SELECT * FROM statement_issue WHERE id=?',(statement_id,)).fetchone()
            if old:
                if (old['run_id'],old['merchant'],old['recipient'],old['body_hash'])!=(run_id,merchant,recipient,digest):
                    raise ValueError('statement_identity_conflict')
                return dict(old)
            db.execute('INSERT INTO statement_issue VALUES (?,?,?,?,?,?)',(statement_id,run_id,merchant,recipient,digest,body))
            return dict(db.execute('SELECT * FROM statement_issue WHERE id=?',(statement_id,)).fetchone())
    finally:db.close()

def show(path, statement_id):
    db=connect(path)
    try:
        db.executescript(SCHEMA)
        row=db.execute('SELECT * FROM statement_issue WHERE id=?',(statement_id,)).fetchone()
        if not row:raise ValueError('unknown_statement')
        return dict(row)
    finally:db.close()
