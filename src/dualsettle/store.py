import sqlite3
from pathlib import Path
SCHEMA="""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS account(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS source_file(id INTEGER PRIMARY KEY,path TEXT,sha TEXT UNIQUE,raw BLOB);
CREATE TABLE IF NOT EXISTS transaction_event(id INTEGER PRIMARY KEY,source_id INTEGER REFERENCES source_file(id),line_no INTEGER,merchant TEXT,tx TEXT,version INTEGER,kind TEXT,amount TEXT,business_at TEXT,arrival_at TEXT,sha TEXT,raw TEXT,UNIQUE(merchant,tx,version));
CREATE TABLE IF NOT EXISTS rate_version(id INTEGER PRIMARY KEY,merchant TEXT,start_at TEXT,end_at TEXT,rate TEXT);
CREATE TABLE IF NOT EXISTS window(id TEXT PRIMARY KEY,start_at TEXT,end_at TEXT,cutoff_at TEXT,version INTEGER);
CREATE TABLE IF NOT EXISTS stream_aggregate(window_id TEXT,merchant TEXT,gross TEXT,fee TEXT,refund TEXT,net TEXT,event_ids TEXT,PRIMARY KEY(window_id,merchant));
CREATE TABLE IF NOT EXISTS batch_line(id TEXT PRIMARY KEY,window_id TEXT,merchant TEXT,amount TEXT,corrects TEXT,source_id INTEGER REFERENCES source_file(id));
CREATE TABLE IF NOT EXISTS adjustment(id INTEGER PRIMARY KEY,window_id TEXT,merchant TEXT,amount TEXT,reason TEXT);
CREATE TABLE IF NOT EXISTS reconcile_run(id INTEGER PRIMARY KEY,window_id TEXT,status TEXT,source_digest TEXT,started_at TEXT DEFAULT CURRENT_TIMESTAMP,ended_at TEXT);
CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES reconcile_run(id),merchant TEXT,event_id INTEGER REFERENCES transaction_event(id),batch_id TEXT,rate_id INTEGER REFERENCES rate_version(id),detail TEXT);
CREATE TABLE IF NOT EXISTS verdict(run_id INTEGER REFERENCES reconcile_run(id),merchant TEXT,status TEXT,stream_net TEXT,batch_net TEXT,reason TEXT,PRIMARY KEY(run_id,merchant));
CREATE TABLE IF NOT EXISTS issue(id INTEGER PRIMARY KEY,code TEXT,merchant TEXT,tx TEXT,detail TEXT);
CREATE TABLE IF NOT EXISTS reconcile_issue(id INTEGER PRIMARY KEY,run_id INTEGER NOT NULL REFERENCES reconcile_run(id),code TEXT NOT NULL,merchant TEXT,tx TEXT,detail TEXT);
CREATE TABLE IF NOT EXISTS reserve_case(id TEXT PRIMARY KEY,merchant TEXT NOT NULL,window_id TEXT NOT NULL REFERENCES window(id),amount TEXT NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reserve_release(id INTEGER PRIMARY KEY,case_id TEXT NOT NULL REFERENCES reserve_case(id),sequence INTEGER NOT NULL,amount TEXT NOT NULL,UNIQUE(case_id,sequence));
CREATE TABLE IF NOT EXISTS allocation_agreement(id TEXT PRIMARY KEY,merchant TEXT NOT NULL,beneficiary TEXT NOT NULL,share TEXT NOT NULL,start_at TEXT NOT NULL,end_at TEXT NOT NULL,version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS payout_plan(id INTEGER PRIMARY KEY,run_id INTEGER NOT NULL REFERENCES reconcile_run(id),merchant TEXT NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,input_digest TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS payout_line(plan_id INTEGER REFERENCES payout_plan(id),beneficiary TEXT,amount TEXT,PRIMARY KEY(plan_id,beneficiary));
CREATE TABLE IF NOT EXISTS import_run(
  id INTEGER PRIMARY KEY,
  source_id INTEGER NOT NULL REFERENCES source_file(id),
  mode TEXT NOT NULL,
  accept_policy TEXT NOT NULL,
  status TEXT NOT NULL,
  path TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  next_line_no INTEGER NOT NULL,
  whole_sha TEXT,
  duplicate_of INTEGER REFERENCES import_run(id),
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT,
  error_code TEXT,
  error_detail TEXT
);
CREATE TABLE IF NOT EXISTS source_segment(
  id INTEGER PRIMARY KEY,
  import_id INTEGER NOT NULL REFERENCES import_run(id),
  seq INTEGER NOT NULL,
  byte_start INTEGER NOT NULL,
  byte_end INTEGER NOT NULL,
  line_start INTEGER NOT NULL,
  line_end INTEGER NOT NULL,
  byte_sha TEXT NOT NULL,
  event_ids TEXT NOT NULL,
  issue_ids TEXT NOT NULL,
  raw BLOB NOT NULL,
  UNIQUE(import_id,seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_import_run_active
  ON import_run(path,mode) WHERE status IN ('in_progress','quarantined');
"""
# Additive migrations for databases created before resumable ingest.
_MIGRATIONS=[
 ("reconcile_run","mode","ALTER TABLE reconcile_run ADD COLUMN mode TEXT NOT NULL DEFAULT 'formal'"),
 ("reconcile_run","preview","ALTER TABLE reconcile_run ADD COLUMN preview INTEGER NOT NULL DEFAULT 0"),
 ("reconcile_run","sealable","ALTER TABLE reconcile_run ADD COLUMN sealable INTEGER NOT NULL DEFAULT 1"),
 ("transaction_event","import_id","ALTER TABLE transaction_event ADD COLUMN import_id INTEGER REFERENCES import_run(id)"),
 ("transaction_event","segment_id","ALTER TABLE transaction_event ADD COLUMN segment_id INTEGER REFERENCES source_segment(id)"),
 ("issue","import_id","ALTER TABLE issue ADD COLUMN import_id INTEGER REFERENCES import_run(id)"),
 ("issue","segment_id","ALTER TABLE issue ADD COLUMN segment_id INTEGER REFERENCES source_segment(id)"),
 ("issue","line_no","ALTER TABLE issue ADD COLUMN line_no INTEGER"),
]
def _migrate(db):
    for table,column,stmt in _MIGRATIONS:
        cols={r['name'] for r in db.execute(f'PRAGMA table_info({table})')}
        if column not in cols:db.execute(stmt)
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p);db.row_factory=sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    db.executescript(SCHEMA);_migrate(db);db.commit()
    return db
