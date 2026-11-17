import sqlite3
from pathlib import Path

SCHEMA_VERSION = 2

# The original ledger schema.  Everything here is created with IF NOT EXISTS so
# an existing database can be opened in place.
CORE_SCHEMA = """
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
"""

# Version 2: offline reviewers, a frozen written review contract, and an
# append-only signature ledger.  Signatures are never updated or deleted.
REVIEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS reviewer(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  roles TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  local_key TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reconcile_submission(
  run_id INTEGER PRIMARY KEY REFERENCES reconcile_run(id),
  submitter TEXT NOT NULL REFERENCES reviewer(id),
  source_digest TEXT NOT NULL,
  submitted_at TEXT NOT NULL,
  signature_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS review_contract(
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL UNIQUE REFERENCES reconcile_run(id),
  route TEXT NOT NULL CHECK(route IN ('standard','risk')),
  submitter TEXT NOT NULL REFERENCES reviewer(id),
  terms TEXT NOT NULL,
  source_digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  contract_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS review_signature(
  id INTEGER PRIMARY KEY,
  contract_id INTEGER NOT NULL REFERENCES review_contract(id),
  run_id INTEGER NOT NULL REFERENCES reconcile_run(id),
  source_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  reviewer TEXT NOT NULL REFERENCES reviewer(id),
  role TEXT NOT NULL,
  decision TEXT NOT NULL,
  opinion TEXT NOT NULL,
  ref_id INTEGER REFERENCES review_signature(id),
  signed_at TEXT NOT NULL,
  content_digest TEXT NOT NULL UNIQUE,
  signature_digest TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS review_signature_contract_idx
  ON review_signature(contract_id,id);
CREATE INDEX IF NOT EXISTS review_signature_ref_idx
  ON review_signature(ref_id,id);
"""


def migrate(db):
    """Apply the forward-only schema migrations to an open connection.

    Version 0 databases (the original schema) are upgraded in place with
    CREATE TABLE IF NOT EXISTS, so no evidence is rewritten or dropped.
    """
    version = db.execute('PRAGMA user_version').fetchone()[0]
    db.executescript(CORE_SCHEMA)
    if version < 2:
        db.executescript(REVIEW_SCHEMA)
    db.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
    return SCHEMA_VERSION


def connect(path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(p)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    migrate(db)
    return db
