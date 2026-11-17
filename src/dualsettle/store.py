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
CREATE TABLE IF NOT EXISTS reserve_decision(
  case_id TEXT NOT NULL REFERENCES reserve_case(id),
  case_version INTEGER NOT NULL CHECK(case_version>0),
  decided_at TEXT NOT NULL,
  amount TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('win','loss')),
  route TEXT CHECK(route IS NULL OR route IN ('period_net','recovery_balance')),
  evidence_ref TEXT NOT NULL,
  recorded_at TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(case_id,case_version)
);
CREATE TABLE IF NOT EXISTS recovery_debt(
  id INTEGER PRIMARY KEY,
  merchant TEXT NOT NULL,
  window_id TEXT NOT NULL REFERENCES window(id),
  case_id TEXT NOT NULL,
  case_version INTEGER NOT NULL,
  amount TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'outstanding',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(case_id,case_version),
  FOREIGN KEY(case_id,case_version) REFERENCES reserve_decision(case_id,case_version)
);
CREATE TABLE IF NOT EXISTS allocation_agreement(id TEXT PRIMARY KEY,merchant TEXT NOT NULL,beneficiary TEXT NOT NULL,share TEXT NOT NULL,start_at TEXT NOT NULL,end_at TEXT NOT NULL,version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS payout_plan(id INTEGER PRIMARY KEY,run_id INTEGER NOT NULL REFERENCES reconcile_run(id),merchant TEXT NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,input_digest TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS payout_line(plan_id INTEGER REFERENCES payout_plan(id),beneficiary TEXT,amount TEXT,PRIMARY KEY(plan_id,beneficiary));
CREATE TABLE IF NOT EXISTS payout_basis(
  plan_id INTEGER PRIMARY KEY REFERENCES payout_plan(id),
  net TEXT NOT NULL,
  held TEXT NOT NULL,
  released TEXT NOT NULL,
  won TEXT NOT NULL,
  loss TEXT NOT NULL,
  period_loss TEXT NOT NULL,
  recovery_loss TEXT NOT NULL,
  base TEXT NOT NULL,
  final_deduction TEXT NOT NULL,
  recovery_debt TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reserve_decision_case ON reserve_decision(case_id);
CREATE INDEX IF NOT EXISTS recovery_debt_merchant ON recovery_debt(merchant,window_id);
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p);db.row_factory=sqlite3.Row;db.executescript(SCHEMA);return db
