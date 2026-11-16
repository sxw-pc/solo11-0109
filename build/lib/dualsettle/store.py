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
"""
def connect(path):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(p);db.row_factory=sqlite3.Row;db.executescript(SCHEMA);return db
