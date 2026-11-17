import csv
import json
from pathlib import Path

import pytest

from dualsettle.ingest import batch, stream
from dualsettle.rules import close, reconcile
from dualsettle.store import connect
from dualsettle.volume_shift import register

CASE = Path(__file__).resolve().parents[1]/'fixtures'/'volume-case.json'


def settled_case(tmp_path):
    doc = json.loads(CASE.read_text(encoding='utf-8'))
    db = tmp_path/'ledger.db'
    with connect(db) as con:
        for wid, day in [('B','2026-01-01'),('T','2026-01-02')]:
            con.execute('INSERT INTO window VALUES (?,?,?,?,?)',
                        (wid, f'{day}T00:00:00Z',
                         f'2026-01-0{int(day[-2:])+1}T00:00:00Z',
                         '2026-01-04T00:00:00Z', 1))
        for merchant in ('M1','M2'):
            con.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',
                        (merchant,'2025-01-01T00:00:00Z','2027-01-01T00:00:00Z','0'))
    rows = []
    for wid, day in [('B','2026-01-01'),('T','2026-01-02')]:
        for merchant, count in doc['synthetic_counts'][wid].items():
            for n in range(count):
                tx = f'{wid}-{merchant}-{n}'
                for version, kind, hour in [(1,'authorize','08'),(2,'confirm','09')]:
                    rows.append(json.dumps({'merchant':merchant,'tx':tx,'version':version,
                                            'kind':kind,'amount':'1.00',
                                            'business_at':f'{day}T{hour}:00:00Z'}))
    event_file = tmp_path/'events.jsonl'
    event_file.write_text('\n'.join(rows), encoding='utf-8')
    assert stream(event_file, db) == 800
    for wid in ('B','T'):
        source = tmp_path/f'{wid}.csv'
        with source.open('w', newline='', encoding='utf-8') as output:
            writer = csv.writer(output)
            writer.writerow(['id','merchant','amount','corrects'])
            for merchant, count in doc['synthetic_counts'][wid].items():
                writer.writerow([f'{wid}-{merchant}',merchant,f'{count}.00',''])
        assert batch(source, db, wid) == 2
    for wid in ('B','T'):
        run, verdict = reconcile(db, wid)
        assert verdict == {'M1':'可封账','M2':'可封账'}
        close(db, run)
    return db


def test_register_closed_activity_case(tmp_path):
    db = settled_case(tmp_path)
    record = register(db, 'volume-A', CASE)
    assert record['baseline_run'] == 1 and record['target_run'] == 2
    assert register(db, 'volume-A', CASE) == record


def test_changed_case_conflicts(tmp_path):
    db = settled_case(tmp_path)
    register(db, 'volume-A', CASE)
    changed = tmp_path/'other.json'
    changed.write_text(CASE.read_text().replace('"target_days":1','"target_days":2'))
    with pytest.raises(ValueError, match='volume_case_conflict'):
        register(db, 'volume-A', changed)
