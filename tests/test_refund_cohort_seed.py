from pathlib import Path

from dualsettle.ingest import stream
from dualsettle.refund_cohort import register
from dualsettle.store import connect
import pytest


FIXTURES=Path(__file__).resolve().parents[1]/'fixtures'


def test_registers_existing_transaction_evidence(tmp_path):
    db=tmp_path/'cohort.db'
    assert stream(FIXTURES/'refund-events.jsonl',db)==12
    got=register(db,'case-1',FIXTURES/'refund-cohort.json')
    assert got['transaction_count']==4
    assert register(db,'case-1',FIXTURES/'refund-cohort.json')==got
    with connect(db) as conn:
        assert conn.execute('SELECT COUNT(*) FROM refund_cohort_case').fetchone()[0]==1
        assert conn.execute('SELECT COUNT(*) FROM transaction_event').fetchone()[0]==12


def test_rejects_missing_confirmed_transaction(tmp_path):
    db=tmp_path/'cohort.db'
    stream(FIXTURES/'refund-events.jsonl',db)
    with connect(db) as conn:
        conn.execute("DELETE FROM transaction_event WHERE tx='T3'")
    with pytest.raises(ValueError,match='missing_cohort_transaction'):
        register(db,'case-1',FIXTURES/'refund-cohort.json')
