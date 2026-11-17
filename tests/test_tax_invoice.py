from pathlib import Path

import pytest

from dualsettle.ingest import stream, batch
from dualsettle.rules import reconcile, close
from dualsettle.store import connect
from dualsettle.tax_invoice import register

FIX = Path(__file__).resolve().parents[1]/'fixtures'


def settled(tmp_path):
    db = tmp_path/'ledger.db'
    with connect(db) as con:
        con.execute('INSERT INTO window VALUES (?,?,?,?,?)',
                    ('D1','2026-01-01T00:00:00Z','2026-01-02T00:00:00Z',
                     '2026-01-02T01:00:00Z',1))
        con.execute('INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)',
                    ('M1','2025-01-01T00:00:00Z','2027-01-01T00:00:00Z','0.04'))
    assert stream(FIX/'fee-events.jsonl', db) == 4
    assert batch(FIX/'fee-batch.csv', db, 'D1') == 1
    run, verdict = reconcile(db, 'D1')
    assert verdict == {'M1':'可封账'}
    close(db, run)
    return db, run


def test_freeze_fee_tax_contract(tmp_path):
    db, run = settled(tmp_path)
    result = register(db, 'tax-M1', 'M1', FIX/'fee-tax-contract.json')
    assert result['merchant'] == 'M1'
    assert register(db, 'tax-M1', 'M1', FIX/'fee-tax-contract.json') == result
    with connect(db) as con:
        assert con.execute('SELECT status FROM reconcile_run WHERE id=?',(run,)).fetchone()[0] == '已归档'
        assert con.execute('SELECT count(*) FROM tax_contract').fetchone()[0] == 1


def test_tax_contract_identity_conflict(tmp_path):
    db, _ = settled(tmp_path)
    register(db, 'tax-M1', 'M1', FIX/'fee-tax-contract.json')
    changed = tmp_path/'changed.json'
    changed.write_text((FIX/'fee-tax-contract.json').read_text().replace('0.10','0.11'))
    with pytest.raises(ValueError, match='tax_contract_conflict'):
        register(db, 'tax-M1', 'M1', changed)
