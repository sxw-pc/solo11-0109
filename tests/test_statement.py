import json
from pathlib import Path
import pytest
from dualsettle.__main__ import demo
from dualsettle.store import connect
from dualsettle import statement


def test_issue_merchant_statement(tmp_path):
    demo(tmp_path)
    db=tmp_path/'data.db';con=connect(db)
    run=con.execute('SELECT max(id) FROM reconcile_run').fetchone()[0]
    con.close()
    case=json.loads((Path(__file__).resolve().parents[1]/'fixtures'/'statement-case.json').read_text())
    first=statement.issue(db,case['id'],run,case['merchant'],case['recipient'])
    assert first['body_hash'] and json.loads(first['body'])['merchant']=='M1'
    assert statement.issue(db,case['id'],run,case['merchant'],case['recipient'])==first
    assert statement.show(db,case['id'])==first
    with pytest.raises(ValueError,match='statement_identity_conflict'):
        statement.issue(db,case['id'],run,case['merchant'],'recipient-B')
