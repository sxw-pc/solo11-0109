from pathlib import Path

import pytest

from dualsettle.__main__ import demo
from dualsettle import routing


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'routing-offers.json'


def test_routing_offer_census(tmp_path):
    demo(tmp_path)
    db = tmp_path / 'data.db'
    digest = routing.register(db, 'route-1', FIXTURE)
    assert routing.summary(db, 'route-1') == {
        'cycle_id': 'D2', 'orders': 2, 'amount_cents': 20000,
        'gateways': 2, 'source_hash': digest}
    assert routing.register(db, 'route-1', FIXTURE) == digest


def test_routing_identity_conflict(tmp_path):
    demo(tmp_path)
    db = tmp_path / 'data.db'
    routing.register(db, 'route-1', FIXTURE)
    altered = tmp_path / 'altered.json'
    altered.write_bytes(FIXTURE.read_bytes() + b' ')
    with pytest.raises(ValueError, match='routing_batch_identity_conflict'):
        routing.register(db, 'route-1', altered)
