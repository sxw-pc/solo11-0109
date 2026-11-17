"""Freeze a prior closed basket for prospective rate quote; no quote solver."""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .contract import validate_rate
from .ingest import batch, stream
from .rules import close, input_digest, reconcile
from .store import connect


@dataclass(frozen=True)
class RateQuoteCase:
    case_id: str
    digest: str
    policy: dict
    frozen_rates: tuple[tuple[str, str], ...]
    confirmed: tuple[tuple[str, str, str], ...]
    benchmark_fees: tuple[tuple[str, str], ...]
    run_status: str
    source_digest: str


def _inside(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("rate quote source must be relative")
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file() or path.is_symlink():
        raise ValueError(f"rate quote source unavailable: {relative}")
    return path


def load_case(manifest: str | Path) -> RateQuoteCase:
    root = Path(__file__).resolve().parents[2]
    path = Path(manifest).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("rate quote manifest must be inside repository")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if set(raw) != {"policy", "events", "batch", "sha256"}:
        raise ValueError("rate quote manifest fields differ")
    names = {raw["policy"], raw["events"], raw["batch"]}
    if len(names) != 3 or set(raw["sha256"]) != names:
        raise ValueError("rate quote digest coverage differs")
    paths = {name: _inside(root, name) for name in names}
    for name, source in paths.items():
        if hashlib.sha256(source.read_bytes()).hexdigest() != raw["sha256"][name]:
            raise ValueError(f"rate quote source changed: {name}")
    policy = json.loads(paths[raw["policy"]].read_text(encoding="utf-8"))
    expected = {"case_id", "benchmark_window_id", "quote_window_id", "current_rates",
                "allowed_rate_min", "allowed_rate_max", "target_fee", "amendment_weights",
                "contracts", "rounding"}
    if set(policy) != expected or policy["benchmark_window_id"] == policy["quote_window_id"]:
        raise ValueError("rate quote policy fields differ")
    merchants = set(policy["current_rates"])
    if not merchants or merchants != set(policy["amendment_weights"]):
        raise ValueError("rate quote merchant coverage differs")
    low, high = Decimal(policy["allowed_rate_min"]), Decimal(policy["allowed_rate_max"])
    if not 0 <= low <= high < 1 or Decimal(policy["target_fee"]) <= 0:
        raise ValueError("invalid prospective quote envelope")
    for merchant, value in policy["current_rates"].items():
        rate = Decimal(value)
        if not low <= rate <= high or Decimal(policy["amendment_weights"][merchant]) <= 0:
            raise ValueError("invalid current merchant rate or weight")
    with tempfile.TemporaryDirectory(prefix="dualsettle-rate-quote-") as folder:
        db_path = Path(folder) / "data.db"
        db = connect(db_path)
        with db:
            db.execute("INSERT INTO window VALUES (?,?,?,?,?)",
                       (policy["benchmark_window_id"], "2026-01-01T00:00:00+00:00",
                        "2026-01-02T00:00:00+00:00", "2026-01-02T01:00:00+00:00", 1))
            for merchant, value in sorted(policy["current_rates"].items()):
                rate_row = {"start_at": "2026-01-01T00:00:00+00:00",
                            "end_at": "2026-01-02T00:00:00+00:00", "rate": value}
                validate_rate(rate_row)
                db.execute("INSERT INTO rate_version(merchant,start_at,end_at,rate) VALUES (?,?,?,?)",
                           (merchant, rate_row["start_at"], rate_row["end_at"], value))
        db.close()
        event_path = Path(folder) / "events.jsonl"
        event_path.write_bytes(paths[raw["events"]].read_bytes())
        batch_path = Path(folder) / "batch.csv"
        batch_path.write_bytes(paths[raw["batch"]].read_bytes())
        stream(event_path, db_path)
        batch(batch_path, db_path, policy["benchmark_window_id"])
        run_id, verdict = reconcile(db_path, policy["benchmark_window_id"])
        if verdict != {merchant: "可封账" for merchant in merchants}:
            raise ValueError("benchmark basket is not closable")
        close(db_path, run_id)
        db = connect(db_path)
        try:
            run = db.execute("SELECT status,source_digest FROM reconcile_run WHERE id=?", (run_id,)).fetchone()
            confirmed = tuple((r["merchant"], r["tx"], r["amount"]) for r in
                              db.execute("SELECT merchant,tx,amount FROM transaction_event WHERE kind='confirm' ORDER BY merchant,tx"))
            if not confirmed or {r[0] for r in confirmed} != merchants:
                raise ValueError("confirmed merchant basket differs")
            frozen_rates = tuple((r["merchant"], r["rate"]) for r in
                                 db.execute("SELECT merchant,rate FROM rate_version ORDER BY merchant"))
            fees = tuple((r["merchant"], r["fee"]) for r in
                         db.execute("SELECT merchant,fee FROM stream_aggregate WHERE window_id=? ORDER BY merchant",
                                    (policy["benchmark_window_id"],)))
            if run["status"] != "已归档" or input_digest(db) != run["source_digest"]:
                raise ValueError("benchmark archive changed")
            source_digest = run["source_digest"]
        finally:
            db.close()
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True,
                                       separators=(",", ":")).encode("utf-8")).hexdigest()
    return RateQuoteCase(policy["case_id"], digest, policy, frozen_rates, confirmed,
                         fees, run["status"], source_digest)


def main() -> int:
    default = Path(__file__).resolve().parents[2] / "fixtures" / "rate_quote" / "case.json"
    case = load_case(sys.argv[1] if len(sys.argv) > 1 else default)
    print(json.dumps({"case_id": case.case_id, "digest": case.digest,
                      "benchmark_status": case.run_status,
                      "confirmed": list(case.confirmed), "current_rates": dict(case.frozen_rates),
                      "benchmark_fees": dict(case.benchmark_fees),
                      "target_fee": case.policy["target_fee"],
                      "quote_window": case.policy["quote_window_id"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
