"""Industry jobs, blueprints, and contracts sync pipeline."""

import time

from .auth import EveAuth
from .esi import ESIClient
from .store import SnapshotStore


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def run_industry_sync(character_id: int, auth: EveAuth, store: SnapshotStore, esi: ESIClient):
    """Fetch and store industry jobs, blueprints, and contracts for a character."""
    t0_total = time.perf_counter()
    now = _utcnow()

    # ── Industry jobs ────────────────────────────────────────────────────────
    t0 = time.perf_counter()
    try:
        raw_jobs = esi.get_industry_jobs(character_id, include_completed=True)
    except Exception as exc:
        print(f"[industry] Jobs fetch failed: {exc}")
        raw_jobs = []
    print(f"[industry] Got {len(raw_jobs)} jobs in {time.perf_counter()-t0:.2f}s")

    if raw_jobs:
        jobs = [{**j, "character_id": character_id, "synced_at": now} for j in raw_jobs]
        store.save_industry_jobs(jobs)

    # ── Blueprints ───────────────────────────────────────────────────────────
    t0 = time.perf_counter()
    try:
        raw_bps = esi.get_blueprints_all(character_id)
    except Exception as exc:
        print(f"[industry] Blueprints fetch failed: {exc}")
        raw_bps = []
    print(f"[industry] Got {len(raw_bps)} blueprints in {time.perf_counter()-t0:.2f}s")

    if raw_bps:
        # Resolve type names — check known names first, then batch universe/names
        bp_type_ids = list({b["type_id"] for b in raw_bps})
        known_names: dict[int, str] = {}
        placeholders = ",".join("?" * len(bp_type_ids))
        for row in store.conn.execute(
            f"SELECT type_id, type_name FROM blueprints WHERE type_id IN ({placeholders}) AND type_name IS NOT NULL",
            bp_type_ids,
        ).fetchall():
            known_names[row[0]] = row[1]
        for row in store.conn.execute(
            f"SELECT type_id, type_name FROM wallet_transactions WHERE type_id IN ({placeholders}) AND type_name IS NOT NULL",
            bp_type_ids,
        ).fetchall():
            known_names.setdefault(row[0], row[1])

        unknown_type_ids = [tid for tid in bp_type_ids if tid not in known_names]
        type_names = dict(known_names)
        if unknown_type_ids:
            try:
                for i in range(0, len(unknown_type_ids), 1000):
                    batch = unknown_type_ids[i:i + 1000]
                    resolved = esi.get_universe_names(batch)
                    for item in resolved:
                        if item.get("category") == "inventory_type":
                            type_names[item["id"]] = item["name"]
            except Exception as exc:
                print(f"[industry] Blueprint name resolution failed: {exc}")

        for b in raw_bps:
            b["type_name"] = type_names.get(b["type_id"])
        store.save_blueprints(character_id, raw_bps)

    # ── Contracts ────────────────────────────────────────────────────────────
    t0 = time.perf_counter()
    try:
        raw_contracts = esi.get_contracts(character_id)
    except Exception as exc:
        print(f"[industry] Contracts fetch failed: {exc}")
        raw_contracts = []
    print(f"[industry] Got {len(raw_contracts)} contracts in {time.perf_counter()-t0:.2f}s")

    if raw_contracts:
        contracts = [{**c, "character_id": character_id, "synced_at": now} for c in raw_contracts]
        store.save_contracts(contracts)

    print(f"[industry] [t] total industry sync: {time.perf_counter()-t0_total:.2f}s")
