"""Planetary interaction (PI) colony sync pipeline.

Unlike every other 10-minute-cadence job in this app (asset_tracker no-ops
while offline, industry_pipeline/hourly_pipeline throttle to hourly while
offline via `_make_sync_fn`), this one runs on a plain interval with NO
online/offline throttle. Extraction and factory cycles progress in real time
whether or not the character is logged in, so an "extractor ready" alert
computed only while the character happens to be online would silently go
stale for anyone who isn't actively playing. Don't "fix" this into
conformity with the other jobs — it's intentional.
"""

import time

from .auth import EveAuth
from .esi import ESIClient
from .store import SnapshotStore


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def run_pi_sync(character_id: int, auth: EveAuth, store: SnapshotStore, esi: ESIClient):
    """Fetch and store PI colony summaries for a character."""
    t0_total = time.perf_counter()

    try:
        raw_planets = esi.get_planets(character_id)
    except Exception as exc:
        print(f"[pi] Planets fetch failed: {exc}")
        return
    print(f"[pi] Got {len(raw_planets)} colonies")

    colonies = []
    for p in raw_planets:
        planet_id = p["planet_id"]
        planet_name = None
        type_id = None
        try:
            info = esi.get_universe_planet(planet_id)
            planet_name = info.get("name")
            type_id = info.get("type_id")
        except Exception as exc:
            print(f"[pi] Planet name resolution failed for {planet_id}: {exc}")

        next_expiry = None
        try:
            detail = esi.get_planet_detail(character_id, planet_id)
            expiries = [
                pin["expiry_time"]
                for pin in detail.get("pins", [])
                if pin.get("expiry_time")
            ]
            if expiries:
                next_expiry = min(expiries)
        except Exception as exc:
            print(f"[pi] Colony detail fetch failed for {planet_id}: {exc}")

        colonies.append({
            "planet_id":       planet_id,
            "planet_name":     planet_name,
            "planet_type":     p.get("planet_type"),
            "type_id":         type_id,
            "solar_system_id": p.get("solar_system_id"),
            "owner_id":        p.get("owner_id"),
            "upgrade_level":   p.get("upgrade_level"),
            "num_pins":        p.get("num_pins"),
            "last_update":     p.get("last_update"),
            "next_expiry":     next_expiry,
        })

    store.save_pi_colonies(character_id, colonies)
    print(f"[pi] [t] total PI sync: {time.perf_counter()-t0_total:.2f}s")
