"""10-minute asset delta tracking — items gained/lost during active sessions."""

import time
from datetime import datetime, timezone

from .auth import EveAuth
from .esi import ESIClient
from .store import SnapshotStore

# On-ship holds only — "Hangar" intentionally excluded (station-side, would contaminate session P&L
# with pre-session stockpiles)
INVENTORY_FLAGS = {
    "Cargo", "DroneBay", "FleetHangar", "Unlocked",
    "SpecializedOreHold", "SpecializedDenseVelocityHold", "SpecializedGasHold",
    "SpecializedMineralHold", "SpecializedSalvageHold", "SpecializedShipHold",
    "SpecializedSmallShipHold", "SpecializedMediumShipHold",
    "SpecializedLargeShipHold", "SpecializedIndustrialShipHold",
    "SpecializedAmmoHold", "SpecializedCommandCenterHold",
    "SpecializedPlanetaryCommoditiesHold",
}


def _aggregate_inventory(assets: list) -> dict[int, int]:
    """Aggregate non-singleton inventory items by type_id."""
    totals: dict[int, int] = {}
    for a in assets:
        if a.get("is_singleton"):
            continue  # skip assembled ships/containers
        if a.get("location_flag", "") not in INVENTORY_FLAGS:
            continue
        type_id = a["type_id"]
        totals[type_id] = totals.get(type_id, 0) + a.get("quantity", 1)
    return totals


def run_asset_tracking(character_id: int, auth: EveAuth, store: SnapshotStore):
    """Fetch assets, diff against last snapshot, value at Jita prices, update session."""
    open_session = store.get_open_session(character_id)
    if not open_session:
        return  # Only track during active sessions

    now = datetime.now(timezone.utc).isoformat()
    token = auth.get_valid_token(character_id)
    esi = ESIClient(access_token=token)

    try:
        try:
            all_assets = esi.get_assets_all(character_id)
        except Exception as e:
            print(f"[asset_tracker] Failed to fetch assets for {character_id}: {e}")
            return

        current = _aggregate_inventory(all_assets)
        prev = store.get_last_asset_snapshot(character_id)

        # Resolve type names for new type_ids
        prev_types = set(prev.keys()) if prev else set()
        new_types = set(current.keys()) - prev_types
        names: dict[int, str] = {}
        if new_types:
            try:
                resolved = esi.get_universe_names(list(new_types))
                names = {e["id"]: e["name"] for e in resolved
                         if e.get("category") == "inventory_type"}
            except Exception:
                pass

        store.save_asset_snapshot(character_id, now, current, names)

        if prev is None:
            return  # No previous snapshot to diff against

        # Compute delta
        all_types = set(prev) | set(current)
        gained: dict[int, int] = {}
        lost:   dict[int, int] = {}
        for type_id in all_types:
            delta = current.get(type_id, 0) - prev.get(type_id, 0)
            if delta > 0:
                gained[type_id] = delta
            elif delta < 0:
                lost[type_id] = abs(delta)

        if not gained and not lost:
            return

        # Fetch Jita 4-4 sell prices for changed type_ids
        changed_types = list(set(gained) | set(lost))
        jita_prices = esi.get_jita_sell_prices(changed_types)

        # Fallback to adjusted market prices for types missing from Jita
        # Use the module-level cache from hourly_pipeline if fresh, else fetch
        fallback: dict[int, float] = {}
        try:
            from .hourly_pipeline import _PRICES_CACHE, _PRICES_EXPIRES
            if time.time() < _PRICES_EXPIRES and _PRICES_CACHE:
                raw_prices = _PRICES_CACHE
                print("[asset_tracker] market prices: using hourly_pipeline cache")
            else:
                raw_prices = esi.get_market_prices()
                print("[asset_tracker] market prices: fetched fresh")
            fallback = {p["type_id"]: p.get("adjusted_price", 0.0) for p in raw_prices}
        except Exception as e:
            print(f"[asset_tracker] WARNING: market prices fallback failed: {e}")

        def _price(type_id: int) -> float:
            return jita_prices.get(type_id) or fallback.get(type_id, 0.0)

        gained_value = round(sum(qty * _price(tid) for tid, qty in gained.items()), 2)
        lost_value   = round(sum(qty * _price(tid) for tid, qty in lost.items()),   2)

        if gained_value or lost_value:
            store.accumulate_session_asset_values(
                character_id, open_session["started_at"], gained_value, lost_value
            )
            print(f"[asset_tracker] char={character_id} "
                  f"gained={gained_value:,.0f} ISK  lost={lost_value:,.0f} ISK")
    finally:
        esi.close()
