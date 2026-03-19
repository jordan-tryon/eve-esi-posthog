"""Trading data sync pipeline."""

import time

from .auth import EveAuth
from .esi import ESIClient
from .store import SnapshotStore


def run_trading_sync(character_id: int, auth: EveAuth, store: SnapshotStore, esi: ESIClient):
    """Fetch and store wallet transactions and market orders for a character."""
    t0_total = time.perf_counter()

    # ── Transactions ──────────────────────────────────────────────────────────
    since_id = store.get_latest_transaction_id(character_id)
    print(f"[trading] Fetching transactions for {character_id} (since_id={since_id})")

    t0 = time.perf_counter()
    raw_txs = esi.get_wallet_transactions_all(character_id, since_id=since_id)
    print(f"[trading] Got {len(raw_txs)} new transactions in {time.perf_counter()-t0:.2f}s")

    if raw_txs:
        # Resolve unknown type_ids via universe/names/
        unknown_type_ids = list({t["type_id"] for t in raw_txs if not t.get("type_name")})
        type_names: dict[int, str] = {}
        if unknown_type_ids:
            t0 = time.perf_counter()
            try:
                # ESI caps at 1000 per call
                for i in range(0, len(unknown_type_ids), 1000):
                    batch = unknown_type_ids[i:i + 1000]
                    resolved = esi.get_universe_names(batch)
                    for item in resolved:
                        if item.get("category") == "inventory_type":
                            type_names[item["id"]] = item["name"]
            except Exception as exc:
                print(f"[trading] Name resolution failed: {exc}")
            print(f"  [t] tx name resolution ({len(unknown_type_ids)} ids): {time.perf_counter()-t0:.2f}s")

        t0 = time.perf_counter()
        entries = []
        for t in raw_txs:
            entries.append({
                "transaction_id": t["transaction_id"],
                "character_id":   character_id,
                "date":           t["date"],
                "type_id":        t["type_id"],
                "type_name":      type_names.get(t["type_id"], t.get("type_name")),
                "quantity":       t["quantity"],
                "unit_price":     t["unit_price"],
                "is_buy":         t.get("is_buy", False),
                "location_id":    t.get("location_id"),
                "journal_ref_id": t.get("journal_ref_id"),
            })
        store.save_transactions(entries)
        print(f"  [t] tx save ({len(entries)} rows): {time.perf_counter()-t0:.2f}s")

    # ── Orders ────────────────────────────────────────────────────────────────
    print(f"[trading] Fetching orders for {character_id}")
    t0 = time.perf_counter()

    active_orders = []
    try:
        active_orders = esi.get_character_orders(character_id)
    except Exception as exc:
        print(f"[trading] Active orders fetch failed: {exc}")

    history_orders = []
    try:
        history_orders = esi.get_character_orders_history(character_id)
    except Exception as exc:
        print(f"[trading] Order history fetch failed: {exc}")

    print(f"[trading] Got {len(active_orders)} active, {len(history_orders)} historical orders in {time.perf_counter()-t0:.2f}s")

    # Resolve type names for orders — skip IDs already named in the DB
    all_order_type_ids = list({o["type_id"] for o in active_orders + history_orders})
    known_names: dict[int, str] = {}
    if all_order_type_ids:
        placeholders = ",".join("?" * len(all_order_type_ids))
        for row in store.conn.execute(
            f"SELECT type_id, type_name FROM market_orders WHERE type_id IN ({placeholders}) AND type_name IS NOT NULL",
            all_order_type_ids,
        ).fetchall():
            known_names[row[0]] = row[1]
        # Also check wallet_transactions for cached names
        for row in store.conn.execute(
            f"SELECT type_id, type_name FROM wallet_transactions WHERE type_id IN ({placeholders}) AND type_name IS NOT NULL",
            all_order_type_ids,
        ).fetchall():
            if row[0] not in known_names:
                known_names[row[0]] = row[1]

    unknown_order_type_ids = [tid for tid in all_order_type_ids if tid not in known_names]
    order_type_names: dict[int, str] = dict(known_names)
    if unknown_order_type_ids:
        t0 = time.perf_counter()
        try:
            for i in range(0, len(unknown_order_type_ids), 1000):
                batch = unknown_order_type_ids[i:i + 1000]
                resolved = esi.get_universe_names(batch)
                for item in resolved:
                    if item.get("category") == "inventory_type":
                        order_type_names[item["id"]] = item["name"]
        except Exception as exc:
            print(f"[trading] Order name resolution failed: {exc}")
        print(f"  [t] order name resolution ({len(unknown_order_type_ids)} new ids): {time.perf_counter()-t0:.2f}s")
    else:
        print(f"  [t] order name resolution: all {len(all_order_type_ids)} names cached, skipped ESI call")

    t0 = time.perf_counter()
    orders_to_save = []
    for o in active_orders:
        orders_to_save.append({
            "order_id":     o["order_id"],
            "character_id": character_id,
            "type_id":      o["type_id"],
            "type_name":    order_type_names.get(o["type_id"]),
            "region_id":    o.get("region_id"),
            "location_id":  o.get("location_id"),
            "is_buy_order": o.get("is_buy_order", False),
            "price":        o["price"],
            "volume_remain": o["volume_remain"],
            "volume_total": o["volume_total"],
            "issued":       o.get("issued"),
            "state":        "active",
        })

    for o in history_orders:
        state = o.get("state", "expired")
        if state not in ("cancelled", "expired"):
            state = "expired"
        orders_to_save.append({
            "order_id":     o["order_id"],
            "character_id": character_id,
            "type_id":      o["type_id"],
            "type_name":    order_type_names.get(o["type_id"]),
            "region_id":    o.get("region_id"),
            "location_id":  o.get("location_id"),
            "is_buy_order": o.get("is_buy_order", False),
            "price":        o["price"],
            "volume_remain": o["volume_remain"],
            "volume_total": o["volume_total"],
            "issued":       o.get("issued"),
            "state":        state,
        })

    if orders_to_save:
        store.save_orders(orders_to_save)
    print(f"  [t] orders save ({len(orders_to_save)} rows): {time.perf_counter()-t0:.2f}s")

    print(f"[trading] [t] total trading sync: {time.perf_counter()-t0_total:.2f}s")
