"""Trading data sync pipeline."""

from .auth import EveAuth
from .esi import ESIClient
from .store import SnapshotStore


def run_trading_sync(character_id: int, auth: EveAuth, store: SnapshotStore, esi: ESIClient):
    """Fetch and store wallet transactions and market orders for a character."""

    # ── Transactions ──────────────────────────────────────────────────────────
    since_id = store.get_latest_transaction_id(character_id)
    print(f"[trading] Fetching transactions for {character_id} (since_id={since_id})")

    raw_txs = esi.get_wallet_transactions_all(character_id, since_id=since_id)
    print(f"[trading] Got {len(raw_txs)} new transactions")

    if raw_txs:
        # Resolve unknown type_ids via universe/names/
        unknown_type_ids = list({t["type_id"] for t in raw_txs if not t.get("type_name")})
        type_names: dict[int, str] = {}
        if unknown_type_ids:
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

    # ── Orders ────────────────────────────────────────────────────────────────
    print(f"[trading] Fetching orders for {character_id}")

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

    print(f"[trading] Got {len(active_orders)} active, {len(history_orders)} historical orders")

    # Resolve type names for orders
    all_order_type_ids = list({o["type_id"] for o in active_orders + history_orders})
    order_type_names: dict[int, str] = {}
    if all_order_type_ids:
        try:
            for i in range(0, len(all_order_type_ids), 1000):
                batch = all_order_type_ids[i:i + 1000]
                resolved = esi.get_universe_names(batch)
                for item in resolved:
                    if item.get("category") == "inventory_type":
                        order_type_names[item["id"]] = item["name"]
        except Exception as exc:
            print(f"[trading] Order name resolution failed: {exc}")

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

    print(f"[trading] Sync complete for {character_id}")
