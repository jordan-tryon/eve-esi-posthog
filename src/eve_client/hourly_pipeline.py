"""Hourly snapshot orchestration."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from .analytics import Analytics
from .auth import EveAuth
from .esi import ESIClient
from .metrics import compute_isk_rates, compute_risk_level, detect_activity_type, detect_session_type, ESCROW_RETURN_TYPES
from .store import SnapshotStore

_PRICES_CACHE: list = []
_PRICES_EXPIRES: float = 0.0


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(fn, default=None):
    try:
        return fn()
    except Exception as e:
        print(f"  [warn] {e}")
        return default


def run_hourly_snapshot(
    character_id: int,
    auth: EveAuth,
    analytics: Analytics,
    store: SnapshotStore,
):
    t0_total = time.perf_counter()
    now = _utcnow()
    print(f"[sync] {now} — character {character_id}")

    token = auth.get_valid_token(character_id)
    esi = ESIClient(access_token=token)

    # --- Fetch raw ESI data (all independent — run in parallel) ---
    t0 = time.perf_counter()
    global _PRICES_CACHE, _PRICES_EXPIRES
    if time.time() < _PRICES_EXPIRES:
        _cached_prices = _PRICES_CACHE
        print("  [t] market prices: cached")
    else:
        _cached_prices = None

    _fetches = {
        "public_info":    (esi.get_public_info,    character_id),
        "wallet_balance": (esi.get_wallet,         character_id),
        "journal_raw":    (esi.get_wallet_journal, character_id),
        "location":       (esi.get_location,       character_id),
        "ship":           (esi.get_ship,           character_id),
        "skills_data":    (esi.get_skills,         character_id),
        "online_info":    (esi.get_online,         character_id),
        "all_assets":     (esi.get_assets_all,     character_id),
    }
    if _cached_prices is None:
        _fetches["market_prices"] = (esi.get_market_prices,)

    _defaults = {
        "public_info": {}, "wallet_balance": 0.0, "journal_raw": [],
        "location": {}, "ship": {}, "skills_data": {}, "online_info": {},
        "all_assets": [], "market_prices": [],
    }
    _results: dict = {}

    def _run_fetch(key):
        fn, *args = _fetches[key]
        return key, _safe(lambda: fn(*args), _defaults[key])

    with ThreadPoolExecutor(max_workers=9) as _pool:
        for _key, _val in _pool.map(_run_fetch, _fetches):
            _results[_key] = _val
    print(f"  [t] parallel ESI fetch: {time.perf_counter()-t0:.2f}s")

    public_info    = _results["public_info"]
    wallet_balance = _results["wallet_balance"]
    journal_raw    = _results["journal_raw"]
    location       = _results["location"]
    ship           = _results["ship"]
    skills_data    = _results["skills_data"]
    online_info    = _results["online_info"]
    all_assets     = _results["all_assets"]
    if _cached_prices is not None:
        market_prices = _cached_prices
    else:
        market_prices = _results.get("market_prices", [])
        if market_prices:
            _PRICES_CACHE   = market_prices
            _PRICES_EXPIRES = time.time() + 1800  # 30-minute cache

    # Resolve system info + ship group in parallel
    t0 = time.perf_counter()
    system_id    = location.get("solar_system_id")
    ship_type_id = ship.get("ship_type_id")

    system_info = {}
    type_info   = {}
    _resolve = {}
    if system_id:    _resolve["sys"]  = (esi.get_system_info, system_id)
    if ship_type_id: _resolve["ship"] = (esi.get_type_info,   ship_type_id)
    if _resolve:
        with ThreadPoolExecutor(max_workers=2) as _p:
            for _k, _v in _p.map(lambda kv: (kv[0], _safe(lambda fn=kv[1][0], a=kv[1][1]: fn(a), {})), _resolve.items()):
                if _k == "sys":  system_info = _v
                if _k == "ship": type_info   = _v

    security_status = system_info.get("security_status")
    system_name     = system_info.get("name")
    ship_group_id   = type_info.get("group_id")
    print(f"  [t] system+ship resolve: {time.perf_counter()-t0:.2f}s")

    # Keep characters table current
    if public_info:
        store.upsert_character(character_id, public_info)

    # Online / session state
    is_online   = bool(online_info.get("online"))
    last_login  = online_info.get("last_login")
    last_logout = online_info.get("last_logout")

    # If offline, ensure no sessions are left open (handles crashes/missed logouts)
    if not is_online and last_logout:
        stale = store.get_open_session(character_id)
        if stale:
            print(f"  [session] Closing stale open session (char offline since {last_logout})")
            store.close_session(character_id, stale["started_at"], last_logout, 0.0)

    # --- Previous snapshot for delta/session comparison ---
    prev = store.get_last_snapshot(character_id)

    # --- Session tracking via last_login / last_logout changes ---
    if prev:
        prev_last_login  = prev.get("last_login")
        prev_last_logout = prev.get("last_logout")

        # New login detected
        if last_login and last_login != prev_last_login:
            # Guard against ESI timestamp jitter (same session can vary ±5s across calls)
            near_dup = store.conn.execute(
                """SELECT id FROM sessions WHERE character_id=?
                   AND ABS(CAST((JULIANDAY(started_at) - JULIANDAY(?)) * 86400 AS INTEGER)) < 120""",
                (character_id, last_login),
            ).fetchone()
            if near_dup:
                print(f"  [session] Suppressed near-duplicate login at {last_login} (existing id={near_dup[0]})")
            else:
                print(f"  [session] Login detected at {last_login}")
                session_type = detect_session_type(ship_group_id)
                store.save_session_start(
                    character_id, last_login,
                    ship_type_id, ship.get("ship_name"),
                    system_id, system_name, security_status,
                    session_type=session_type,
                )
                analytics.capture_session_start(character_id, {
                    "started_at":      last_login,
                    "ship_type_id":    ship_type_id,
                    "ship_name":       ship.get("ship_name"),
                    "solar_system_id": system_id,
                    "system_name":     system_name,
                    "security_status": security_status,
                    "session_type":    session_type,
                })

        # New logout detected
        if last_logout and last_logout != prev_last_logout:
            print(f"  [session] Logout detected at {last_logout}")
            open_session = store.get_open_session(character_id)
            session_isk = 0.0
            duration_hours = None

            if open_session:
                session_journal = store.get_journal_entries_between(
                    character_id, open_session["started_at"], last_logout
                )
                session_isk = sum(
                    e["amount"] for e in session_journal
                    if e["amount"] > 0 and e.get("ref_type") not in ESCROW_RETURN_TYPES
                )
                try:
                    t0 = datetime.fromisoformat(open_session["started_at"].replace("Z", "+00:00"))
                    t1 = datetime.fromisoformat(last_logout.replace("Z", "+00:00"))
                    duration_hours = round((t1 - t0).total_seconds() / 3600, 2)
                except Exception:
                    pass
                store.close_session(character_id, open_session["started_at"], last_logout, session_isk)

            analytics.capture_session_end(character_id, {
                "ended_at":        last_logout,
                "isk_earned":      round(session_isk, 2),
                "duration_hours":  duration_hours,
                "ship_type_id":    ship_type_id,
                "ship_name":       ship.get("ship_name"),
                "solar_system_id": system_id,
                "system_name":     system_name,
                "security_status": security_status,
            })
            print(f"  [session] ISK earned this session: {session_isk:,.0f}")

    # --- Killmails ---
    t0 = time.perf_counter()
    recent_km_list = _safe(lambda: esi.get_killmails_recent(character_id), [])
    candidate_ids  = [km["killmail_id"] for km in recent_km_list]
    unseen_ids     = store.get_unseen_killmail_ids(character_id, candidate_ids)
    km_id_to_hash  = {km["killmail_id"]: km["killmail_hash"] for km in recent_km_list}

    new_killmails = []
    for km_id in unseen_ids:
        km_hash = km_id_to_hash[km_id]
        detail = _safe(lambda: esi.get_killmail(km_id, km_hash), {})
        if not detail:
            continue

        is_loss = int(detail.get("victim", {}).get("character_id") == character_id)
        lost_ship_type = detail.get("victim", {}).get("ship_type_id")
        km_system_id   = detail.get("solar_system_id")
        km_time        = detail.get("killmail_time", now)

        new_killmails.append({
            "killmail_id":     km_id,
            "killmail_hash":   km_hash,
            "kill_time":       km_time,
            "is_loss":         is_loss,
            "ship_type_id":    lost_ship_type,
            "solar_system_id": km_system_id,
        })

        if is_loss:
            # Resolve names for the loss event
            loss_system_info = _safe(lambda: esi.get_system_info(km_system_id), {}) if km_system_id else {}
            loss_type_info   = _safe(lambda: esi.get_type_info(lost_ship_type), {}) if lost_ship_type else {}
            print(f"  [loss] Ship lost: {loss_type_info.get('name', lost_ship_type)} in {loss_system_info.get('name', km_system_id)}")
            analytics.capture_ship_loss(character_id, {
                "kill_time":        km_time,
                "ship_type_id":     lost_ship_type,
                "ship_name":        loss_type_info.get("name"),
                "solar_system_id":  km_system_id,
                "system_name":      loss_system_info.get("name"),
                "security_status":  loss_system_info.get("security_status"),
                "killmail_id":      km_id,
            })

    if new_killmails:
        store.save_killmails(character_id, new_killmails)
    print(f"  [t] killmails ({len(unseen_ids)} new): {time.perf_counter()-t0:.2f}s")

    since_24h     = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recent_losses = store.get_recent_losses(character_id, since_24h)

    # --- Save journal and skills ---
    t0 = time.perf_counter()
    if journal_raw:
        store.save_journal_entries(character_id, journal_raw)
    if skills_data.get("skills"):
        store.save_skills(character_id, skills_data["skills"])
    print(f"  [t] journal+skills save ({len(journal_raw)} entries): {time.perf_counter()-t0:.2f}s")

    # Resolve and cache skill names (only for skills with NULL name)
    t0 = time.perf_counter()
    unnamed = [r[0] for r in store.conn.execute(
        "SELECT skill_id FROM skills WHERE character_id=? AND skill_name IS NULL",
        (character_id,)
    ).fetchall()]
    if unnamed:
        try:
            resolved = esi.get_universe_names(unnamed[:1000])
            name_map = {e["id"]: e["name"] for e in resolved if e.get("category") == "inventory_type"}
            store.update_skill_names(character_id, name_map)
            print(f"  [skills] Resolved {len(name_map)} skill names")
        except Exception as e:
            print(f"  [warn] Skill name resolution failed: {e}")
    print(f"  [t] skill names ({len(unnamed)} unnamed): {time.perf_counter()-t0:.2f}s")

    # Resolve and cache skill group names (only for named skills missing a group)
    t0 = time.perf_counter()
    ungrouped = [r[0] for r in store.conn.execute(
        "SELECT skill_id FROM skills WHERE character_id=? AND skill_name IS NOT NULL AND group_name IS NULL",
        (character_id,)
    ).fetchall()]
    if ungrouped:
        try:
            def _fetch_skill_group(sid):
                try:
                    info = esi.get_type_info(sid)
                    gid  = info.get("group_id")
                    if gid:
                        return sid, esi.get_group_info(gid).get("name", "")
                except Exception:
                    pass
                return sid, None

            group_map = {}
            with ThreadPoolExecutor(max_workers=10) as pool:
                for sid, gname in pool.map(_fetch_skill_group, ungrouped[:500]):
                    if gname:
                        group_map[sid] = gname
            if group_map:
                store.update_skill_groups(character_id, group_map)
                print(f"  [skills] Resolved {len(group_map)} skill groups")
        except Exception as e:
            print(f"  [warn] Skill group resolution failed: {e}")
    print(f"  [t] skill groups ({len(ungrouped)} ungrouped): {time.perf_counter()-t0:.2f}s")

    # --- Compute metrics ---
    # Total ISK/hr = wallet delta (real net flow). Category breakdown from journal.
    isk_rates     = None
    activity_type = "Unknown"

    # Journal-based breakdown + activity (last 24h)
    journal_24h    = store.get_journal_entries_between(character_id, since_24h, now)
    journal_rates  = compute_isk_rates(journal_24h, since_24h, now) if journal_24h else None
    if journal_24h:
        activity_type = detect_activity_type(journal_24h, since_24h)

    # Ship type is a more reliable signal for Mining/Exploration:
    # their income flows through market_transaction (excluded as trade), so the
    # journal analysis can't detect them. Override here when the ship says otherwise.
    ship_session_type = detect_session_type(ship_group_id)
    if is_online and ship_session_type in ("Mining", "Exploration"):
        activity_type = ship_session_type

    if prev and prev.get("wallet_balance") is not None and wallet_balance is not None:
        prev_dt    = datetime.fromisoformat(prev["captured_at"])
        now_dt     = datetime.fromisoformat(now)
        elapsed_h  = max((now_dt - prev_dt).total_seconds() / 3600, 0.01)
        delta      = wallet_balance - prev["wallet_balance"]
        isk_hr     = round(delta / elapsed_h, 2)
        isk_rates  = {
            "isk_hour":          isk_hr,
            "isk_hour_bounty":   journal_rates["isk_hour_bounty"]   if journal_rates else 0.0,
            "isk_hour_trade":    journal_rates["isk_hour_trade"]     if journal_rates else 0.0,
            "isk_hour_industry": journal_rates["isk_hour_industry"]  if journal_rates else 0.0,
            "isk_hour_other":    journal_rates["isk_hour_other"]     if journal_rates else 0.0,
        }

    # --- Estimated total account value = wallet + assets at adjusted market price ---
    price_map = {p["type_id"]: p.get("adjusted_price", 0.0) for p in market_prices}
    asset_value = sum(
        a.get("quantity", 1) * price_map.get(a["type_id"], 0.0)
        for a in all_assets
    )
    estimated_value = round((wallet_balance or 0.0) + asset_value, 2)
    print(f"  [value] wallet={wallet_balance:,.0f}  assets={asset_value:,.0f}  total={estimated_value:,.0f}")

    # --- Wealth ISK/hr (unrealized gains rate) ---
    # Delta of total estimated account value between snapshots / elapsed hours
    wealth_isk_hour = 0.0
    if prev and prev.get("estimated_value") is not None:
        prev_est = prev["estimated_value"]
        prev_dt  = datetime.fromisoformat(prev["captured_at"])
        now_dt   = datetime.fromisoformat(now)
        elapsed_h = max((now_dt - prev_dt).total_seconds() / 3600, 0.01)
        wealth_isk_hour = round((estimated_value - prev_est) / elapsed_h, 2)

    # --- At-risk value: ship hull + fitted/cargo assets when in space ---
    # Character is in space if no station and no structure
    in_space = not location.get("station_id") and not location.get("structure_id")
    at_risk_value = 0.0
    if in_space and all_assets and market_prices:
        ship_item_id = ship.get("ship_item_id")
        # Ship hull value
        if ship_type_id:
            at_risk_value += price_map.get(ship_type_id, 0.0)
        # All assets located ON the ship (location_id = ship_item_id)
        if ship_item_id:
            for a in all_assets:
                if a.get("location_id") == ship_item_id:
                    at_risk_value += a.get("quantity", 1) * price_map.get(a["type_id"], 0.0)
        at_risk_value = round(at_risk_value, 2)
        print(f"  [risk] In space — at_risk_value={at_risk_value:,.0f}")

    risk_level = compute_risk_level(security_status, ship_group_id, recent_losses, at_risk_value)

    # --- Build and save snapshot ---
    snap = {
        "character_id":    character_id,
        "character_name":  public_info.get("name"),
        "corporation_id":  public_info.get("corporation_id"),
        "captured_at":     now,
        "wallet_balance":  wallet_balance,
        "solar_system_id": system_id,
        "system_name":     system_name,
        "station_id":      location.get("station_id"),
        "structure_id":    location.get("structure_id"),
        "ship_type_id":    ship_type_id,
        "ship_name":       ship.get("ship_name"),
        "ship_group_id":   ship_group_id,
        "security_status": security_status,
        "total_sp":        skills_data.get("total_sp"),
        "unallocated_sp":  skills_data.get("unallocated_sp"),
        "isk_hour":        isk_rates["isk_hour"] if isk_rates else 0.0,
        "isk_hour_bounty": isk_rates["isk_hour_bounty"] if isk_rates else 0.0,
        "isk_hour_trade":  isk_rates["isk_hour_trade"] if isk_rates else 0.0,
        "isk_hour_industry": isk_rates["isk_hour_industry"] if isk_rates else 0.0,
        "isk_hour_other":  isk_rates["isk_hour_other"] if isk_rates else 0.0,
        "risk_level":      risk_level,
        "activity_type":   activity_type,
        "recent_losses":   recent_losses,
        "online":           int(is_online),
        "last_login":       last_login,
        "last_logout":      last_logout,
        "estimated_value":  estimated_value,
        "wealth_isk_hour":  wealth_isk_hour,
        "at_risk_value":    at_risk_value,
    }

    store.save_snapshot(snap)
    analytics.capture_hourly_snapshot(character_id, snap)
    analytics.flush()

    print(f"[sync] Done — {public_info.get('name')} | {wallet_balance:,.0f} ISK | {'ONLINE' if is_online else 'offline'} | risk: {risk_level} | activity: {activity_type}")
    print(f"[sync] [t] total hourly snapshot: {time.perf_counter()-t0_total:.2f}s")
    return snap
