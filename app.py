"""EVE Character Tracker — public multi-character web app."""

import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

load_dotenv()

from src.eve_client.analytics import Analytics
from src.eve_client.asset_tracker import run_asset_tracking
from src.eve_client.auth import EveAuth
from src.eve_client.esi import ESIClient
from src.eve_client.hourly_pipeline import run_hourly_snapshot
from src.eve_client.metrics import compute_trade_pnl, compute_cancelled_order_losses, ESCROW_RETURN_TYPES
from src.eve_client.store import SnapshotStore
from src.eve_client.trading_pipeline import run_trading_sync

# ── Globals ───────────────────────────────────────────────────────────────────

store = SnapshotStore("eve_snapshots.db")

auth = EveAuth(
    client_id=os.environ["EVE_CLIENT_ID"],
    client_secret=os.environ["EVE_CLIENT_SECRET"],
    callback_url=os.environ["EVE_CALLBACK_URL"],
    store=store,
)
analytics = Analytics(
    api_key=os.environ["POSTHOG_API_KEY"],
    host=os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com"),
)

scheduler          = BackgroundScheduler()
_sync_jobs:         dict[int, dict] = {}   # character_id → {running, last, error}
_trading_sync_jobs: dict[int, dict] = {}   # character_id → {running, last, error}


# ── Scheduler helpers ─────────────────────────────────────────────────────────

def _run_full_sync(character_id: int):
    """Core sync logic shared by scheduler and manual trigger."""
    _sync_jobs.setdefault(character_id, {})["running"] = True
    _sync_jobs[character_id]["error"] = None
    try:
        run_hourly_snapshot(character_id, auth, analytics, store)
        token = auth.get_valid_token(character_id)
        esi = ESIClient(access_token=token)
        try:
            run_trading_sync(character_id, auth, store, esi)
        finally:
            esi.close()
        now = datetime.now(timezone.utc).isoformat()
        _sync_jobs[character_id]["last"] = now
        _trading_sync_jobs.setdefault(character_id, {})["last"] = now
        _trading_sync_jobs[character_id]["running"] = False
        _trading_sync_jobs[character_id]["error"] = None
    except Exception as e:
        _sync_jobs[character_id]["error"] = str(e)
        print(f"[scheduler] Error syncing {character_id}: {e}")
    finally:
        _sync_jobs[character_id]["running"] = False


def _make_sync_fn(character_id: int):
    def _sync():
        # When offline, only sync hourly to save API calls
        last = store.get_last_snapshot(character_id)
        if last and not last.get("online"):
            try:
                elapsed_min = (datetime.now(timezone.utc) -
                               datetime.fromisoformat(last["captured_at"])
                               ).total_seconds() / 60
                if elapsed_min < 55:
                    return
            except Exception:
                pass
        _run_full_sync(character_id)
    return _sync


def _make_asset_fn(character_id: int):
    def _track():
        try:
            run_asset_tracking(character_id, auth, store)
        except Exception as e:
            print(f"[asset_tracker] Error for {character_id}: {e}")
    return _track


def register_character_job(character_id: int, run_now: bool = True):
    job_id = f"sync_{character_id}"
    if not scheduler.get_job(job_id):
        scheduler.add_job(
            _make_sync_fn(character_id),
            trigger=IntervalTrigger(minutes=10),
            id=job_id,
            next_run_time=datetime.now(timezone.utc) if run_now else None,
        )
        _sync_jobs[character_id] = {"running": False, "last": None, "error": None}

    asset_job_id = f"assets_{character_id}"
    if not scheduler.get_job(asset_job_id):
        scheduler.add_job(
            _make_asset_fn(character_id),
            trigger=IntervalTrigger(minutes=10),
            id=asset_job_id,
            next_run_time=datetime.now(timezone.utc) if run_now else None,
        )

    print(f"[scheduler] Registered jobs for character {character_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schedule all already-registered characters
    for cid in store.get_all_character_ids():
        register_character_job(cid, run_now=True)

    if not scheduler.running:
        scheduler.start()
    print(f"[app] Scheduler started with {len(store.get_all_character_ids())} character(s).")
    yield
    scheduler.shutdown(wait=False)


# ── FastAPI ───────────────────────────────────────────────────────────────────

app       = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


def _fmt_isk(val) -> str:
    """Full integer with commas up to 1T; T suffix beyond that."""
    if val is None:
        return "—"
    if val >= 1_000_000_000_000:
        return f"{val / 1_000_000_000_000:,.2f}T"
    return f"{val:,.0f}"


templates.env.filters["fmt_isk"] = _fmt_isk


def _safe_esi(fn, default=None):
    """Run an ESI call, returning default on any exception."""
    try:
        return fn()
    except Exception as e:
        print(f"  [esi] {e}")
        return default


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.get("/auth/start")
def auth_start():
    return RedirectResponse(auth.get_auth_url())


@app.get("/auth/callback")
@app.get("/callback")
def auth_callback(code: str):
    character_id = auth.exchange_code(code)
    if not scheduler.running:
        scheduler.start()
    register_character_job(character_id, run_now=True)
    return RedirectResponse(f"/c/{character_id}")


# ── Public pages ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    characters  = store.get_all_characters()
    recent_snaps = []
    recent_losses = []

    for ch in characters:
        snap = store.get_last_snapshot(ch["character_id"])
        if snap:
            recent_snaps.append(snap)

    # Recent losses across all characters
    rows = store.conn.execute(
        """SELECT k.*, c.character_name
           FROM killmails k
           LEFT JOIN characters c ON k.character_id = c.character_id
           WHERE k.is_loss = 1
           ORDER BY k.kill_time DESC LIMIT 20"""
    ).fetchall()
    recent_losses = [dict(r) for r in rows]

    return templates.TemplateResponse("landing.html", {
        "request": request,
        "characters": characters,
        "recent_snaps": {s["character_id"]: s for s in recent_snaps},
        "recent_losses": recent_losses,
    })


@app.get("/c/{character_id}", response_class=HTMLResponse)
def character_page(request: Request, character_id: int):
    character = store.get_character(character_id)
    if not character:
        return HTMLResponse("<h2>Character not tracked. <a href='/auth/start'>Add yours?</a></h2>", status_code=404)
    current   = store.get_last_snapshot(character_id) or {}
    snapshots = store.get_snapshots(character_id, limit=168)
    sessions  = store.conn.execute(
        "SELECT * FROM sessions WHERE character_id=? ORDER BY started_at DESC LIMIT 20",
        (character_id,)
    ).fetchall()
    losses = store.conn.execute(
        "SELECT * FROM killmails WHERE character_id=? AND is_loss=1 ORDER BY kill_time DESC LIMIT 20",
        (character_id,)
    ).fetchall()
    status = _sync_jobs.get(character_id, {})
    skills = store.get_skills(character_id)

    return templates.TemplateResponse("character.html", {
        "request":    request,
        "character":  character,
        "current":    current,
        "snapshots":  snapshots,
        "sessions":   [dict(s) for s in sessions],
        "losses":     [dict(l) for l in losses],
        "sync_status": status,
        "skills":     skills,
    })


@app.get("/skills/{character_id}", response_class=HTMLResponse)
def skills_page(request: Request, character_id: int):
    character = store.get_character(character_id)
    if not character:
        return RedirectResponse("/")
    current    = store.get_last_snapshot(character_id) or {}
    skill_rows = store.get_skills(character_id)
    return templates.TemplateResponse("skills.html", {
        "request":      request,
        "current":      current,
        "skills":       skill_rows,
        "character_id": character_id,
        "character":    character,
    })


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/characters")
def api_characters():
    chars  = store.get_all_characters()
    result = []
    for ch in chars:
        snap = store.get_last_snapshot(ch["character_id"])
        result.append({**ch, "latest_snapshot": snap})
    return result


@app.get("/api/c/{character_id}/current")
def api_current(character_id: int):
    return store.get_last_snapshot(character_id) or {}


def _build_isk_buckets(journal: list, since_dt: datetime, until_dt: datetime,
                        minutes: int = 20, sessions: list | None = None) -> list:
    bucket_size = timedelta(minutes=minutes)
    buckets = []
    t = since_dt
    while t < until_dt:
        t_end = min(t + bucket_size, until_dt)
        earned = 0.0
        for e in journal:
            if e["amount"] <= 0:
                continue
            try:
                ed = datetime.fromisoformat(e["date"].replace("Z", "+00:00"))
            except Exception:
                continue
            if t <= ed < t + bucket_size:
                earned += e["amount"]
        elapsed_h = (t_end - t).total_seconds() / 3600
        buckets.append({
            "start":      t.isoformat(),
            "label":      t.strftime("%H:%M") if minutes < 60 else t.strftime("%m/%d %H:%M"),
            "isk_earned": round(earned, 2),
            "loot_value": 0.0,
            "isk_hour":   round(earned / elapsed_h, 2) if elapsed_h > 0 else 0.0,
        })
        t += bucket_size

    # Distribute each session's assets_gained_value evenly across its buckets
    if sessions:
        for s in sessions:
            loot = s.get("assets_gained_value") or 0.0
            if not loot:
                continue
            try:
                s_start = datetime.fromisoformat(s["started_at"].replace("Z", "+00:00"))
                s_end   = datetime.fromisoformat(s["ended_at"].replace("Z", "+00:00")) if s.get("ended_at") else until_dt
            except Exception:
                continue
            idxs = [i for i, b in enumerate(buckets)
                    if datetime.fromisoformat(b["start"]) + bucket_size > s_start
                    and datetime.fromisoformat(b["start"]) < s_end]
            if idxs:
                per_bucket = loot / len(idxs)
                for i in idxs:
                    buckets[i]["loot_value"] = round(buckets[i]["loot_value"] + per_bucket, 2)

    return buckets


def _extract_timeline_events(snaps: list, sessions: list, buckets: list, bucket_minutes: int = 20) -> list:
    if not buckets:
        return []
    _bucket_size = timedelta(minutes=bucket_minutes)

    def _bucket_idx(ts_str: str) -> int:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            for i, b in enumerate(buckets):
                b_start = datetime.fromisoformat(b["start"])
                if b_start <= ts < b_start + _bucket_size:
                    return i
            # clamp to last bucket if after end
            last = datetime.fromisoformat(buckets[-1]["start"])
            if ts >= last:
                return len(buckets) - 1
        except Exception:
            pass
        return -1

    events = []
    prev = None
    for snap in snaps:
        if prev:
            if snap.get("solar_system_id") != prev.get("solar_system_id"):
                bi = _bucket_idx(snap["captured_at"])
                if bi >= 0:
                    events.append({"bucket_index": bi, "timestamp": snap["captured_at"],
                                   "type": "jump",
                                   "label": f"{prev.get('system_name','?')} → {snap.get('system_name','?')}"})
            prev_docked = prev.get("station_id") or prev.get("structure_id")
            curr_docked = snap.get("station_id") or snap.get("structure_id")
            if not prev_docked and curr_docked:
                bi = _bucket_idx(snap["captured_at"])
                if bi >= 0:
                    events.append({"bucket_index": bi, "timestamp": snap["captured_at"],
                                   "type": "dock", "label": "Docked"})
            elif prev_docked and not curr_docked:
                bi = _bucket_idx(snap["captured_at"])
                if bi >= 0:
                    events.append({"bucket_index": bi, "timestamp": snap["captured_at"],
                                   "type": "undock", "label": "Undocked"})
            if snap.get("ship_type_id") and snap.get("ship_type_id") != prev.get("ship_type_id"):
                bi = _bucket_idx(snap["captured_at"])
                if bi >= 0:
                    events.append({"bucket_index": bi, "timestamp": snap["captured_at"],
                                   "type": "ship", "label": f"Ship: {snap.get('ship_name','?')}"})
        prev = snap

    for s in sessions:
        bi = _bucket_idx(s["started_at"])
        if bi >= 0:
            events.append({"bucket_index": bi, "timestamp": s["started_at"],
                           "type": "login", "label": "Session Start"})
        if s.get("ended_at"):
            bi = _bucket_idx(s["ended_at"])
            if bi >= 0:
                events.append({"bucket_index": bi, "timestamp": s["ended_at"],
                               "type": "logout", "label": "Session End"})

    # Deduplicate (same bucket + type), sort chronologically
    seen: set = set()
    deduped = []
    for e in sorted(events, key=lambda x: x["timestamp"]):
        key = (e["bucket_index"], e["type"])
        if key not in seen:
            seen.add(key)
            deduped.append(e)
    return deduped


def _build_session_isk(sessions: list, journal: list) -> list:
    now = datetime.now(timezone.utc)
    result = []
    for s in sessions:
        start = s["started_at"].replace("Z", "+00:00")
        end   = s["ended_at"].replace("Z", "+00:00") if s.get("ended_at") else now.isoformat()
        earned = sum(
            e["amount"] for e in journal
            if e["amount"] > 0
            and e.get("ref_type") not in ESCROW_RETURN_TYPES
            and start <= e["date"].replace("Z", "+00:00") < end
        )
        try:
            hours = max((datetime.fromisoformat(end) - datetime.fromisoformat(start))
                        .total_seconds() / 3600, 0.01)
        except Exception:
            hours = 1.0
        try:
            label = datetime.fromisoformat(s["started_at"].replace("Z", "+00:00")).strftime("%b %d %H:%M")
        except Exception:
            label = s["started_at"][:16]
        assets_gained = s.get("assets_gained_value") or 0.0
        assets_lost   = s.get("assets_lost_value") or 0.0
        net_assets    = assets_gained - assets_lost
        total_value   = round(earned + net_assets, 2)
        result.append({
            "label":          label,
            "started_at":     s["started_at"],
            "ended_at":       s.get("ended_at"),
            "session_type":   s.get("session_type"),
            "isk_earned":     round(earned, 2),
            "assets_gained":  round(assets_gained, 2),
            "assets_lost":    round(assets_lost, 2),
            "net_assets":     round(net_assets, 2),
            "total_value":    total_value,
            "isk_hour":       round(total_value / hours, 2),
            "duration_h":     round(hours, 2),
            "active":         not bool(s.get("ended_at")),
        })
    return result


def _aggregate_snaps(snaps: list[dict], granularity: str) -> list[dict]:
    """Bucket hourly snapshots into day/month aggregates for the history API."""
    def bucket(s: dict) -> str:
        ts = s["captured_at"]
        if granularity == "day":   return ts[:10]       # YYYY-MM-DD
        if granularity == "month": return ts[:7]        # YYYY-MM
        return ts[:16]

    buckets: dict[str, list[dict]] = defaultdict(list)
    for s in snaps:
        buckets[bucket(s)].append(s)

    RATE_FIELDS = ("isk_hour", "isk_hour_bounty", "isk_hour_trade",
                   "isk_hour_industry", "isk_hour_other", "wealth_isk_hour")
    result = []
    for key in sorted(buckets):
        group = buckets[key]
        last  = group[-1]
        def avg(f: str) -> float:
            vals = [s.get(f) or 0 for s in group]
            return round(sum(vals) / len(vals), 2)
        result.append({**last, **{f: avg(f) for f in RATE_FIELDS}})
    return result


@app.get("/api/c/{character_id}/history")
def api_history(character_id: int, days: int = 7, granularity: str = "hour"):
    from datetime import timedelta
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    snaps = store.get_snapshots_since(character_id, since)
    snaps.reverse()  # oldest first
    if granularity == "hour":
        return snaps
    return _aggregate_snaps(snaps, granularity)


@app.get("/api/c/{character_id}/isk-timeline")
def api_isk_timeline(character_id: int, hours: int = 24):
    now      = datetime.now(timezone.utc)
    since_dt = now - timedelta(hours=hours)
    since    = since_dt.isoformat()

    journal  = store.get_journal_entries_between(character_id, since, now.isoformat())
    snaps    = list(reversed(store.get_snapshots_since(character_id, since)))  # chronological

    sessions_rows = store.conn.execute(
        "SELECT * FROM sessions WHERE character_id=? ORDER BY started_at DESC LIMIT 30",
        (character_id,),
    ).fetchall()
    sessions = [dict(s) for s in reversed(sessions_rows)]  # chronological

    bucket_minutes = 60 if hours > 48 else 20
    buckets  = _build_isk_buckets(journal, since_dt, now, bucket_minutes, sessions=sessions)
    events   = _extract_timeline_events(snaps, [s for s in sessions if s["started_at"] >= since], buckets, bucket_minutes)
    session_data = _build_session_isk(sessions, journal)

    journal_count = len(journal)
    income_count  = sum(
        1 for e in journal
        if e["amount"] > 0 and e.get("ref_type") not in ESCROW_RETURN_TYPES
    )

    return {
        "buckets": buckets, "events": events, "sessions": session_data,
        "journal_count": journal_count, "income_count": income_count,
        "bucket_minutes": bucket_minutes,
    }


@app.get("/api/c/{character_id}/fitting")
def api_fitting(character_id: int):
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    ship       = esi.get_ship(character_id)
    ship_item  = ship["ship_item_id"]
    type_info  = esi.get_type_info(ship["ship_type_id"])
    all_assets = esi.get_assets_all(character_id)
    fitted     = [a for a in all_assets if a.get("location_id") == ship_item]
    prices_raw = esi.get_market_prices()
    price_map  = {p["type_id"]: p.get("average_price") or p.get("adjusted_price", 0.0) for p in prices_raw}

    type_ids = list({a["type_id"] for a in fitted})
    names    = {
        e["id"]: e["name"]
        for e in esi.get_universe_names(type_ids)
        if e.get("category") == "inventory_type"
    }

    SLOT_GROUPS = {
        "high":  [f"HiSlot{i}"  for i in range(8)],
        "mid":   [f"MedSlot{i}" for i in range(8)],
        "low":   [f"LoSlot{i}"  for i in range(8)],
        "rig":   [f"RigSlot{i}" for i in range(3)],
        "drone": ["DroneBay"],
        "cargo": ["Cargo"],
    }
    slots = defaultdict(list)
    for item in fitted:
        flag     = item.get("location_flag", "Other")
        name     = names.get(item["type_id"], str(item["type_id"]))
        qty      = item.get("quantity", 1)
        val      = round(qty * price_map.get(item["type_id"], 0.0), 2)
        for group, flags in SLOT_GROUPS.items():
            if flag in flags:
                slots[group].append({"name": name, "qty": qty, "flag": flag, "value": val})
                break
        else:
            slots["other"].append({"name": name, "qty": qty, "flag": flag, "value": val})

    hull_value  = round(price_map.get(ship["ship_type_id"], 0.0), 2)
    slot_totals = {grp: round(sum(i["value"] for i in items), 2) for grp, items in slots.items()}
    total_value = round(hull_value + sum(slot_totals.values()), 2)

    location = esi.get_location(character_id)
    in_space = not location.get("station_id") and not location.get("structure_id")

    return {
        "ship_name":       ship["ship_name"],
        "ship_type":       type_info.get("name"),
        "ship_type_id":    ship["ship_type_id"],
        "hull_value":      hull_value,
        "total_value":     total_value,
        "slot_totals":     slot_totals,
        "slots":           dict(slots),
        "in_space":        in_space,
    }


def _extract_skill_reqs(type_info: dict) -> list[dict]:
    """Extract required skills from dogma attributes."""
    SKILL_MAP = {182:277, 183:278, 184:279, 1285:1286, 1289:1287, 1290:1288}
    attrs = {a['attribute_id']: a['value'] for a in type_info.get('dogma_attributes', [])}
    reqs = []
    for skill_attr, level_attr in SKILL_MAP.items():
        if skill_attr in attrs and level_attr in attrs:
            sid = int(attrs[skill_attr])
            lvl = int(attrs[level_attr])
            if sid > 0 and lvl > 0:
                reqs.append({"skill_id": sid, "level": lvl})
    return reqs


# Skill group → performance category
# Navigation (speed/agility/durability): Navigation, Spaceship Command, Engineering, Armor, Shields
_NAV_GROUPS  = {257, 275, 1216, 1210, 1209}
# Damage: Gunnery, Missile Launcher Operation, Drones
_DMG_GROUPS  = {255, 256, 273}
# Utility: everything else (Scanning/Science 1217/272, Electronics 270, etc.)


def _resolve_skill_meta(skill_ids: list[int], esi: ESIClient) -> tuple[dict, dict]:
    """Return (skill_names, skill_groups) for a list of skill type IDs."""
    skill_names: dict[int, str] = {}
    skill_groups: dict[int, int] = {}
    if not skill_ids:
        return skill_names, skill_groups
    try:
        resolved = esi.get_universe_names(skill_ids)
        skill_names = {e["id"]: e["name"] for e in resolved if e.get("category") == "inventory_type"}
    except Exception:
        pass

    def _fetch_group(sid):
        try:
            return sid, esi.get_type_info(sid).get("group_id", 0)
        except Exception:
            return sid, 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        for sid, gid in pool.map(_fetch_group, skill_ids):
            skill_groups[sid] = gid

    return skill_names, skill_groups


def _build_performance(
    all_reqs: dict,
    char_skills: dict,
    skill_names: dict,
    skill_groups: dict,
) -> dict:
    """Categorize skills, score each bucket, return categories + overall."""
    nav_reqs  = {s: r for s, r in all_reqs.items() if skill_groups.get(s, 0) in _NAV_GROUPS}
    dmg_reqs  = {s: r for s, r in all_reqs.items() if skill_groups.get(s, 0) in _DMG_GROUPS}
    util_reqs = {s: r for s, r in all_reqs.items()
                 if skill_groups.get(s, 0) not in _NAV_GROUPS | _DMG_GROUPS}

    def _score(reqs: dict) -> tuple[float, list]:
        total, max_total = 0.0, 0.0
        details = []
        for sid, req in reqs.items():
            trained  = char_skills.get(sid, 0)
            required = req["level"]
            total     += min(trained, 5) * required
            max_total += 5 * required
            details.append({
                "skill_id":    sid,
                "skill_name":  skill_names.get(sid, f"Skill {sid}"),
                "trained":     trained,
                "required":    required,
                "required_by": req.get("required_by", ""),
            })
        pct = round(total / max_total * 100, 1) if max_total else 100.0
        details.sort(key=lambda r: r["trained"] / 5)
        return pct, details

    nav_pct, nav_skills   = _score(nav_reqs)
    dmg_pct, dmg_skills   = _score(dmg_reqs)
    util_pct, util_skills = _score(util_reqs)

    has_damage = bool(dmg_reqs)
    cat_scores = [nav_pct, util_pct] + ([dmg_pct] if has_damage else [])
    overall    = round(sum(cat_scores) / len(cat_scores), 1) if cat_scores else 100.0

    return {
        "overall": overall,
        "categories": {
            "navigation": {"label": "Navigation & Durability", "score": nav_pct,  "skills": nav_skills,  "present": True},
            "damage":     {"label": "Damage",                  "score": dmg_pct,  "skills": dmg_skills,  "present": has_damage},
            "utility":    {"label": "Utility",                 "score": util_pct, "skills": util_skills, "present": bool(util_reqs)},
        },
    }


@app.get("/api/c/{character_id}/optimal")
def api_optimal(character_id: int):
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    ship       = esi.get_ship(character_id)
    ship_item  = ship["ship_item_id"]
    all_assets = esi.get_assets_all(character_id)
    fitted     = [a for a in all_assets if a.get("location_id") == ship_item]

    CARGO_FLAGS = {"Cargo", "DroneBay"}
    module_type_ids = {ship["ship_type_id"]}
    for a in fitted:
        if a.get("location_flag") not in CARGO_FLAGS:
            module_type_ids.add(a["type_id"])

    all_reqs: dict[int, dict] = {}
    item_names: dict[int, str] = {}
    for tid in module_type_ids:
        try:
            info = esi.get_type_info(tid)
            item_names[tid] = info.get("name", str(tid))
            for req in _extract_skill_reqs(info):
                sid = req["skill_id"]
                if sid not in all_reqs or req["level"] > all_reqs[sid]["level"]:
                    all_reqs[sid] = {"level": req["level"], "required_by": item_names[tid]}
        except Exception:
            pass

    skill_names, skill_groups = _resolve_skill_meta(list(all_reqs.keys()), esi)
    char_skills = store.get_skills_map(character_id)
    perf = _build_performance(all_reqs, char_skills, skill_names, skill_groups)

    return {
        "ship_name": ship["ship_name"],
        "ship_type": item_names.get(ship["ship_type_id"], "Unknown"),
        **perf,
    }


@app.get("/api/c/{character_id}/ships")
def api_ships(character_id: int):
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    all_assets = esi.get_assets_all(character_id)

    # Pre-filter to assembled (singleton) items in hangars — ships live here
    SHIP_FLAGS = {"Hangar", "AssetSafety", "FleetHangar", "CorpSAG1", "CorpSAG2",
                  "CorpSAG3", "CorpSAG4", "CorpSAG5", "CorpSAG6", "CorpSAG7"}
    type_counts: dict[int, int] = defaultdict(int)
    for a in all_assets:
        if a.get("is_singleton") and a.get("location_flag") in SHIP_FLAGS:
            type_counts[a["type_id"]] += 1

    if not type_counts:
        return []

    # Fetch type_info + group_info in parallel, filter to ships (group.category_id == 6)
    def _fetch_type(tid):
        try:
            return tid, esi.get_type_info(tid)
        except Exception:
            return tid, None

    type_infos: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=15) as pool:
        for tid, info in pool.map(_fetch_type, list(type_counts)):
            if info:
                type_infos[tid] = info

    # Resolve unique group_ids → category_id in parallel
    unique_gids = {info.get("group_id") for info in type_infos.values() if info.get("group_id")}

    def _fetch_group(gid):
        try:
            return gid, esi.get_group_info(gid)
        except Exception:
            return gid, {}

    group_infos: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        for gid, ginfo in pool.map(_fetch_group, list(unique_gids)):
            group_infos[gid] = ginfo

    # Filter to ships: group.category_id == 6
    ship_infos: dict[int, dict] = {}
    group_names: dict[int, str] = {}
    for tid, info in type_infos.items():
        gid = info.get("group_id")
        ginfo = group_infos.get(gid, {})
        if ginfo.get("category_id") == 6:
            ship_infos[tid] = info
            group_names[gid] = ginfo.get("name", "")

    if not ship_infos:
        return []

    # Collect all skill requirements across all ship hulls
    ship_reqs: dict[int, dict] = {}
    all_skill_ids: set[int] = set()
    for tid, info in ship_infos.items():
        reqs: dict[int, dict] = {}
        for req in _extract_skill_reqs(info):
            sid = req["skill_id"]
            if sid not in reqs or req["level"] > reqs[sid]["level"]:
                reqs[sid] = {"level": req["level"], "required_by": info.get("name", str(tid))}
            all_skill_ids.add(sid)
        ship_reqs[tid] = reqs

    skill_names, skill_groups = _resolve_skill_meta(list(all_skill_ids), esi)
    char_skills = store.get_skills_map(character_id)

    results = []
    for tid, info in ship_infos.items():
        reqs = ship_reqs[tid]
        if not reqs:
            continue
        perf = _build_performance(reqs, char_skills, skill_names, skill_groups)
        results.append({
            "type_id":    tid,
            "ship_name":  info.get("name", str(tid)),
            "group_name": group_names.get(info.get("group_id"), ""),
            "quantity":   type_counts[tid],
            **perf,
        })

    results.sort(key=lambda r: r["overall"])
    return results


@app.post("/api/c/{character_id}/sync")
def api_sync(character_id: int, background_tasks: BackgroundTasks):
    if not store.get_token(character_id):
        return JSONResponse({"error": "character not registered"}, status_code=404)
    if _sync_jobs.get(character_id, {}).get("running"):
        return JSONResponse({"status": "already_running"})
    # Mark running BEFORE handing off so status polls don't race
    _sync_jobs.setdefault(character_id, {})["running"] = True
    _sync_jobs[character_id]["error"] = None
    # Manual trigger bypasses the offline-hourly throttle
    background_tasks.add_task(_run_full_sync, character_id)
    return JSONResponse({"status": "started"})


@app.get("/api/c/{character_id}/status")
def api_status(character_id: int):
    return _sync_jobs.get(character_id, {"running": False, "last": None, "error": None})


# ── Trading ───────────────────────────────────────────────────────────────────

@app.get("/trading/{character_id}", response_class=HTMLResponse)
def trading_page(request: Request, character_id: int):
    character = store.get_character(character_id)
    if not character:
        return HTMLResponse("<h2>Character not tracked. <a href='/auth/start'>Add yours?</a></h2>", status_code=404)
    current = store.get_last_snapshot(character_id) or {}
    return templates.TemplateResponse("trading.html", {
        "request":   request,
        "character": character,
        "current":   current,
    })


@app.get("/api/c/{character_id}/trading-data")
def api_trading_data(character_id: int):
    transactions = store.get_transactions(character_id, days=30)
    from datetime import timedelta
    since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    until = datetime.now(timezone.utc).isoformat()
    journal = store.get_journal_entries_between(character_id, since, until)
    orders = store.get_orders(character_id)

    pnl = compute_trade_pnl(transactions, journal)
    cancelled = compute_cancelled_order_losses(orders)
    active = [o for o in orders if o["state"] == "active"]
    tracked = store.get_tracked_items(character_id)

    return {
        "pnl":             pnl,
        "cancelled_orders": cancelled,
        "active_orders":   active,
        "tracked_items":   tracked,
    }


@app.post("/api/c/{character_id}/sync-trading")
def api_sync_trading(character_id: int, background_tasks: BackgroundTasks):
    if not store.get_token(character_id):
        return JSONResponse({"error": "character not registered"}, status_code=404)
    if _trading_sync_jobs.get(character_id, {}).get("running"):
        return JSONResponse({"status": "already_running"})

    _trading_sync_jobs.setdefault(character_id, {})["running"] = True
    _trading_sync_jobs[character_id]["error"] = None

    def _do_sync():
        try:
            token = auth.get_valid_token(character_id)
            esi = ESIClient(access_token=token)
            run_trading_sync(character_id, auth, store, esi)
            esi.close()
            _trading_sync_jobs[character_id]["last"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            _trading_sync_jobs[character_id]["error"] = str(e)
            print(f"[trading] Error syncing {character_id}: {e}")
        finally:
            _trading_sync_jobs[character_id]["running"] = False

    background_tasks.add_task(_do_sync)
    return JSONResponse({"status": "started"})


@app.get("/api/c/{character_id}/trading-status")
def api_trading_status(character_id: int):
    return _trading_sync_jobs.get(character_id, {"running": False, "last": None, "error": None})


@app.get("/api/c/{character_id}/escrow")
def api_escrow(character_id: int):
    """Return ISK currently locked in escrow, bucketed by type."""
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)
    try:
        orders    = _safe_esi(lambda: esi.get_character_orders(character_id))
        contracts = _safe_esi(lambda: esi.get_contracts(character_id))
    finally:
        esi.close()

    orders    = orders    or []
    contracts = contracts or []

    # Market buy orders — escrow field is ISK locked per order
    buy_escrow_orders = [
        {
            "type_id":   o.get("type_id"),
            "type_name": o.get("type_name", f"Type {o.get('type_id')}"),
            "price":     o.get("price", 0.0),
            "volume":    o.get("volume_remain", 0),
            "escrow":    round(o.get("escrow", 0.0), 2),
            "location_id": o.get("location_id"),
        }
        for o in orders if o.get("is_buy_order") and o.get("state") == "active"
    ]
    buy_escrow_total = round(sum(o["escrow"] for o in buy_escrow_orders), 2)

    active_statuses = {"outstanding", "in_progress"}
    active_contracts = [c for c in contracts if c.get("status") in active_statuses]

    # Courier collateral YOU posted (you accepted a courier contract and put up collateral)
    collateral_locked = [
        {
            "contract_id": c.get("contract_id"),
            "collateral":  round(c.get("collateral", 0.0), 2),
            "reward":      round(c.get("reward", 0.0), 2),
            "status":      c.get("status"),
            "date_expired": c.get("date_expired"),
        }
        for c in active_contracts
        if c.get("type") == "courier"
        and c.get("acceptor_id") == character_id
        and c.get("status") == "in_progress"
    ]
    collateral_total = round(sum(c["collateral"] for c in collateral_locked), 2)

    # Courier rewards YOU posted (you issued a courier contract, reward in escrow)
    rewards_posted = [
        {
            "contract_id": c.get("contract_id"),
            "reward":      round(c.get("reward", 0.0), 2),
            "collateral":  round(c.get("collateral", 0.0), 2),
            "status":      c.get("status"),
            "date_expired": c.get("date_expired"),
        }
        for c in active_contracts
        if c.get("type") == "courier"
        and c.get("issuer_id") == character_id
    ]
    rewards_posted_total = round(sum(r["reward"] for r in rewards_posted), 2)

    # Auction bids you placed (ISK locked until outbid or won)
    auction_bids = [
        {
            "contract_id": c.get("contract_id"),
            "buyout":      round(c.get("buyout", 0.0), 2),
            "status":      c.get("status"),
            "date_expired": c.get("date_expired"),
        }
        for c in active_contracts
        if c.get("type") == "auction"
        and c.get("acceptor_id") == character_id
    ]
    auction_bids_total = round(sum(b["buyout"] for b in auction_bids), 2)

    total_in_escrow = round(
        buy_escrow_total + collateral_total + rewards_posted_total + auction_bids_total, 2
    )

    return {
        "total_in_escrow":     total_in_escrow,
        "buy_orders": {
            "total":   buy_escrow_total,
            "entries": sorted(buy_escrow_orders, key=lambda x: x["escrow"], reverse=True),
        },
        "collateral_locked": {
            "total":   collateral_total,
            "entries": collateral_locked,
        },
        "rewards_posted": {
            "total":   rewards_posted_total,
            "entries": rewards_posted,
        },
        "auction_bids": {
            "total":   auction_bids_total,
            "entries": auction_bids,
        },
    }


@app.get("/api/c/{character_id}/clone")
def api_clone(character_id: int):
    """Return active clone implant value and current skill training.

    Cache TTL: 1 hour when character is online/in active session, 24 hours otherwise.
    """
    now = datetime.now(timezone.utc)

    # Determine TTL based on whether character is currently online
    last_snap = store.get_last_snapshot(character_id)
    in_session = bool(last_snap and last_snap.get("online"))
    ttl_hours = 1 if in_session else 24

    # Return cached value if still fresh
    cached = store.get_clone_cache(character_id)
    if cached:
        try:
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            age_hours = (now - fetched_at).total_seconds() / 3600
            if age_hours < ttl_hours:
                return {
                    "implant_count": cached["implant_count"],
                    "implant_value": cached["implant_value"],
                    "training":      cached["training"],
                    "cached":        True,
                    "fetched_at":    cached["fetched_at"],
                }
        except Exception:
            pass  # Bad timestamp — fall through to refresh

    # Fetch fresh data from ESI
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)
    try:
        implants   = _safe_esi(lambda: esi.get_implants(character_id))   or []
        skillqueue = _safe_esi(lambda: esi.get_skillqueue(character_id)) or []
        prices_raw = _safe_esi(lambda: esi.get_market_prices())          or []
    finally:
        esi.close()

    price_map     = {p["type_id"]: p.get("average_price") or p.get("adjusted_price", 0.0) for p in prices_raw}
    implant_value = round(sum(price_map.get(tid, 0.0) for tid in implants), 2)
    if implants and not prices_raw:
        print(f"[clone] WARNING: market prices unavailable, implant_value will be 0")

    # Find the currently-training skill (earliest finish_date in the future)
    training = None
    for entry in sorted(skillqueue, key=lambda e: e.get("queue_position", 99)):
        fd = entry.get("finish_date")
        if not fd:
            continue
        try:
            finish = datetime.fromisoformat(fd.replace("Z", "+00:00"))
            if finish > now:
                hours_remaining = round((finish - now).total_seconds() / 3600, 1)
                skill_name = None
                row = store.conn.execute(
                    "SELECT skill_name FROM skills WHERE character_id=? AND skill_id=? AND skill_name IS NOT NULL",
                    (character_id, entry["skill_id"])
                ).fetchone()
                if row:
                    skill_name = row[0]
                training = {
                    "skill_id":        entry["skill_id"],
                    "skill_name":      skill_name or f"Skill {entry['skill_id']}",
                    "trained_level":   entry.get("finished_level", entry.get("level", 1)),
                    "finish_date":     fd,
                    "hours_remaining": hours_remaining,
                }
                break
        except Exception:
            continue

    store.set_clone_cache(character_id, len(implants), implant_value, training)

    return {
        "implant_count": len(implants),
        "implant_value": implant_value,
        "training":      training,
        "cached":        False,
        "fetched_at":    now.isoformat(),
    }


@app.post("/api/c/{character_id}/track-item")
async def api_track_item(request: Request, character_id: int):
    body = await request.json()
    type_id   = int(body["type_id"])
    region_id = int(body.get("region_id", 10000002))
    type_name = body.get("type_name")
    store.add_tracked_item(character_id, type_id, region_id, type_name=type_name)

    # Fetch market history and return it
    esi = None
    try:
        token = auth.get_valid_token(character_id)
        esi = ESIClient(access_token=token)
        raw = esi.get_market_history(region_id, type_id)
        rows = [{"type_id": type_id, "region_id": region_id, **r} for r in raw]
        store.save_market_history(rows)
    except Exception as e:
        print(f"[trading] Market history fetch failed for {type_id}: {e}")
    finally:
        if esi:
            esi.close()

    history = store.get_market_history(type_id, region_id, days=30)
    return {"ok": True, "history": history}


@app.delete("/api/c/{character_id}/track-item/{type_id}")
def api_untrack_item(character_id: int, type_id: int, region_id: int = 10000002):
    store.remove_tracked_item(character_id, type_id, region_id)
    return {"ok": True}


@app.get("/api/c/{character_id}/market-history/{type_id}")
def api_market_history(character_id: int, type_id: int, region_id: int = 10000002):
    esi = None
    try:
        token = auth.get_valid_token(character_id)
        esi = ESIClient(access_token=token)
        raw = esi.get_market_history(region_id, type_id)
        rows = [{"type_id": type_id, "region_id": region_id, **r} for r in raw]
        store.clear_market_history(type_id, region_id)
        store.save_market_history(rows)
    except Exception as e:
        print(f"[trading] Market history fetch failed for {type_id}: {e}")
    finally:
        if esi:
            esi.close()
    return store.get_market_history(type_id, region_id, days=30)


@app.get("/api/c/{character_id}/inventory")
def api_inventory(character_id: int):
    """Return all hangar assets grouped by location, with names and est. value."""
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    try:
        all_assets  = esi.get_assets_all(character_id)
        prices_raw  = esi.get_market_prices()
    finally:
        esi.close()

    price_map = {p["type_id"]: p.get("adjusted_price", 0.0) for p in prices_raw}

    # Only keep items sitting in hangars (not fitted/in cargo/containers)
    HANGAR_FLAGS = {
        "Hangar", "AssetSafety", "FleetHangar",
        "CorpSAG1", "CorpSAG2", "CorpSAG3",
        "CorpSAG4", "CorpSAG5", "CorpSAG6", "CorpSAG7",
        "Deliveries",
    }
    hangar_items = [a for a in all_assets if a.get("location_flag") in HANGAR_FLAGS]

    if not hangar_items:
        return {"by_location": {}, "location_names": {}}

    # Resolve type names
    type_ids = list({a["type_id"] for a in hangar_items})
    type_names: dict[int, str] = {}
    try:
        esi2 = ESIClient(access_token=token)
        for i in range(0, len(type_ids), 1000):
            resolved = esi2.get_universe_names(type_ids[i:i+1000])
            for r in resolved:
                if r.get("category") == "inventory_type":
                    type_names[r["id"]] = r["name"]
        esi2.close()
    except Exception as e:
        print(f"[inventory] Name resolution failed: {e}")

    # Resolve location names (NPC stations + solar systems via universe/names)
    location_ids = list({a["location_id"] for a in hangar_items})
    location_names: dict[int, str] = {}
    # Split into NPC station range and structure range
    npc_ids       = [lid for lid in location_ids if lid < 1_000_000_000_000]
    structure_ids = [lid for lid in location_ids if lid >= 1_000_000_000_000]
    try:
        if npc_ids:
            esi3 = ESIClient(access_token=token)
            for i in range(0, len(npc_ids), 1000):
                resolved = esi3.get_universe_names(npc_ids[i:i+1000])
                for r in resolved:
                    location_names[r["id"]] = r["name"]
            esi3.close()
    except Exception as e:
        print(f"[inventory] Location name resolution failed: {e}")

    for sid in structure_ids:
        try:
            esi4 = ESIClient(access_token=token)
            info = esi4._get(f"/universe/structures/{sid}/")
            esi4.close()
            location_names[sid] = info.get("name", f"Structure {sid}")
        except Exception:
            location_names[sid] = f"Inaccessible Structure"

    # Group by location
    by_location: dict[str, list] = {}
    for a in hangar_items:
        loc_id   = a["location_id"]
        loc_name = location_names.get(loc_id, f"Location {loc_id}")
        item = {
            "type_id":   a["type_id"],
            "type_name": type_names.get(a["type_id"], f"Type {a['type_id']}"),
            "quantity":  a.get("quantity", 1),
            "est_value": round(a.get("quantity", 1) * price_map.get(a["type_id"], 0.0), 0),
            "location_id": loc_id,
        }
        by_location.setdefault(loc_name, []).append(item)

    # Sort each location's items by est_value desc
    for items in by_location.values():
        items.sort(key=lambda x: x["est_value"], reverse=True)

    return {"by_location": by_location, "location_names": location_names}


if __name__ == "__main__":
    import os, signal
    pid_file = ".app.pid"
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    try:
        uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
    finally:
        if os.path.exists(pid_file):
            os.unlink(pid_file)
